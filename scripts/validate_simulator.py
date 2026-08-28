"""시뮬레이터가 생성한 데이터가 설계 의도대로인지 검증한다.

탐지기 없이 데이터 자체만 본다. 여기서 확인하는 것은 세 가지다.
  1) baseline 학습 구간(warmup)에 이상이 섞이지 않았는가
  2) 정상 시나리오가 실제 장애보다 작게 튀는가
  3) 정상 트래픽 증가에서 부하 지표와 품질 지표가 분리되는가

docs/results.md의 표를 이 스크립트로 재현한다.
"""

import sys
from pathlib import Path

import numpy as np
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.simulator import simulate

ROOT = Path(__file__).resolve().parents[1]

LOAD_KPIS = {"link_util_pct", "prb_util_pct"}


def deviation_sd(seg, normal, kpi, direction):
    """정상 구간 표준편차를 단위로 한 최대 이탈폭을 구한다.

    direction에 따라 나쁜 방향만 본다. lower_is_worse인 KPI는
    아래로 내려간 폭을 양수로 돌려준다.

    Args:
        seg: 대상 구간의 DataFrame.
        normal: 어떤 시나리오에도 속하지 않는 구간의 DataFrame.
        kpi: KPI 이름.
        direction: higher_is_worse 또는 lower_is_worse.

    Returns:
        float. 나쁜 방향의 최대 이탈폭.
    """
    mu, sd = normal[kpi].mean(), normal[kpi].std()
    z = (seg[kpi] - mu) / sd
    return z.max() if direction == "higher_is_worse" else -z.min()


def check_warmup(df, events, warmup):
    """warmup 구간이 이상 없이 깨끗한지 확인한다."""
    n_ev = sum(1 for e in events if e.start_idx < warmup)
    ratio = df.iloc[:warmup].label.mean() * 100
    print("\n[1] warmup 청정성")
    print(f"  구간 길이       : {warmup} samples")
    print(f"  구간 내 이벤트   : {n_ev}개")
    print(f"  구간 내 이상 라벨 : {ratio:.2f}%")
    ok = n_ev == 0 and ratio == 0.0
    print(f"  판정            : {'PASS' if ok else 'FAIL'}")
    return ok


def check_magnitude_order(df, events, kpi_cfg, normal):
    """정상 시나리오가 모든 장애 시나리오보다 작게 튀는지 확인한다."""
    print("\n[2] 시나리오별 최대 이탈폭")
    result = {}
    for name in sorted({e.scenario for e in events}):
        peaks = []
        is_anom = True
        for e in (x for x in events if x.scenario == name):
            is_anom = e.is_anomaly
            seg = df.iloc[e.start_idx:e.end_idx]
            for kpi in e.kpis:
                peaks.append(
                    deviation_sd(seg, normal, kpi, kpi_cfg[kpi]["direction"])
                )
        result[name] = (float(np.mean(peaks)), is_anom)

    for name, (val, is_anom) in sorted(
        result.items(), key=lambda x: -x[1][0]
    ):
        tag = "장애" if is_anom else "정상"
        print(f"  {name:22s} [{tag}] {val:5.2f} sd")

    normal_vals = [v for v, a in result.values() if not a]
    anom_vals = [v for v, a in result.values() if a]
    ok = not normal_vals or max(normal_vals) < min(anom_vals)
    print(f"  판정            : {'PASS' if ok else 'FAIL'} "
          f"(정상 시나리오가 모든 장애보다 작아야 함)")
    return ok


def check_load_quality_split(df, events, kpi_cfg, normal, scen_cfg):
    """정상 트래픽 증가에서 부하와 품질 지표가 분리되는지 확인한다."""
    normal_scenarios = [
        n for n, c in scen_cfg.items() if not c.get("is_anomaly", True)
    ]
    print("\n[3] 부하 / 품질 지표 분리")
    ok = True
    for name in normal_scenarios:
        evs = [e for e in events if e.scenario == name]
        if not evs:
            continue
        print(f"  [{name}]")
        loads, quals = [], []
        for kpi in evs[0].kpis:
            peaks = [
                deviation_sd(
                    df.iloc[e.start_idx:e.end_idx], normal, kpi,
                    kpi_cfg[kpi]["direction"]
                )
                for e in evs
            ]
            val = float(np.mean(peaks))
            group = "부하" if kpi in LOAD_KPIS else "품질"
            (loads if group == "부하" else quals).append(val)
            print(f"    {kpi:24s} [{group}] {val:5.2f} sd")
        if loads and quals:
            passed = min(loads) > max(quals)
            ok &= passed
            print(f"    최소 부하 {min(loads):.2f} sd > 최대 품질 "
                  f"{max(quals):.2f} sd : {'PASS' if passed else 'FAIL'}")
    return ok


def check_clipping(df, kpi_cfg):
    """clip 경계에 눌린 샘플 수를 보고한다.

    판정하지 않고 보고만 한다. 소수의 포화는 정상이다.
    """
    print("\n[4] clip 포화 (참고)")
    total = 0
    for kpi, cfg in kpi_cfg.items():
        lo, hi = cfg["clip"]
        n_lo = int((df[kpi] <= lo + 1e-9).sum())
        n_hi = int((df[kpi] >= hi - 1e-9).sum())
        if n_lo or n_hi:
            print(f"  {kpi:24s} 하한 {n_lo}건  상한 {n_hi}건")
            total += n_lo + n_hi
    print(f"  전체 {len(df):,}건 중 {total}건")


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    scen_yaml = yaml.safe_load((ROOT / "config/scenarios.yaml").read_text())

    df, events = simulate(kpi_yaml, scen_yaml)
    normal = df[df.scenario == ""]

    smp = kpi_yaml["sampling"]
    warmup = int(24 * 60 * 60 / smp["interval_sec"] * smp.get("warmup_days", 0))
    kpi_cfg = kpi_yaml["kpis"]

    print(f"KPI {len(kpi_cfg)}종 / {smp['days']}일 / "
          f"{smp['interval_sec']}초 간격 / {len(df):,} samples")
    print(f"이상 비율 {df.label.mean() * 100:.2f}%  "
          f"이벤트 {len(events)}건  정상 구간 {len(normal):,} samples")

    results = [
        check_warmup(df, events, warmup),
        check_magnitude_order(df, events, kpi_cfg, normal),
        check_load_quality_split(
            df, events, kpi_cfg, normal, scen_yaml["scenarios"]
        ),
    ]
    check_clipping(df, kpi_cfg)

    print("\n" + "=" * 46)
    print("전체 판정:", "PASS" if all(results) else "FAIL")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
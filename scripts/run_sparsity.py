"""장애 발생 빈도를 바꿔가며 성능 변화를 측정한다.

실제 운용 환경에서 장애는 드물다. 장애가 희소해지면 정상 구간이
길어지므로 오탐 기회가 늘고, 알람 중 진짜 장애의 비율이 나빠진다.
이 조건에서도 미탐 없이 동작하는지 확인한다.

각 조건마다 seed를 여러 개 돌려 평균을 낸다.
"""

import copy
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.detector import Baseline, detect, detect_naive
from sentinel.evaluate import evaluate, false_positive_reduction
from sentinel.simulator import simulate
from sentinel.windowing import aggregate_frame

ROOT = Path(__file__).resolve().parents[1]

SEEDS = [20260827, 11, 202, 3003, 40004]

# 시나리오별 count에 곱할 배수. 1.0이 기본 설정이다.
DENSITY = {
    "희소 (x0.4)": 0.4,
    "감소 (x0.7)": 0.7,
    "기본 (x1.0)": 1.0,
    "증가 (x1.5)": 1.5,
}


def scale_counts(scen_yaml, factor):
    """모든 시나리오의 발생 횟수에 배수를 적용한다.

    최소 1건은 남긴다. 0건이 되면 해당 시나리오가 사라져
    조건 간 비교가 성립하지 않는다.

    Args:
        scen_yaml: scenarios.yaml 내용.
        factor: 곱할 배수.

    Returns:
        수정된 사본.
    """
    out = copy.deepcopy(scen_yaml)
    for cfg in out["scenarios"].values():
        c = cfg["count"]
        if isinstance(c, (list, tuple)):
            cfg["count"] = [max(1, round(c[0] * factor)),
                            max(1, round(c[1] * factor))]
        else:
            cfg["count"] = max(1, round(c * factor))
    return out


def run_one(seed, kpi_yaml, scen_yaml, det_yaml):
    """생성부터 평가까지 한 번 수행한다."""
    scen = copy.deepcopy(scen_yaml)
    scen["seed"] = seed

    df, _ = simulate(kpi_yaml, scen)
    kpis = list(kpi_yaml["kpis"])

    interval_min = kpi_yaml["sampling"]["interval_sec"] // 60
    w = det_yaml["window"]
    windowed = aggregate_frame(
        df, w["size_min"] // interval_min,
        w["stride_min"] // interval_min, kpis, w["agg"]
    )
    windowed["ts"] = pd.to_datetime(windowed["ts"])
    windowed["scenario"] = windowed["scenario"].fillna("")

    bl = det_yaml["baseline"]
    warmup = int(24 * 60 / interval_min * bl["warmup_days"])
    train = windowed.iloc[:warmup]
    test = windowed.iloc[warmup:].reset_index(drop=True)
    step_min = interval_min * w["stride_min"]

    baseline = Baseline(bl["bucket_minutes"], bl["min_samples"]).fit(train, kpis)
    m_prop = evaluate(
        detect(test, baseline, kpis, det_yaml["detector"]),
        interval_min=step_min,
    )
    m_naive = evaluate(
        detect_naive(test, kpis, train, det_yaml["naive_baseline"]),
        interval_min=step_min,
    )
    return {
        "n_events": m_prop["tp"] + m_prop["fn"],
        "f1": m_prop["f1"],
        "precision": m_prop["precision"],
        "recall": m_prop["recall"],
        "fp": m_prop["fp"],
        "fn": m_prop["fn"],
        "mttd": m_prop["mttd_min"],
        "naive_recall": m_naive["recall"],
        "naive_fp": m_naive["fp"],
        "fp_reduction": false_positive_reduction(m_naive["fp"], m_prop["fp"]),
    }


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    scen_yaml = yaml.safe_load((ROOT / "config/scenarios.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())

    d = det_yaml["detector"]
    print(f"설정: z={d['z_threshold']}  alpha={d['ewma_alpha']}  "
          f"persistence={d['persistence']}")
    print(f"조건당 seed {len(SEEDS)}개\n")

    print(f"  {'조건':>12} {'장애':>5} {'F1':>6} {'P':>6} {'R':>6} "
          f"{'FP':>5} {'FN':>5} {'지연':>7} {'오탐감소':>8}")

    rows = []
    for label, factor in DENSITY.items():
        scen = scale_counts(scen_yaml, factor)
        runs = [run_one(s, kpi_yaml, scen, det_yaml) for s in SEEDS]

        def avg(key):
            vals = np.array([r[key] for r in runs], dtype=float)
            vals = vals[~np.isnan(vals)]
            return vals.mean() if len(vals) else float("nan")

        print(f"  {label:>12} {avg('n_events'):>5.1f} {avg('f1'):>6.3f} "
              f"{avg('precision'):>6.3f} {avg('recall'):>6.3f} "
              f"{avg('fp'):>5.1f} {avg('fn'):>5.1f} "
              f"{avg('mttd'):>6.1f}분 {avg('fp_reduction'):>7.1f}%")

        rows.append({
            "condition": label, "factor": factor,
            "n_events": avg("n_events"), "f1": avg("f1"),
            "precision": avg("precision"), "recall": avg("recall"),
            "fp": avg("fp"), "fn": avg("fn"), "mttd_min": avg("mttd"),
            "naive_recall": avg("naive_recall"),
            "fp_reduction_pct": avg("fp_reduction"),
        })

    min_recall = min(r["recall"] for r in rows)
    print(f"\n모든 조건에서 recall 최저값: {min_recall:.3f}")

    out = ROOT / "data/sparsity.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
"""seed를 바꿔가며 성능을 반복 측정한다.

단일 데이터셋에서 조정한 파라미터가 다른 데이터에서도 통하는지
확인한다. 표본이 작을 때 일부 지표는 우연의 영향을 받으므로,
평균뿐 아니라 범위와 표준편차를 함께 본다.

시뮬레이터부터 다시 돌리므로 seed마다 장애 배치와 노이즈가 모두
달라진다. 파라미터는 config에 있는 값을 그대로 사용한다.
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

SEEDS = [20260827, 11, 202, 3003, 40004, 555555, 6060606]


def run_one(seed, kpi_yaml, scen_yaml, det_yaml):
    """seed 하나로 생성부터 평가까지 수행한다.

    Args:
        seed: 시뮬레이터 난수 seed.
        kpi_yaml: kpi.yaml 내용.
        scen_yaml: scenarios.yaml 내용.
        det_yaml: detector.yaml 내용.

    Returns:
        기준선과 제안 방식의 지표를 담은 dict.
    """
    scen = copy.deepcopy(scen_yaml)
    scen["seed"] = seed

    df, events = simulate(kpi_yaml, scen)
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

    n_anom = sum(1 for e in events
                 if e.is_anomaly and e.start_idx >= warmup * w["stride_min"])
    return {
        "seed": seed,
        "n_events": n_anom,
        "naive_f1": m_naive["f1"],
        "naive_fp": m_naive["fp"],
        "naive_recall": m_naive["recall"],
        "naive_mttd": m_naive["mttd_min"],
        "f1": m_prop["f1"],
        "precision": m_prop["precision"],
        "recall": m_prop["recall"],
        "fp": m_prop["fp"],
        "fn": m_prop["fn"],
        "mttd": m_prop["mttd_min"],
        "fp_reduction": false_positive_reduction(m_naive["fp"], m_prop["fp"]),
    }


def summarize(rows, key, label, fmt="{:.3f}"):
    """지표 하나의 평균, 표준편차, 범위를 출력한다."""
    vals = np.array([r[key] for r in rows], dtype=float)
    vals = vals[~np.isnan(vals)]
    if len(vals) == 0:
        print(f"  {label:16s} 측정 불가")
        return
    mean = fmt.format(vals.mean())
    sd = fmt.format(vals.std())
    lo, hi = fmt.format(vals.min()), fmt.format(vals.max())
    print(f"  {label:16s} 평균 {mean}  표준편차 {sd}  범위 {lo} ~ {hi}")


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    scen_yaml = yaml.safe_load((ROOT / "config/scenarios.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())

    d = det_yaml["detector"]
    print(f"설정: z={d['z_threshold']}  alpha={d['ewma_alpha']}  "
          f"persistence={d['persistence']}")
    print(f"seed {len(SEEDS)}개로 반복 측정\n")

    print(f"  {'seed':>9} {'장애':>4} {'F1':>6} {'P':>6} {'R':>6} "
          f"{'FP':>4} {'FN':>4} {'지연':>7} {'오탐감소':>8}")
    rows = []
    for seed in SEEDS:
        r = run_one(seed, kpi_yaml, scen_yaml, det_yaml)
        rows.append(r)
        print(f"  {r['seed']:>9} {r['n_events']:>4} {r['f1']:>6.3f} "
              f"{r['precision']:>6.3f} {r['recall']:>6.3f} "
              f"{r['fp']:>4} {r['fn']:>4} {r['mttd']:>6.1f}분 "
              f"{r['fp_reduction']:>7.1f}%")

    print("\n[제안 방식]")
    summarize(rows, "f1", "F1")
    summarize(rows, "precision", "Precision")
    summarize(rows, "recall", "Recall")
    summarize(rows, "fp", "오탐 건수", "{:.1f}")
    summarize(rows, "fn", "미탐 건수", "{:.1f}")
    summarize(rows, "mttd", "탐지 지연(분)", "{:.1f}")
    summarize(rows, "fp_reduction", "오탐 감소율(%)", "{:.1f}")

    print("\n[비교 기준선]")
    summarize(rows, "naive_f1", "F1")
    summarize(rows, "naive_recall", "Recall")
    summarize(rows, "naive_fp", "오탐 건수", "{:.1f}")

    n_perfect = sum(1 for r in rows if r["recall"] == 1.0)
    print(f"\nrecall 1.000 달성: {n_perfect}/{len(rows)} seed")

    out = ROOT / "data/multiseed.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
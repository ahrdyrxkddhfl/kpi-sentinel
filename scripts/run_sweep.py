"""탐지 파라미터를 훑어 성능 변화를 기록한다.

단일 파라미터를 바꿔가며 지표가 어떻게 움직이는지 본다.
최적값 하나를 고르는 것보다, 교환 관계가 어디서 꺾이는지를
확인하는 것이 목적이다.
"""

import copy
import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.detector import Baseline, detect, detect_naive
from sentinel.evaluate import evaluate, false_positive_reduction

ROOT = Path(__file__).resolve().parents[1]

SWEEPS = {
    "persistence": [1, 2, 3, 4, 5, 6, 7, 8, 10],
    "z_threshold": [2.0, 2.5, 3.0, 3.5, 4.0],
    "ewma_alpha": [0.1, 0.2, 0.3, 0.5, 0.7, 1.0],
}


def run_once(test, train, baseline, kpis, det_cfg, interval_min):
    """설정 하나로 탐지와 평가를 수행한다.

    Args:
        test: 평가 구간 DataFrame.
        train: 학습 구간 DataFrame.
        baseline: 학습이 끝난 Baseline.
        kpis: KPI 이름 목록.
        det_cfg: detector 설정 dict.
        interval_min: 한 행이 나타내는 시간 (분).

    Returns:
        평가 지표 dict.
    """
    result = detect(test, baseline, kpis, det_cfg)
    return evaluate(result, interval_min=interval_min)


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())

    src = ROOT / "data/windowed/kpi.csv"
    if not src.exists():
        print("윈도우 집계 결과가 없다. scripts/run_window.py를 먼저 실행할 것.")
        return 1

    df = pd.read_csv(src)
    df["ts"] = pd.to_datetime(df["ts"])
    df["scenario"] = df["scenario"].fillna("")
    kpis = list(kpi_yaml["kpis"])

    bl_cfg = det_yaml["baseline"]
    interval_min = (kpi_yaml["sampling"]["interval_sec"] // 60
                    * det_yaml["window"]["stride_min"])
    warmup = int(24 * 60 / (kpi_yaml["sampling"]["interval_sec"] // 60)
                 * bl_cfg["warmup_days"])

    train = df.iloc[:warmup]
    test = df.iloc[warmup:].reset_index(drop=True)

    baseline = Baseline(bl_cfg["bucket_minutes"], bl_cfg["min_samples"])
    baseline.fit(train, kpis)

    naive = detect_naive(test, kpis, train, det_yaml["naive_baseline"])
    m_naive = evaluate(naive, interval_min=interval_min)
    print(f"비교 기준선: F1 {m_naive['f1']:.3f}  FP {m_naive['fp']}  "
          f"지연 {m_naive['mttd_min']:.1f}분\n")

    rows = []
    for param, values in SWEEPS.items():
        print(f"[{param}]")
        print(f"  {'값':>6}  {'F1':>6} {'P':>6} {'R':>6} "
              f"{'FP':>4} {'FN':>4} {'지연':>7} {'오탐감소':>8}")
        for v in values:
            cfg = copy.deepcopy(det_yaml["detector"])
            cfg[param] = v
            m = run_once(test, train, baseline, kpis, cfg, interval_min)
            fpr = false_positive_reduction(m_naive["fp"], m["fp"])
            print(f"  {v:>6}  {m['f1']:>6.3f} {m['precision']:>6.3f} "
                  f"{m['recall']:>6.3f} {m['fp']:>4} {m['fn']:>4} "
                  f"{m['mttd_min']:>6.1f}분 {fpr:>7.1f}%")
            rows.append({
                "param": param, "value": v,
                "f1": m["f1"], "precision": m["precision"],
                "recall": m["recall"], "fp": m["fp"], "fn": m["fn"],
                "mttd_min": m["mttd_min"], "fp_reduction_pct": fpr,
            })
        print()

    out = ROOT / "data/sweep.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
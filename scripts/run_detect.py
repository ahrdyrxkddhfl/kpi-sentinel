"""탐지 엔진을 실행해 알람을 생성한다.

제안 방식과 비교 기준선을 함께 돌려 같은 조건에서 비교할 수 있게 한다.
"""

import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.detector import Baseline, detect, detect_naive

ROOT = Path(__file__).resolve().parents[1]


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())

    src = ROOT / "data/windowed/kpi.csv"
    if not src.exists():
        print(f"윈도우 집계 결과가 없다: {src}")
        print("먼저 scripts/run_window.py를 실행할 것.")
        return 1

    df = pd.read_csv(src)
    df["ts"] = pd.to_datetime(df["ts"])
    df["scenario"] = df["scenario"].fillna("")
    kpis = list(kpi_yaml["kpis"])

    bl_cfg = det_yaml["baseline"]
    interval_min = kpi_yaml["sampling"]["interval_sec"] // 60
    warmup = int(24 * 60 / interval_min * bl_cfg["warmup_days"])

    train = df.iloc[:warmup]
    test = df.iloc[warmup:].reset_index(drop=True)

    if train.label.sum() > 0:
        print(f"경고: 학습 구간에 이상 라벨 {int(train.label.sum())}건이 있다.")

    baseline = Baseline(bl_cfg["bucket_minutes"], bl_cfg["min_samples"])
    baseline.fit(train, kpis)

    proposed = detect(test, baseline, kpis, det_yaml["detector"])
    naive = detect_naive(test, kpis, train, det_yaml["naive_baseline"])

    out_dir = ROOT / "data/alerts"
    out_dir.mkdir(parents=True, exist_ok=True)
    proposed.to_csv(out_dir / "proposed.csv", index=False)
    naive.to_csv(out_dir / "naive.csv", index=False)

    print(f"학습 {len(train):,}행 / 평가 {len(test):,}행")
    print(f"  제안 방식  알람 {int(proposed.alert.sum()):,}건")
    print(f"  기준선     알람 {int(naive.alert.sum()):,}건")
    print(f"  -> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
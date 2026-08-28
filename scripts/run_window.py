"""슬라이딩 윈도우 집계를 실행해 결과를 저장한다."""

import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.windowing import aggregate_frame

ROOT = Path(__file__).resolve().parents[1]


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())

    src = ROOT / "data/raw/kpi.csv"
    if not src.exists():
        print(f"원본 데이터가 없다: {src}")
        return 1

    df = pd.read_csv(src)
    df["scenario"] = df["scenario"].fillna("")
    kpi_names = list(kpi_yaml["kpis"])

    interval_min = kpi_yaml["sampling"]["interval_sec"] // 60
    w = det_yaml["window"]
    size = w["size_min"] // interval_min
    stride = w["stride_min"] // interval_min

    out_df = aggregate_frame(df, size, stride, kpi_names, w["agg"])

    out = ROOT / "data/windowed/kpi.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out, index=False)

    print(f"윈도우 {size} x stride {stride} ({w['agg']})")
    print(f"  입력 {len(df):,}행 -> 출력 {len(out_df):,}행")
    print(f"  이상 라벨 {out_df.label.sum():,}건 "
          f"({out_df.label.mean() * 100:.2f}%)")
    print(f"  -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
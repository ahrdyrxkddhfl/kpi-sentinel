"""탐지 결과를 DuckDB에 적재한다."""

import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.storage import Store

ROOT = Path(__file__).resolve().parents[1]


def main():
    cfg = yaml.safe_load((ROOT / "config/storage.yaml").read_text())

    src = ROOT / "data/alerts/proposed.csv"
    if not src.exists():
        print("탐지 결과가 없다. scripts/run_detect.py를 먼저 실행할 것.")
        return 1

    df = pd.read_csv(src)
    df["ts"] = pd.to_datetime(df["ts"])
    df["scenario"] = df["scenario"].fillna("")
    df["trigger"] = df["trigger"].fillna("")

    z_cols = [c for c in df.columns if c.startswith("z_")]
    kpi_cols = [c for c in df.columns
                if c not in ("ts", "label", "scenario", "alert", "trigger")
                and not c.startswith("z_")]

    with Store(ROOT / cfg["duckdb"]["path"]) as store:
        n1 = store.write(cfg["tables"]["metrics"],
                         df[["ts", *kpi_cols, "label", "scenario"]])
        n2 = store.write(cfg["tables"]["alerts"],
                         df[["ts", "alert", "trigger", "label",
                             "scenario", *z_cols]])
        print(f"metrics {n1:,}행, alerts {n2:,}행 적재")

        summary = store.query("""
            SELECT scenario,
                   COUNT(*) AS rows,
                   SUM(alert) AS alert_rows
            FROM alerts
            WHERE scenario <> ''
            GROUP BY scenario
            ORDER BY alert_rows DESC
        """)
        print(f"\n{summary.to_string(index=False)}")
        print(f"\n-> {ROOT / cfg['duckdb']['path']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
"""시뮬레이터를 실행해 원본 데이터를 생성한다."""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.simulator import simulate

ROOT = Path(__file__).resolve().parents[1]


def main():
    kpi_cfg = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    scen_cfg = yaml.safe_load((ROOT / "config/scenarios.yaml").read_text())

    df, events = simulate(kpi_cfg, scen_cfg)

    out = ROOT / "data/raw/kpi.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    print(f"rows={len(df)}  cols={len(df.columns)}  -> {out}")
    print(f"anomaly ratio = {df.label.mean() * 100:.2f}%")
    print("\nevents:")
    for e in events:
        tag = "" if e.is_anomaly else "  (label=0)"
        print(f"  {e.scenario:22s} {df.ts[e.start_idx]} "
              f"+{e.end_idx - e.start_idx:4d}min  {list(e.kpis)}{tag}")


if __name__ == "__main__":
    main()
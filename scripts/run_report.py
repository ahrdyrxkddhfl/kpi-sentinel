"""알람 구간에 대한 장애 리포트를 생성한다."""

import argparse
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.report import build_report, summarize_window
from sentinel.storage import Store

from sentinel.evaluate import find_runs

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", help="리포트를 생성할 시나리오 이름")
    parser.add_argument("--dry-run", action="store_true",
                        help="API를 호출하지 않고 요약만 출력한다")
    args = parser.parse_args()

    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    st_yaml = yaml.safe_load((ROOT / "config/storage.yaml").read_text())
    rp_yaml = yaml.safe_load((ROOT / "config/report.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())
    kpis = list(kpi_yaml["kpis"])
    t = st_yaml["tables"]

    db = ROOT / st_yaml["duckdb"]["path"]
    if not db.exists():
        print("저장소가 없다. scripts/run_store.py를 먼저 실행할 것.")
        return 1

    with Store(db) as store:
        rows = store.query(
            f"SELECT ts, scenario FROM {t['alerts']} ORDER BY ts"
        )
        alerts_flag = store.query(
            f"SELECT alert FROM {t['alerts']} ORDER BY ts"
        )["alert"].to_numpy()

        target = args.scenario
        if target:
            mask = (rows["scenario"] == target).to_numpy()
        else:
            mask = (rows["scenario"] != "").to_numpy()

        segments = find_runs(mask)
        if not segments:
            print("해당 시나리오 구간이 없다.")
            return 1

        # 알람이 가장 많이 발생한 이벤트 하나를 고른다.
        best = max(segments,
                   key=lambda s: int(alerts_flag[s[0]:s[1]].sum()))
        s_idx, e_idx = best
        scenario = rows["scenario"].iloc[s_idx]
        start = rows["ts"].iloc[s_idx]
        end = rows["ts"].iloc[e_idx - 1]

        print(f"대상 구간: {scenario}  {start} ~ {end}  "
              f"({e_idx - s_idx}분, 전체 {len(segments)}건 중 1건)\n")

        summary = summarize_window(store, t["metrics"], t["alerts"],
                                   start, end, kpis,
                                   det_yaml["detector"]["z_threshold"])

    if args.dry_run:
        import json
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    text = build_report(summary, rp_yaml["model"], rp_yaml["max_tokens"],
                        rp_yaml["domain_hint"])

    out = ROOT / f"data/reports/{scenario}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(text)
    print(f"\n-> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
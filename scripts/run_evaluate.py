"""탐지 결과를 평가해 핵심 지표를 출력한다.

F1, 평균 탐지 지연(MTTD), 오탐 감소율이 이 스크립트의 산출물이다.
"""

import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.evaluate import evaluate, false_positive_reduction

ROOT = Path(__file__).resolve().parents[1]


def show(name, m):
    """지표 하나를 보기 좋게 출력한다."""
    print(f"\n[{name}]")
    print(f"  TP {m['tp']}  FP {m['fp']}  FN {m['fn']}")
    print(f"  precision {m['precision']:.3f}  "
          f"recall {m['recall']:.3f}  F1 {m['f1']:.3f}")
    print(f"  평균 탐지 지연 {m['mttd_min']:.1f}분")
    if m["per_scenario"]:
        print("  시나리오별 탐지율")
        for scen, s in sorted(m["per_scenario"].items()):
            avg = (sum(s["delays"]) / len(s["delays"])
                   if s["delays"] else float("nan"))
            print(f"    {scen:22s} {s['hit']}/{s['total']}  "
                  f"지연 {avg:.1f}분")
    if m["fp_by_scenario"]:
        print("  오탐 발생 위치")
        for scen, c in sorted(m["fp_by_scenario"].items(),
                              key=lambda x: -x[1]):
            print(f"    {scen:22s} {c}건")


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())

    alerts = ROOT / "data/alerts"
    if not (alerts / "proposed.csv").exists():
        print("탐지 결과가 없다. 먼저 scripts/run_detect.py를 실행할 것.")
        return 1

    interval_min = (kpi_yaml["sampling"]["interval_sec"] // 60
                    * det_yaml["window"]["stride_min"])

    proposed = pd.read_csv(alerts / "proposed.csv")
    naive = pd.read_csv(alerts / "naive.csv")
    for df in (proposed, naive):
        df["scenario"] = df["scenario"].fillna("")

    m_naive = evaluate(naive, interval_min=interval_min)
    m_prop = evaluate(proposed, interval_min=interval_min)

    show("비교 기준선 (전역 z-score)", m_naive)
    show("제안 방식 (요일x시간대 baseline + EWMA + 지속성)", m_prop)

    fpr = false_positive_reduction(m_naive["fp"], m_prop["fp"])

    print("\n" + "=" * 52)
    print("핵심 지표")
    print(f"  F1            {m_naive['f1']:.3f} -> {m_prop['f1']:.3f}")
    print(f"  평균 탐지 지연 {m_naive['mttd_min']:.1f}분 -> "
          f"{m_prop['mttd_min']:.1f}분")
    print(f"  오탐 감소율    {fpr:.1f}%  "
          f"(FP {m_naive['fp']} -> {m_prop['fp']})")


if __name__ == "__main__":
    sys.exit(main())
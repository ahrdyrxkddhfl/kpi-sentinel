"""스트림 신뢰성 실험용 다중 셀 데이터를 생성한다.

탐지 성능 실험에 쓰는 data/raw/kpi.csv는 건드리지 않는다.
생성 직후 0번 셀이 단일 시뮬레이션 결과와 같은지 확인해,
기존 실험 수치의 재현성이 유지되는지 보장한다.
"""

import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.simulator import simulate, simulate_entities

ROOT = Path(__file__).resolve().parents[1]


def check_first_entity(df, kpi_cfg, scen_cfg, id_column, first_id):
    """0번 개체가 단일 시뮬레이션 결과와 같은지 확인한다.

    Args:
        df: simulate_entities가 반환한 DataFrame.
        kpi_cfg: kpi.yaml을 파싱한 dict.
        scen_cfg: scenarios.yaml을 파싱한 dict.
        id_column: 개체 식별자 컬럼 이름.
        first_id: 0번 개체의 식별자.

    Raises:
        AssertionError: 두 결과가 다를 때.
    """
    single, _ = simulate(kpi_cfg, scen_cfg)
    first = (df[df[id_column] == first_id]
             .drop(columns=id_column)
             .reset_index(drop=True))
    pd.testing.assert_frame_equal(first, single)


def main():
    kpi_cfg = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    scen_cfg = yaml.safe_load((ROOT / "config/scenarios.yaml").read_text())
    me = yaml.safe_load((ROOT / "config/stream.yaml").read_text())["multi_entity"]

    df, events = simulate_entities(
        kpi_cfg, scen_cfg, me["count"], me["id_column"], me["id_prefix"]
    )

    first_id = f"{me['id_prefix']}000"
    check_first_entity(df, kpi_cfg, scen_cfg, me["id_column"], first_id)

    out = ROOT / me["path"]
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    print(f"rows={len(df):,}  cells={me['count']}  -> {out}")
    print(f"{first_id} == 단일 시뮬레이션 결과: 확인")
    for eid, ev in events.items():
        n_anom = sum(e.is_anomaly for e in ev)
        print(f"  {eid}: 이상 {n_anom}건, 함정 {len(ev) - n_anom}건")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""스트림 적재 결과를 셀별 배치 집계와 값까지 비교한다.

run_consume_cells.py는 셀별 적재 건수만 확인한다. 적재가
ON CONFLICT DO NOTHING이라 같은 (셀, ts)에 틀린 값이 먼저 들어가면
건수는 맞아도 값이 틀린 채로 남는다. 리밸런스로 낡은 윈도우 버퍼가
섞이는 경우가 그렇다. 이 스크립트는 그런 경우를 잡아낸다.
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.windowing import aggregate_frame

ROOT = Path(__file__).resolve().parents[1]
SHOW_EXAMPLES = 5          # 불일치 예시를 몇 건까지 출력할지 (출력 길이만 바뀜)


def expected_frame(path, key_col, size, stride, kpis, agg):
    """원본 CSV를 셀별로 배치 집계해 정답 윈도우 결과를 만든다.

    Args:
        path: 다중 셀 원본 CSV 경로.
        key_col: 셀 식별자 컬럼 이름.
        size: 윈도우 크기.
        stride: 집계 간격.
        kpis: 집계할 수치 컬럼 목록.
        agg: 집계 방식.

    Returns:
        key_col, ts, KPI 컬럼을 담은 DataFrame.
    """
    df = pd.read_csv(path, dtype={"scenario": str})
    df["ts"] = pd.to_datetime(df["ts"])
    parts = []
    for key, group in df.groupby(key_col, sort=True):
        part = aggregate_frame(group, size, stride, kpis, agg)
        part[key_col] = key
        parts.append(part[[key_col, "ts", *kpis]])
    return pd.concat(parts, ignore_index=True)


def stored_frame(dsn, password, table, key_col, kpis):
    """적재된 테이블을 읽는다.

    Args:
        dsn: 비밀번호를 뺀 접속 문자열.
        password: 접속 비밀번호.
        table: 테이블 이름.
        key_col: 셀 식별자 컬럼 이름.
        kpis: 읽을 수치 컬럼 목록.

    Returns:
        key_col, ts, KPI 컬럼을 담은 DataFrame.
    """
    import psycopg
    from psycopg import sql

    cols = [key_col, "ts", *kpis]
    query = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(sql.Identifier(c) for c in cols), sql.Identifier(table))
    with psycopg.connect(dsn, password=password) as conn:
        rows = conn.execute(query).fetchall()
    out = pd.DataFrame(rows, columns=cols)
    out["ts"] = pd.to_datetime(out["ts"])
    return out


def compare(expected, stored, key_col, kpis):
    """두 결과를 (셀, ts)로 맞춰 누락·초과·값 불일치를 센다.

    Args:
        expected: 정답 DataFrame.
        stored: 적재된 DataFrame.
        key_col: 셀 식별자 컬럼 이름.
        kpis: 비교할 수치 컬럼 목록.

    Returns:
        (셀별 요약 DataFrame, 값이 다른 행 DataFrame) 튜플.
    """
    m = expected.merge(stored, on=[key_col, "ts"], how="outer",
                       suffixes=("_exp", "_got"), indicator=True)
    both = m[m["_merge"] == "both"]
    bad = np.zeros(len(both), dtype=bool)
    for k in kpis:
        bad |= ~np.isclose(both[f"{k}_exp"], both[f"{k}_got"], rtol=1e-9, atol=1e-9)
    mismatched = both[bad]

    summary = pd.DataFrame({
        "기대": expected.groupby(key_col).size(),
        "적재": stored.groupby(key_col).size(),
        "누락": m[m["_merge"] == "left_only"].groupby(key_col).size(),
        "초과": m[m["_merge"] == "right_only"].groupby(key_col).size(),
        "값불일치": mismatched.groupby(key_col).size(),
    }).fillna(0).astype(int)
    return summary, mismatched


def main():
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise SystemExit("POSTGRES_PASSWORD가 없다. .env 파일에 설정할 것.")

    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())
    pg = yaml.safe_load((ROOT / "config/storage.yaml").read_text())["postgres"]
    me = stream_cfg["multi_entity"]
    key_col = me["id_column"]
    kpis = list(kpi_yaml["kpis"])

    interval_min = kpi_yaml["sampling"]["interval_sec"] // 60
    w = det_yaml["window"]
    size = w["size_min"] // interval_min
    stride = w["stride_min"] // interval_min

    expected = expected_frame(ROOT / me["path"], key_col, size, stride, kpis, w["agg"])
    stored = stored_frame(pg["dsn"], password, pg["table"], key_col, kpis)
    summary, mismatched = compare(expected, stored, key_col, kpis)

    print(f"테이블 {pg['table']} vs 셀별 배치 집계\n")
    print(summary.to_string())
    problems = int(summary[["누락", "초과", "값불일치"]].to_numpy().sum())
    if problems == 0:
        print("\n누락·초과·값 불일치 없음")
        return 0
    if len(mismatched):
        print(f"\n값이 다른 행 예시 (최대 {SHOW_EXAMPLES}건)")
        cols = [key_col, "ts"] + [c for k in kpis[:2] for c in (f"{k}_exp", f"{k}_got")]
        print(mismatched[cols].head(SHOW_EXAMPLES).to_string(index=False))
    return 1


if __name__ == "__main__":
    sys.exit(main())

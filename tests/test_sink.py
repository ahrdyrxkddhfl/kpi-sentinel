"""PostgresSink의 멱등 적재를 확인한다.

같은 결과를 다시 적재해도 행이 늘지 않는지 본다. 커밋 전략 중 일부는
재시작 후 같은 레코드를 다시 읽으므로, 이 성질이 깨지면 중복 행이 생긴다.

실제 PostgreSQL이 필요하다. docker compose로 postgres를 띄우고 .env에
POSTGRES_PASSWORD를 둔 뒤 실행한다. 접속할 수 없으면 테스트를 건너뛴다.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

psycopg = pytest.importorskip("psycopg")
dotenv = pytest.importorskip("dotenv")

from sentinel.sink import PostgresSink  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TEST_TABLE = "sink_idempotency_test"


@pytest.fixture
def sink():
    """테스트 전용 테이블을 가진 PostgresSink를 만들고, 끝나면 테이블을 지운다."""
    dotenv.load_dotenv(ROOT / ".env")
    pg = yaml.safe_load((ROOT / "config/storage.yaml").read_text())["postgres"]
    try:
        s = PostgresSink(pg["dsn"], TEST_TABLE, "entity_id", ["value"],
                         password=os.environ.get("POSTGRES_PASSWORD"))
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL에 접속할 수 없다: {e}")
    s.truncate()
    yield s
    s.drop()
    s.close()


def make_rows(n, start=0):
    """개체 하나의 윈도우 결과 n건을 만든다."""
    t0 = datetime(2026, 1, 1)
    return [{"entity_id": "e-000", "ts": t0 + timedelta(minutes=5 * i),
             "value": float(i), "label": 0, "scenario": None}
            for i in range(start, start + n)]


def test_same_batch_twice_is_stored_once(sink):
    """같은 배치를 두 번 넣으면 두 번째는 전부 건너뛴다."""
    rows = make_rows(3)
    assert sink.write(rows) == (3, 0)
    assert sink.write(rows) == (0, 3)
    assert sink.count_by_key() == {"e-000": 3}


def test_overlapping_batch_inserts_only_new_rows(sink):
    """앞 배치와 일부가 겹치면 새 행만 들어간다.

    window_safe 전략이 재시작 후 버퍼를 다시 채우려고 앞부분을 다시 읽는
    상황과 같다.
    """
    assert sink.write(make_rows(3, start=0)) == (3, 0)
    assert sink.write(make_rows(3, start=2)) == (2, 1)
    assert sink.count_by_key() == {"e-000": 5}


def test_empty_batch_does_nothing(sink):
    """빈 배치는 아무것도 하지 않는다."""
    assert sink.write([]) == (0, 0)
    assert sink.count_by_key() == {}

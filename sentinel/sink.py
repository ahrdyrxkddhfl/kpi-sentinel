"""윈도우 집계 결과를 PostgreSQL에 증분·멱등으로 적재한다.

도메인 비의존. 컬럼 이름의 의미를 알지 못하고, 설정으로 받은 키 컬럼과
수치 컬럼으로 테이블을 만든다.

스트림 경로의 적재에 DuckDB 대신 PostgreSQL을 쓰는 이유는 동시 쓰기다.
DuckDB는 파일 하나에 한 프로세스만 쓸 수 있어, 같은 컨슈머 그룹의
컨슈머 여러 개가 동시에 적재할 수 없다.

멱등성은 (키, ts) 기본키로 보장한다. 같은 결과가 여러 번 들어와도 한 번만
저장되므로, 장애 후 재처리로 같은 레코드를 다시 읽어도 결과가 오염되지 않는다.
"""

from datetime import datetime, timezone

import psycopg
from psycopg import sql

MS_PER_SEC = 1000


class PostgresSink:
    """윈도우 결과를 적재하는 PostgreSQL 테이블 하나를 다룬다.

    연결은 autocommit으로 연다. write가 반환된 시점에는 해당 배치가 이미
    커밋되어 있다. 오프셋 커밋을 적재 이후로 미루는 전략이 이 성질에 기댄다.

    with 문으로 쓰면 연결이 자동으로 닫힌다.

    Attributes:
        table: 테이블 이름.
        key_column: 개체 식별자 컬럼 이름.
        value_columns: 수치 컬럼 이름 목록.
    """

    def __init__(self, dsn, table, key_column, value_columns, password=None):
        """연결을 열고 테이블이 없으면 만든다.

        Args:
            dsn: 비밀번호를 뺀 PostgreSQL 접속 문자열.
            table: 테이블 이름.
            key_column: 개체 식별자 컬럼 이름. 기본키의 첫 번째 컬럼이 된다.
            value_columns: 수치 컬럼 이름 목록.
            password: 접속 비밀번호. 설정 파일에 두지 않고 호출하는 쪽이
                환경변수에서 읽어 넘긴다. None이면 접속 문자열만 쓴다.
        """
        self.table = table
        self.key_column = key_column
        self.value_columns = list(value_columns)
        self.conn = psycopg.connect(dsn, password=password, autocommit=True)
        self._ensure_table()
        self._insert_sql = self._build_insert()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        """연결을 닫는다."""
        self.conn.close()

    def _ensure_table(self):
        """테이블이 없으면 만든다. 예전 테이블에 produced_at이 없으면 추가한다.

        처리 지연은 ingested_at(적재 시각) - produced_at(프로듀서가 보낸 시각)으로
        잰다. ts는 시뮬레이터가 만든 과거 날짜라서 ingested_at과 빼면
        몇 주짜리 값이 나오므로 지연 측정에 쓸 수 없다.
        """
        cols = [
            sql.SQL("{} text NOT NULL").format(sql.Identifier(self.key_column)),
            sql.SQL("ts timestamp NOT NULL"),
        ]
        cols += [sql.SQL("{} double precision").format(sql.Identifier(c))
                 for c in self.value_columns]
        cols += [
            sql.SQL("label integer"),
            sql.SQL("scenario text"),
            sql.SQL("produced_at timestamptz"),
            sql.SQL("ingested_at timestamptz NOT NULL DEFAULT now()"),
            sql.SQL("PRIMARY KEY ({}, ts)").format(
                sql.Identifier(self.key_column)),
        ]
        self.conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(self.table), sql.SQL(", ").join(cols)))
        self.conn.execute(sql.SQL(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS produced_at timestamptz"
        ).format(sql.Identifier(self.table)))

    def _build_insert(self):
        """배치 전체를 한 문장으로 넣는 INSERT 문을 만든다.

        컬럼별 배열을 unnest로 펼쳐 한 번에 넣는다. 행마다 INSERT를
        보내는 것보다 왕복이 적고, rowcount로 실제 삽입 건수를 바로 얻는다.

        Returns:
            psycopg sql.Composed.
        """
        names = [self.key_column, "ts", *self.value_columns, "label", "scenario",
                 "produced_at"]
        types = ["text", "timestamp", *["float8"] * len(self.value_columns),
                 "int4", "text", "timestamptz"]
        arrays = sql.SQL(", ").join(
            sql.SQL("%s::{}[]").format(sql.SQL(t)) for t in types)
        return sql.SQL(
            "INSERT INTO {table} ({cols}) SELECT * FROM unnest({arrays}) "
            "ON CONFLICT ({key}, ts) DO NOTHING"
        ).format(
            table=sql.Identifier(self.table),
            cols=sql.SQL(", ").join(sql.Identifier(n) for n in names),
            arrays=arrays,
            key=sql.Identifier(self.key_column),
        )

    def write(self, rows):
        """윈도우 결과 여러 건을 한 번에 적재한다.

        이미 있는 (키, ts)는 건너뛴다. 한 문장으로 실행되므로 배치 전체가
        들어가거나 전혀 들어가지 않는다.

        Args:
            rows: 윈도우 결과 dict 목록.

        Returns:
            (inserted, ignored) 튜플. ignored는 기본키 충돌로 건너뛴 건수다.
        """
        if not rows:
            return 0, 0
        params = [
            [r[self.key_column] for r in rows],
            [r["ts"] for r in rows],
            *[[r[c] for r in rows] for c in self.value_columns],
            [r.get("label") for r in rows],
            [r.get("scenario") for r in rows],
            [ms_to_datetime(r.get("produced_at")) for r in rows],
        ]
        cur = self.conn.execute(self._insert_sql, params)
        inserted = cur.rowcount
        return inserted, len(rows) - inserted

    def count_by_key(self):
        """키별 적재 건수를 돌려준다.

        Returns:
            키 -> 건수 dict.
        """
        rows = self.conn.execute(sql.SQL(
            "SELECT {key}, count(*) FROM {table} GROUP BY 1 ORDER BY 1"
        ).format(key=sql.Identifier(self.key_column),
                 table=sql.Identifier(self.table))).fetchall()
        return dict(rows)

    def latency_ms(self, percentiles):
        """전송부터 적재까지 걸린 시간의 분포를 구한다.

        produced_at이 있는 행만 센다. 프로듀서와 컨슈머를 동시에 돌린
        실시간 실행에서만 의미가 있다. 미리 쌓아 둔 토픽을 나중에 읽으면
        토픽에 머문 시간까지 더해진다.

        Args:
            percentiles: 0~1 사이 값 목록 (예: [0.5, 0.95]).

        Returns:
            {"n": 건수, "max": 최댓값, <p>: 백분위값} dict. 단위는 ms.
            해당 행이 없으면 {"n": 0}.
        """
        lag = sql.SQL("extract(epoch FROM ingested_at - produced_at) * {}").format(
            sql.Literal(MS_PER_SEC))
        row = self.conn.execute(sql.SQL(
            "SELECT count(*), max({lag}), "
            "percentile_cont(%s::float8[]) WITHIN GROUP (ORDER BY {lag}) "
            "FROM {table} WHERE produced_at IS NOT NULL"
        ).format(lag=lag, table=sql.Identifier(self.table)),
            [list(percentiles)]).fetchone()
        n, worst, values = row
        if not n:
            return {"n": 0}
        out = {"n": n, "max": float(worst)}
        out.update({p: float(v) for p, v in zip(percentiles, values)})
        return out

    def truncate(self):
        """테이블을 비운다. 실험을 처음부터 다시 할 때 쓴다."""
        self.conn.execute(sql.SQL("TRUNCATE {}").format(
            sql.Identifier(self.table)))

    def drop(self):
        """테이블을 지운다. 테스트가 끝난 뒤 정리할 때 쓴다."""
        self.conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(self.table)))


def ms_to_datetime(ms):
    """epoch 밀리초를 UTC datetime으로 바꾼다.

    Args:
        ms: epoch 밀리초 또는 None.

    Returns:
        timezone이 붙은 datetime 또는 None.
    """
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / MS_PER_SEC, tz=timezone.utc)

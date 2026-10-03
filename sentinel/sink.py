"""윈도우 집계 결과를 PostgreSQL에 증분·멱등으로 적재한다.

도메인 비의존. 컬럼 이름의 의미를 알지 못하고, 설정으로 받은 키 컬럼과
수치 컬럼으로 테이블을 만든다.

스트림 경로의 적재에 DuckDB 대신 PostgreSQL을 쓰는 이유는 동시 쓰기다.
DuckDB는 파일 하나에 한 프로세스만 쓸 수 있어, 같은 컨슈머 그룹의
컨슈머 여러 개가 동시에 적재할 수 없다.

멱등성은 (키, ts) 기본키로 보장한다. 같은 결과가 여러 번 들어와도 한 번만
저장되므로, 장애 후 재처리로 같은 레코드를 다시 읽어도 결과가 오염되지 않는다.
"""

import psycopg
from psycopg import sql


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
        """테이블이 없으면 만든다.

        ingested_at은 적재 시각이다. 이벤트 시각(ts)과의 차이로
        처리 지연을 측정하는 데 쓴다.
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
            sql.SQL("ingested_at timestamptz NOT NULL DEFAULT now()"),
            sql.SQL("PRIMARY KEY ({}, ts)").format(
                sql.Identifier(self.key_column)),
        ]
        self.conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(self.table), sql.SQL(", ").join(cols)))

    def _build_insert(self):
        """배치 전체를 한 문장으로 넣는 INSERT 문을 만든다.

        컬럼별 배열을 unnest로 펼쳐 한 번에 넣는다. 행마다 INSERT를
        보내는 것보다 왕복이 적고, rowcount로 실제 삽입 건수를 바로 얻는다.

        Returns:
            psycopg sql.Composed.
        """
        names = [self.key_column, "ts", *self.value_columns, "label", "scenario"]
        types = ["text", "timestamp", *["float8"] * len(self.value_columns),
                 "int4", "text"]
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

    def truncate(self):
        """테이블을 비운다. 실험을 처음부터 다시 할 때 쓴다."""
        self.conn.execute(sql.SQL("TRUNCATE {}").format(
            sql.Identifier(self.table)))

    def drop(self):
        """테이블을 지운다. 테스트가 끝난 뒤 정리할 때 쓴다."""
        self.conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(self.table)))

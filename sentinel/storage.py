"""알람과 지표 이력을 저장하고 조회한다.

도메인 비의존. 컬럼 이름의 의미를 알지 못하고, 주어진 DataFrame을
그대로 적재한 뒤 SQL로 되돌려준다.

CSV 대신 DuckDB를 쓰는 이유는 조회 때문이다. 리포트를 만들 때
특정 구간의 알람만 뽑거나 KPI별 통계를 집계해야 하는데, 매번 전체
파일을 읽어 필터링하는 것보다 질의가 명확하다.
"""

from pathlib import Path

import duckdb


class Store:
    """DuckDB 파일 하나를 다루는 저장소.

    with 문으로 쓰면 연결이 자동으로 닫힌다.
    """

    def __init__(self, path):
        """저장소를 연다. 파일이 없으면 새로 만든다.

        Args:
            path: DuckDB 파일 경로.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(path))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        """연결을 닫는다."""
        self.con.close()

    def write(self, table, df):
        """DataFrame을 테이블에 덮어쓴다.

        기존 테이블이 있으면 지우고 다시 만든다. 이 프로젝트는
        매번 전체를 다시 생성하므로 증분 적재를 하지 않는다.

        Args:
            table: 테이블 이름.
            df: 적재할 DataFrame.

        Returns:
            적재된 행 수.
        """
        self.con.register("_tmp", df)
        self.con.execute(f"DROP TABLE IF EXISTS {table}")
        self.con.execute(f"CREATE TABLE {table} AS SELECT * FROM _tmp")
        self.con.unregister("_tmp")
        return len(df)

    def query(self, sql, params=None):
        """SQL을 실행해 DataFrame으로 돌려준다.

        Args:
            sql: 질의문.
            params: 바인딩할 파라미터 목록.

        Returns:
            결과 DataFrame.
        """
        return self.con.execute(sql, params or []).df()

    def tables(self):
        """저장된 테이블 이름 목록을 돌려준다."""
        return self.con.execute("SHOW TABLES").df()["name"].tolist()
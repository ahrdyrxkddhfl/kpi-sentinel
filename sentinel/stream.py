"""스트림 소스 추상.

sentinel은 레코드가 어디서 오는지 알지 못한다. Kafka든 파일이든
dict를 하나씩 내놓는 반복자이기만 하면 된다. 이 경계 덕분에
브로커를 파일 재생으로 교체해도 하위 모듈은 변경되지 않는다.
"""

import json
from pathlib import Path


def replay_csv(path, kpi_names):
    """CSV를 한 행씩 dict로 내놓는다.

    브로커를 쓰지 않을 때의 대체 소스다.

    Args:
        path: CSV 경로.
        kpi_names: 숫자로 변환할 컬럼 이름 목록.

    Yields:
        {"ts": str, <kpi>: float, "label": int, "scenario": str} 형태의 dict.
    """
    import csv

    with Path(path).open(newline="") as f:
        for row in csv.DictReader(f):
            rec = {"ts": row["ts"], "label": int(row["label"]),
                   "scenario": row["scenario"]}
            for k in kpi_names:
                rec[k] = float(row[k])
            yield rec


def decode(payload):
    """브로커에서 받은 바이트를 dict로 되돌린다.

    Args:
        payload: UTF-8로 인코딩된 JSON 바이트.

    Returns:
        dict.
    """
    return json.loads(payload.decode("utf-8"))


def encode(record):
    """dict를 브로커로 보낼 바이트로 만든다.

    Args:
        record: 직렬화 가능한 dict.

    Returns:
        bytes.
    """
    return json.dumps(record, ensure_ascii=False).encode("utf-8")
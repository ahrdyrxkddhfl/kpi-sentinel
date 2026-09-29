"""다중 셀 토픽을 읽어 셀별로 윈도우 집계하고 PostgreSQL에 적재한다.

레코드는 배치 단위로 처리한다. 배치 하나를 읽으면 셀별 윈도우에 넣고,
나온 집계 결과를 한 번에 적재한다.

오프셋 커밋은 아직 클라이언트 기본값(주기적 자동 커밋)을 따른다.
자동 커밋은 적재 여부와 관계없이 커밋하므로 컨슈머가 죽으면 유실이
생길 수 있다. 다음 단계에서 이 동작을 재현하고 고친다.
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.sink import PostgresSink
from sentinel.stream import decode
from sentinel.windowing import KeyedSlidingWindow

ROOT = Path(__file__).resolve().parents[1]


def kafka_batches(bootstrap, topic, group_id, batch_size, idle_timeout):
    """브로커에서 레코드를 배치 단위로 읽는다.

    Args:
        bootstrap: 브로커 주소.
        topic: 토픽 이름.
        group_id: 컨슈머 그룹 이름.
        batch_size: 한 번에 가져올 최대 메시지 수.
        idle_timeout: 이 시간(초) 동안 새 메시지가 없으면 종료한다.

    Yields:
        레코드 dict 목록.
    """
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    idle = 0
    try:
        while idle < idle_timeout:
            msgs = consumer.consume(num_messages=batch_size, timeout=1.0)
            if not msgs:
                idle += 1
                continue
            idle = 0
            batch = []
            for m in msgs:
                if m.error():
                    print("error:", m.error())
                    continue
                batch.append(decode(m.value()))
            yield batch
    finally:
        consumer.close()


def process(batches, windows, sink):
    """배치를 윈도우에 넣고 결과를 적재한다.

    레코드 소스와 분리해 두어, 브로커 없이도 같은 처리를 검증할 수 있다.

    Args:
        batches: 레코드 dict 목록을 내놓는 반복자.
        windows: KeyedSlidingWindow.
        sink: write(rows)를 가진 적재 대상.

    Returns:
        received, emitted, inserted, ignored, per_key(키별 수신 건수 Counter)를
        담은 dict.
    """
    stats = {"received": 0, "emitted": 0, "inserted": 0, "ignored": 0,
             "per_key": Counter()}
    for batch in batches:
        out = []
        for rec in batch:
            stats["received"] += 1
            stats["per_key"][rec[windows.key_column]] += 1
            row = windows.push(rec)
            if row is not None:
                out.append(row)
        inserted, ignored = sink.write(out)
        stats["emitted"] += len(out)
        stats["inserted"] += inserted
        stats["ignored"] += ignored
    return stats


def expected_rows(n, size, stride):
    """레코드 n건에서 나와야 하는 윈도우 결과 건수를 계산한다.

    Args:
        n: 수신 레코드 수.
        size: 윈도우 크기.
        stride: 집계 간격.

    Returns:
        int.
    """
    return 0 if n < size else (n - size) // stride + 1


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reset", action="store_true",
                        help="적재 테이블을 비우고 시작한다")
    args = parser.parse_args()

    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())
    pg = yaml.safe_load((ROOT / "config/storage.yaml").read_text())["postgres"]
    kpi_names = list(kpi_yaml["kpis"])
    me = stream_cfg["multi_entity"]

    interval_min = kpi_yaml["sampling"]["interval_sec"] // 60
    w = det_yaml["window"]
    size = w["size_min"] // interval_min
    stride = w["stride_min"] // interval_min
    windows = KeyedSlidingWindow(me["id_column"], size, stride, kpi_names,
                                 w["agg"])

    with PostgresSink(pg["dsn"], pg["table"], me["id_column"],
                      kpi_names) as sink:
        if args.reset:
            sink.truncate()
            print(f"테이블 초기화: {pg['table']}")

        batches = kafka_batches(
            stream_cfg["kafka"]["bootstrap_servers"], me["topic"],
            me["group_id"], me["batch_size"], me["idle_timeout_sec"],
        )
        stats = process(batches, windows, sink)
        stored = sink.count_by_key()

    if stats["received"] == 0:
        print(f"수신된 레코드가 없다. (topic={me['topic']})")
        print("이미 읽은 오프셋일 수 있다. 컨슈머 그룹을 삭제하고 재시도할 것:")
        print("  docker exec kpi-sentinel-redpanda "
              f"rpk group delete {me['group_id']}")
        return 1

    print(f"수신 {stats['received']:,}건 -> 윈도우 결과 {stats['emitted']:,}건")
    print(f"  적재 {stats['inserted']:,}건, 중복 무시 {stats['ignored']:,}건")
    print(f"\n셀별 정합성 (테이블 {pg['table']}):")
    ok = True
    for key in sorted(stats["per_key"]):
        n = stats["per_key"][key]
        exp = expected_rows(n, size, stride)
        got = stored.get(key, 0)
        mark = "일치" if exp == got else "불일치"
        ok &= exp == got
        print(f"  {key}  수신 {n:,}  기대 {exp:,}  적재 {got:,}  {mark}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

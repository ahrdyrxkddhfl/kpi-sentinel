"""다중 셀 데이터를 셀 ID를 키로 삼아 여러 파티션에 나눠 보낸다.

같은 키는 항상 같은 파티션으로 가므로 한 셀 안의 순서는 보장된다.
파티션끼리의 순서는 보장되지 않지만, 윈도우 집계를 셀 단위로 하므로
셀 안의 순서만 지켜지면 충분하다.

전송이 끝나면 셀마다 실제로 어느 파티션에 들어갔는지 확인한다.
한 셀이 두 파티션에 나뉘어 들어갔다면 순서 보장이 깨진 것이다.
"""

import sys
from collections import Counter, defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.stream import encode, replay_csv

ROOT = Path(__file__).resolve().parents[1]


def ensure_topic(bootstrap, topic, partitions):
    """토픽이 없으면 만들고, 있으면 파티션 수가 설정과 같은지 확인한다.

    파티션 수가 바뀌면 키 해시가 가리키는 파티션도 바뀐다. 기존 데이터와
    새 데이터에서 같은 셀이 서로 다른 파티션에 들어가 순서가 깨지므로,
    다르면 자동으로 늘리지 않고 중단한다.

    Args:
        bootstrap: 브로커 주소.
        topic: 토픽 이름.
        partitions: 기대하는 파티션 수.

    Returns:
        설정과 일치하면 True, 파티션 수가 다르면 False.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    meta = admin.list_topics(timeout=10)
    if topic in meta.topics:
        actual = len(meta.topics[topic].partitions)
        if actual != partitions:
            print(f"토픽 {topic}의 파티션 수가 {actual}개다. 설정은 {partitions}개.")
            print("  토픽을 지우고 다시 실행할 것:")
            print(f"  docker exec -it kpi-sentinel-redpanda rpk topic delete {topic}")
            return False
        return True

    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    futures[topic].result()
    print(f"토픽 생성: {topic} (파티션 {partitions}개)")
    return True


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    kpi_names = list(kpi_yaml["kpis"])
    me = stream_cfg["multi_entity"]
    bootstrap = stream_cfg["kafka"]["bootstrap_servers"]
    key_col = me["id_column"]

    src = ROOT / me["path"]
    if not src.exists():
        print(f"원본 데이터가 없다: {src}")
        print("먼저 scripts/run_simulate_cells.py를 실행할 것.")
        return 1

    if not ensure_topic(bootstrap, me["topic"], me["partitions"]):
        return 1

    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "partitioner": me["partitioner"],
    })

    placed = defaultdict(Counter)   # 셀 -> {파티션: 건수}
    failed = 0

    def on_delivery(err, msg):
        """브로커의 전송 결과를 기록한다."""
        nonlocal failed
        if err is not None:
            failed += 1
            return
        placed[msg.key().decode("utf-8")][msg.partition()] += 1

    sent = 0
    for rec in replay_csv(src, kpi_names, key_column=key_col):
        producer.produce(
            me["topic"],
            key=rec[key_col].encode("utf-8"),
            value=encode(rec),
            on_delivery=on_delivery,
        )
        sent += 1
        if sent % 5000 == 0:
            producer.poll(0)
            print(f"  sent {sent:,}")
    producer.flush()

    print(f"\n총 {sent:,}건 전송, 실패 {failed}건 -> topic={me['topic']}")
    print("\n셀 -> 파티션 배치:")
    per_partition = Counter()
    split_cells = []
    for cell in sorted(placed):
        parts = placed[cell]
        per_partition.update(parts)
        if len(parts) > 1:
            split_cells.append(cell)
        desc = ", ".join(f"p{p}: {n:,}건" for p, n in sorted(parts.items()))
        print(f"  {cell}  {desc}")

    print("\n파티션별 건수:")
    for p in range(me["partitions"]):
        print(f"  p{p}: {per_partition.get(p, 0):,}건")

    if split_cells:
        print(f"\n경고: 여러 파티션에 나뉜 셀이 있다: {split_cells}")
        return 1
    print("\n모든 셀이 하나의 파티션에만 들어갔다. 셀 단위 순서 보장 확인.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

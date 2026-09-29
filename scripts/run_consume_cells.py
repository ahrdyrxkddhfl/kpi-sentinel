"""다중 셀 토픽을 읽어 셀별로 윈도우 집계하고 PostgreSQL에 적재한다.

레코드는 배치 단위로 처리한다. 배치 하나를 읽으면 셀별 윈도우에 넣고,
나온 집계 결과를 한 번에 적재한다.

오프셋 커밋 시점은 전략으로 고를 수 있다. 유실이 어디서 생기는지
비교하기 위해 일부러 잘못된 전략도 남겨 두었다.

    auto          클라이언트가 주기적으로 커밋한다. 적재 여부와 무관하다.
    before_write  읽자마자 커밋한다. 적재 전에 죽으면 그 배치가 사라진다.
    after_write   적재 후 마지막으로 읽은 위치를 커밋한다. 윈도우 버퍼가
                  메모리에만 있어, 재시작 후 버퍼를 다시 채우는 동안의
                  결과가 사라진다.
    window_safe   적재 후, 윈도우 버퍼를 복구할 수 있는 위치까지만
                  커밋한다. 일부를 다시 읽지만 적재가 멱등이라 결과는
                  중복되지 않는다.

--crash-after N을 주면 N번째 레코드를 처리한 뒤 프로세스를 강제 종료한다.
정리 코드 없이 죽는 상황을 흉내 낸다. 죽는 시점은 --crash-stage로 고른다.

    pre_write     N번째 레코드를 윈도우에 넣은 직후, 그 배치를 적재하기 전
    pre_commit    N번째 레코드가 든 배치를 적재한 직후, 커밋하기 전
"""

import argparse
import csv
import json
import os
import sys
from collections import Counter
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.checkpoint import ResumeTracker
from sentinel.sink import PostgresSink
from sentinel.stream import decode
from sentinel.windowing import KeyedSlidingWindow

ROOT = Path(__file__).resolve().parents[1]
STRATEGIES = ("auto", "before_write", "after_write", "window_safe")
CRASH_STAGES = ("pre_write", "pre_commit")
CRASH_EXIT_CODE = 3


def next_positions(batch):
    """배치에서 파티션별로 다음에 읽을 위치를 구한다.

    Args:
        batch: (partition, offset, record) 튜플 목록.

    Returns:
        파티션 -> 마지막 오프셋 + 1 dict.
    """
    out = {}
    for partition, offset, _ in batch:
        out[partition] = max(out.get(partition, 0), offset + 1)
    return out


class CommitStrategy:
    """오프셋 커밋 시점을 정하는 전략의 공통 인터페이스.

    처리 흐름의 세 지점에서 훅이 호출된다. 기본 구현은 아무것도 하지 않는다.

    Attributes:
        commit_fn: 파티션 -> 위치 dict를 받아 동기로 커밋하는 함수.
    """

    def __init__(self, commit_fn):
        """전략을 초기화한다.

        Args:
            commit_fn: 파티션 -> 위치 dict를 받아 커밋하는 함수.
        """
        self.commit_fn = commit_fn

    def after_read(self, batch):
        """배치를 읽은 직후, 처리하기 전에 호출된다."""

    def after_push(self, partition, offset, key, emitted):
        """레코드 하나를 윈도우에 넣은 직후 호출된다."""

    def after_write(self, batch):
        """배치의 윈도우 결과를 적재한 직후 호출된다."""


class CommitBeforeWrite(CommitStrategy):
    """읽자마자 커밋한다. 최대 한 번 처리(at-most-once)에 해당한다."""

    def after_read(self, batch):
        self.commit_fn(next_positions(batch))


class CommitAfterWrite(CommitStrategy):
    """적재 후 마지막으로 읽은 위치를 커밋한다."""

    def after_write(self, batch):
        self.commit_fn(next_positions(batch))


class CommitWindowSafe(CommitStrategy):
    """적재 후 윈도우 버퍼를 복구할 수 있는 위치까지만 커밋한다."""

    def __init__(self, commit_fn, size, stride):
        """전략을 초기화한다.

        Args:
            commit_fn: 파티션 -> 위치 dict를 받아 커밋하는 함수.
            size: 윈도우 크기 (레코드 수).
            stride: 집계 간격 (레코드 수).
        """
        super().__init__(commit_fn)
        self.tracker = ResumeTracker(size, stride)

    def after_push(self, partition, offset, key, emitted):
        self.tracker.observe(partition, key, offset, emitted)

    def after_write(self, batch):
        self.commit_fn(self.tracker.safe_positions())


def make_strategy(name, commit_fn, size, stride):
    """이름으로 커밋 전략을 만든다.

    Args:
        name: STRATEGIES 중 하나.
        commit_fn: 파티션 -> 위치 dict를 받아 커밋하는 함수.
        size: 윈도우 크기 (레코드 수).
        stride: 집계 간격 (레코드 수).

    Returns:
        CommitStrategy.

    Raises:
        ValueError: 알 수 없는 전략 이름일 때.
    """
    if name == "auto":
        return CommitStrategy(commit_fn)
    if name == "before_write":
        return CommitBeforeWrite(commit_fn)
    if name == "after_write":
        return CommitAfterWrite(commit_fn)
    if name == "window_safe":
        return CommitWindowSafe(commit_fn, size, stride)
    raise ValueError(f"알 수 없는 커밋 전략: {name}")


def process(batches, windows, sink, strategy, crash_after=None,
            crash_stage="pre_write", crash=None):
    """배치를 윈도우에 넣고 결과를 적재한다.

    레코드 소스와 분리해 두어, 브로커 없이도 같은 처리를 검증할 수 있다.

    Args:
        batches: (partition, offset, record) 튜플 목록을 내놓는 반복자.
        windows: KeyedSlidingWindow.
        sink: write(rows)를 가진 적재 대상.
        strategy: CommitStrategy.
        crash_after: 이 건수의 레코드를 처리한 뒤 crash를 호출한다.
            None이면 호출하지 않는다.
        crash_stage: CRASH_STAGES 중 하나. crash를 호출할 시점이다.
        crash: 그 시점까지의 통계 dict를 받아 호출되는 함수.
            기본값은 프로세스 강제 종료.

    Returns:
        received, emitted, inserted, ignored를 담은 dict.

    Raises:
        ValueError: crash_stage가 알 수 없는 값일 때.
    """
    if crash_stage not in CRASH_STAGES:
        raise ValueError(f"알 수 없는 종료 시점: {crash_stage}")
    if crash is None:
        def crash(_stats):
            os._exit(CRASH_EXIT_CODE)

    def reached():
        return crash_after is not None and stats["received"] >= crash_after

    stats = {"received": 0, "emitted": 0, "inserted": 0, "ignored": 0}
    for batch in batches:
        strategy.after_read(batch)
        out = []
        for partition, offset, rec in batch:
            stats["received"] += 1
            row = windows.push(rec)
            strategy.after_push(partition, offset, rec[windows.key_column],
                                row is not None)
            if row is not None:
                out.append(row)
            if crash_stage == "pre_write" and reached():
                crash(stats)
        inserted, ignored = sink.write(out)
        if crash_stage == "pre_commit" and reached():
            crash(stats)
        strategy.after_write(batch)
        stats["emitted"] += len(out)
        stats["inserted"] += inserted
        stats["ignored"] += ignored
    return stats


def expected_rows(n, size, stride):
    """레코드 n건에서 나와야 하는 윈도우 결과 건수를 계산한다.

    Args:
        n: 레코드 수.
        size: 윈도우 크기.
        stride: 집계 간격.

    Returns:
        int.
    """
    return 0 if n < size else (n - size) // stride + 1


def expected_by_key(path, key_column, size, stride):
    """원본 파일 기준으로 키별 기대 적재 건수를 구한다.

    Args:
        path: 원본 CSV 경로.
        key_column: 개체 식별자 컬럼 이름.
        size: 윈도우 크기.
        stride: 집계 간격.

    Returns:
        키 -> 기대 건수 dict.
    """
    with Path(path).open(newline="") as f:
        counts = Counter(row[key_column] for row in csv.DictReader(f))
    return {k: expected_rows(n, size, stride) for k, n in counts.items()}


def open_consumer(bootstrap, topic, group_id, auto_commit, session_timeout_ms):
    """컨슈머를 만들고 구독한다.

    Args:
        bootstrap: 브로커 주소.
        topic: 토픽 이름.
        group_id: 컨슈머 그룹 이름.
        auto_commit: 자동 커밋 사용 여부.
        session_timeout_ms: 이 시간 동안 하트비트가 없으면 그룹에서 제외된다.
            강제 종료된 컨슈머가 파티션을 오래 붙잡지 않도록 짧게 둔다.

    Returns:
        (consumer, assigned) 튜플. assigned는 파티션을 할당받았는지를
        담은 dict이다.
    """
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": auto_commit,
        "session.timeout.ms": session_timeout_ms,
        "heartbeat.interval.ms": session_timeout_ms // 3,
    })
    assigned = {"yes": False}

    def on_assign(_consumer, partitions):
        assigned["yes"] = bool(partitions)

    consumer.subscribe([topic], on_assign=on_assign)
    return consumer, assigned


def kafka_batches(consumer, assigned, batch_size, idle_timeout, startup_timeout):
    """브로커에서 레코드를 배치 단위로 읽는다.

    파티션을 할당받기 전에는 startup_timeout까지 기다린다. 직전에 강제
    종료된 컨슈머가 그룹에서 빠질 때까지 할당이 늦어질 수 있기 때문이다.

    Args:
        consumer: 구독을 마친 컨슈머.
        assigned: open_consumer가 돌려준 할당 여부 dict.
        batch_size: 한 번에 가져올 최대 메시지 수.
        idle_timeout: 할당 후 이 시간(초) 동안 새 메시지가 없으면 종료한다.
        startup_timeout: 할당을 기다리는 최대 시간(초).

    Yields:
        (partition, offset, record) 튜플 목록.
    """
    idle, waited = 0, 0
    while True:
        msgs = consumer.consume(num_messages=batch_size, timeout=1.0)
        if not msgs:
            if assigned["yes"]:
                idle += 1
                if idle >= idle_timeout:
                    return
            else:
                waited += 1
                if waited >= startup_timeout:
                    print("파티션을 할당받지 못했다.")
                    return
            continue
        idle = 0
        batch = []
        for m in msgs:
            if m.error():
                print("error:", m.error())
                continue
            batch.append((m.partition(), m.offset(), decode(m.value())))
        if batch:
            yield batch


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reset", action="store_true",
                        help="적재 테이블을 비우고 시작한다")
    parser.add_argument("--strategy", choices=STRATEGIES,
                        help="오프셋 커밋 전략. 생략하면 설정값을 쓴다")
    parser.add_argument("--group", help="컨슈머 그룹. 생략하면 설정값을 쓴다")
    parser.add_argument("--crash-after", type=int,
                        help="이 건수를 처리한 뒤 강제 종료한다")
    parser.add_argument("--crash-stage", choices=CRASH_STAGES,
                        default="pre_write", help="강제 종료 시점")
    parser.add_argument("--stats-out", help="처리 통계를 JSON으로 저장할 경로")
    args = parser.parse_args()

    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())
    pg = yaml.safe_load((ROOT / "config/storage.yaml").read_text())["postgres"]
    kpi_names = list(kpi_yaml["kpis"])
    me = stream_cfg["multi_entity"]
    key_col = me["id_column"]
    strategy_name = args.strategy or me["commit_strategy"]
    group_id = args.group or me["group_id"]

    interval_min = kpi_yaml["sampling"]["interval_sec"] // 60
    w = det_yaml["window"]
    size = w["size_min"] // interval_min
    stride = w["stride_min"] // interval_min
    windows = KeyedSlidingWindow(key_col, size, stride, kpi_names, w["agg"])

    from confluent_kafka import TopicPartition

    consumer, assigned = open_consumer(
        stream_cfg["kafka"]["bootstrap_servers"], me["topic"], group_id,
        strategy_name == "auto", me["session_timeout_ms"],
    )

    def commit(positions):
        if positions:
            consumer.commit(
                offsets=[TopicPartition(me["topic"], p, o)
                         for p, o in positions.items()],
                asynchronous=False,
            )

    strategy = make_strategy(strategy_name, commit, size, stride)

    def crash(stats):
        """통계를 남기고 정리 코드 없이 즉시 종료한다."""
        if args.stats_out:
            Path(args.stats_out).write_text(json.dumps(stats))
        os._exit(CRASH_EXIT_CODE)
    print(f"커밋 전략: {strategy_name}  그룹: {group_id}")

    with PostgresSink(pg["dsn"], pg["table"], key_col, kpi_names) as sink:
        if args.reset:
            sink.truncate()
            print(f"테이블 초기화: {pg['table']}")
        try:
            batches = kafka_batches(consumer, assigned, me["batch_size"],
                                    me["idle_timeout_sec"],
                                    me["startup_timeout_sec"])
            stats = process(batches, windows, sink, strategy,
                            crash_after=args.crash_after,
                            crash_stage=args.crash_stage, crash=crash)
        finally:
            consumer.close()
        stored = sink.count_by_key()

    expected = expected_by_key(ROOT / me["path"], key_col, size, stride)
    stats["stored_total"] = sum(stored.values())
    stats["expected_total"] = sum(expected.values())
    if args.stats_out:
        Path(args.stats_out).write_text(json.dumps(stats))

    print(f"수신 {stats['received']:,}건 -> 윈도우 결과 {stats['emitted']:,}건")
    print(f"  적재 {stats['inserted']:,}건, 중복 무시 {stats['ignored']:,}건")
    print(f"\n셀별 정합성 (테이블 {pg['table']}):")
    ok = True
    for key in sorted(expected):
        exp, got = expected[key], stored.get(key, 0)
        ok &= exp == got
        mark = "일치" if exp == got else f"누락 {exp - got:,}건"
        print(f"  {key}  기대 {exp:,}  적재 {got:,}  {mark}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

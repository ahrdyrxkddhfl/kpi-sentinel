"""스트림에서 레코드를 읽어 슬라이딩 윈도우로 집계한다.

배치 경로(run_window.py)와 같은 결과를 내되, 전체 데이터를 메모리에
올리지 않고 도착하는 레코드를 하나씩 처리한다. SlidingWindow가
레코드 단위 push 방식인 이유가 이것이다.

브로커가 없거나 응답하지 않으면 파일 재생으로 대체한다. 스트리밍
계층은 교체 가능한 부품이며, 하위 단계는 어느 쪽이든 동일하게 동작한다.
"""

import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.stream import decode, replay_csv
from sentinel.windowing import SlidingWindow

ROOT = Path(__file__).resolve().parents[1]


def iter_kafka(cfg, kpi_names, timeout=5.0):
    """브로커에서 레코드를 하나씩 읽는다.

    Args:
        cfg: stream.yaml의 kafka 설정.
        kpi_names: 사용하지 않는다. 파일 재생과 시그니처를 맞추기 위한 것이다.
        timeout: 이 시간 동안 새 메시지가 없으면 종료한다.

    Yields:
        레코드 dict.
    """
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": cfg["bootstrap_servers"],
        "group.id": cfg["group_id"],
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([cfg["topic"]])
    try:
        while True:
            msg = consumer.poll(timeout=timeout)
            if msg is None:
                break
            if msg.error():
                print("error:", msg.error())
                continue
            yield decode(msg.value())
    finally:
        consumer.close()


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    det_yaml = yaml.safe_load((ROOT / "config/detector.yaml").read_text())
    kpi_names = list(kpi_yaml["kpis"])

    mode = stream_cfg["mode"]
    if mode == "kafka":
        source = iter_kafka(stream_cfg["kafka"], kpi_names)
        origin = f"topic={stream_cfg['kafka']['topic']}"
    else:
        src = ROOT / stream_cfg["file"]["path"]
        if not src.exists():
            print(f"원본 데이터가 없다: {src}")
            return 1
        source = replay_csv(src, kpi_names)
        origin = str(src)

    interval_min = kpi_yaml["sampling"]["interval_sec"] // 60
    w = det_yaml["window"]
    window = SlidingWindow(
        w["size_min"] // interval_min,
        w["stride_min"] // interval_min,
        kpi_names,
        w["agg"],
    )

    received, emitted, n_anom = 0, 0, 0
    first_ts, last_ts = None, None
    rows = []

    for rec in source:
        received += 1
        if first_ts is None:
            first_ts = rec["ts"]
        last_ts = rec["ts"]
        out = window.push(rec)
        if out is not None:
            emitted += 1
            n_anom += out.get("label", 0)
            rows.append(out)

    if received == 0:
        print(f"수신된 레코드가 없다. ({origin})")
        if mode == "kafka":
            print("이미 읽은 오프셋일 수 있다. 컨슈머 그룹을 삭제하고 재시도할 것:")
            print("  docker exec -it kpi-sentinel-redpanda "
                  f"rpk group delete {stream_cfg['kafka']['group_id']}")
        return 1

    out_path = ROOT / "data/windowed/kpi_stream.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)

    print(f"소스: {origin}")
    print(f"  수신     {received:,}건  ({first_ts} ~ {last_ts})")
    print(f"  집계 출력 {emitted:,}건")
    print(f"  이상 라벨 {n_anom:,}건 ({n_anom / emitted * 100:.2f}%)")
    print(f"  -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
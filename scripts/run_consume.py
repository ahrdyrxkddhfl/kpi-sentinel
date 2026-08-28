"""스트림에서 레코드를 읽어 건수와 라벨 분포를 확인한다.

관통 확인용이다. 윈도우 집계는 아직 붙이지 않는다.
"""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.stream import decode

ROOT = Path(__file__).resolve().parents[1]


def main():
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    kc = stream_cfg["kafka"]

    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": kc["bootstrap_servers"],
        "group.id": kc["group_id"],
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([kc["topic"]])

    n, n_anom = 0, 0
    first_ts, last_ts = None, None
    try:
        while True:
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                break
            if msg.error():
                print("error:", msg.error())
                continue
            rec = decode(msg.value())
            n += 1
            n_anom += rec["label"]
            if first_ts is None:
                first_ts = rec["ts"]
            last_ts = rec["ts"]
    finally:
        consumer.close()

    print(f"수신 {n:,}건")
    if n:
        print(f"  기간   : {first_ts} ~ {last_ts}")
        print(f"  이상 라벨: {n_anom:,}건 ({n_anom / n * 100:.2f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
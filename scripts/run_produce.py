"""생성된 CSV를 스트림으로 흘려보낸다.

브로커 접속은 실행 환경의 문제이므로 sentinel 밖에 둔다.
"""

import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.stream import encode, replay_csv

ROOT = Path(__file__).resolve().parents[1]


def main():
    kpi_yaml = yaml.safe_load((ROOT / "config/kpi.yaml").read_text())
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    kpi_names = list(kpi_yaml["kpis"])

    src = ROOT / stream_cfg["file"]["path"]
    if not src.exists():
        print(f"원본 데이터가 없다: {src}")
        print("먼저 scripts/run_simulate.py를 실행할 것.")
        return 1

    from confluent_kafka import Producer

    kc = stream_cfg["kafka"]
    producer = Producer({"bootstrap.servers": kc["bootstrap_servers"]})
    delay = stream_cfg["replay_delay_sec"]

    sent = 0
    for rec in replay_csv(src, kpi_names):
        producer.produce(kc["topic"], value=encode(rec))
        sent += 1
        if sent % 5000 == 0:
            producer.poll(0)
            print(f"  sent {sent:,}")
        if delay:
            time.sleep(delay)

    producer.flush()
    print(f"총 {sent:,}건 전송 -> topic={kc['topic']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
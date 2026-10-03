"""리밸런스 때 커밋 위치와 윈도우 버퍼가 꼬이지 않는지 브로커 없이 검증한다.

컨슈머 A가 파티션 0·1을 처리하다 리밸런스로 1을 B에게 넘기고, B가 처리 중
죽어서 1이 다시 A에게 돌아오는 상황을 그대로 흉내 낸다. 브로커 대신
파티션 로그는 리스트로, 커밋 저장소는 dict로 둔다.
"""

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from run_consume_cells import make_strategy, process  # noqa: E402
from sentinel.windowing import KeyedSlidingWindow, aggregate_frame  # noqa: E402

import pandas as pd  # noqa: E402

KEY = "cell_id"
KPIS = ["v"]
SIZE, STRIDE = 5, 1
BATCH = 7                      # 배치 크기. 셀 경계와 일부러 어긋나게 둔다
KEYS_OF = {0: ["c0", "c1", "c2"], 1: ["c3", "c4", "c5"]}
N_PER_KEY = 40


def make_logs():
    """파티션별 로그를 만든다. 한 파티션 안에서 셀들이 번갈아 들어온다."""
    rng = np.random.default_rng(0)
    logs = {}
    for p, keys in KEYS_OF.items():
        logs[p] = [{KEY: k, "ts": f"2026-08-03 00:{i:02d}", "v": float(rng.normal())}
                   for i in range(N_PER_KEY) for k in keys]
    return logs


class FakeSink:
    """(키, ts) 기본키에 ON CONFLICT DO NOTHING으로 적재하는 가짜 저장소."""

    def __init__(self):
        self.rows = {}

    def write(self, rows):
        inserted = 0
        for r in rows:
            pk = (r[KEY], r["ts"])
            if pk not in self.rows:
                self.rows[pk] = r["v"]
                inserted += 1
        return inserted, len(rows) - inserted


class FakeConsumer:
    """파티션 로그를 커밋 위치부터 배치로 읽는 가짜 컨슈머 하나."""

    def __init__(self, logs, committed, history, sink, fix):
        self.logs, self.committed, self.history = logs, committed, history
        self.sink, self.fix = sink, fix
        self.windows = KeyedSlidingWindow(KEY, SIZE, STRIDE, KPIS)
        self.strategy = make_strategy("window_safe", self.commit, SIZE, STRIDE)
        self.pos = {}           # 지금 맡은 파티션 -> 다음에 읽을 위치
        self.read = 0

    def commit(self, positions):
        for p, o in positions.items():
            self.committed[p] = o
            self.history.append((p, o))

    def assign(self, partitions):
        for p in partitions:
            self.pos[p] = self.committed.get(p, 0)

    def revoke(self, partitions):
        if self.fix:
            self.windows.drop(self.strategy.release(partitions))
        for p in partitions:
            self.pos.pop(p, None)

    def run(self, n_batches):
        """맡은 파티션을 번갈아 n_batches 배치만큼 읽어 처리한다."""
        def batches():
            for _ in range(n_batches):
                batch = []
                for p in sorted(self.pos):
                    log, start = self.logs[p], self.pos[p]
                    for off in range(start, min(start + BATCH, len(log))):
                        batch.append((p, off, log[off]))
                    self.pos[p] = min(start + BATCH, len(log))
                if not batch:
                    return
                self.read += len(batch)
                yield batch
        process(batches(), self.windows, self.sink, self.strategy)


def run_scenario(fix):
    """A가 1을 B에게 넘기고, B가 죽어 1이 A에게 돌아오는 흐름을 실행한다."""
    logs = make_logs()
    committed, history, sink = {}, [], FakeSink()
    a = FakeConsumer(logs, committed, history, sink, fix)
    b = FakeConsumer(logs, committed, history, sink, fix)

    a.assign([0, 1]); a.run(5)                 # A 혼자 처리
    a.revoke([0, 1]); a.assign([0])            # B 합류: 전부 반납 후 재분배 (eager)
    b.assign([1]); b.run(8)                    # B가 파티션 1을 한참 처리하다
    a.run(3)                                   # 그동안 A도 파티션 0을 처리
    a.revoke([0]); a.assign([0, 1])            # B가 죽어 1이 A에게 돌아옴
    a.run(100)                                 # 끝까지
    return committed, history, sink, a.read + b.read


def expected_rows():
    """셀별 배치 집계로 만든 정답."""
    logs = make_logs()
    df = pd.DataFrame([r for log in logs.values() for r in log])
    out = {}
    for key, g in df.groupby(KEY):
        for r in aggregate_frame(g, SIZE, STRIDE, KPIS).to_dict("records"):
            out[(key, r["ts"])] = r["v"]
    return out


def regressions(history):
    """같은 파티션의 커밋 위치가 뒤로 간 횟수를 센다."""
    last, n = {}, 0
    for p, o in history:
        if p in last and o < last[p]:
            n += 1
        last[p] = o
    return n


def test_without_release_commits_go_backwards():
    """반납 처리가 없으면 A가 넘겨준 파티션의 옛 위치를 커밋해 B의 위치를 되돌린다."""
    _, history, _, _ = run_scenario(fix=False)
    assert regressions(history) > 0


def test_release_keeps_commits_monotonic():
    """반납 처리가 있으면 어느 파티션의 커밋 위치도 뒤로 가지 않는다."""
    _, history, _, _ = run_scenario(fix=True)
    assert regressions(history) == 0


def test_release_result_matches_batch_and_rereads_less():
    """반납 처리가 있어도 결과는 배치와 같고, 다시 읽는 양은 줄어든다."""
    _, _, sink_fix, read_fix = run_scenario(fix=True)
    _, _, sink_old, read_old = run_scenario(fix=False)
    expected = expected_rows()
    assert sink_fix.rows.keys() == expected.keys()
    assert all(np.isclose(sink_fix.rows[k], v) for k, v in expected.items())
    assert read_fix < read_old

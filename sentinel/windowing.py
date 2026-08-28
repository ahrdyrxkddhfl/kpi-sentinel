"""슬라이딩 윈도우 집계.

도메인 비의존. 컬럼 이름의 의미를 알지 못하고, 주어진 수치 컬럼을
윈도우 단위로 요약한다.

윈도우를 두는 이유는 두 가지다. 단일 시점의 노이즈를 완화하고,
스트림에서 도착하는 레코드를 고정 크기 단위로 묶어 하위 단계가
일정한 형태의 입력을 받게 한다.
"""

from collections import deque

import numpy as np
import pandas as pd

AGG_FUNCS = {
    "mean": np.mean,
    "max": np.max,
    "p95": lambda x: np.percentile(x, 95),
}


class SlidingWindow:
    """고정 크기 슬라이딩 윈도우를 유지하며 집계값을 내놓는다.

    스트림에서 레코드를 하나씩 받아 처리하도록 만들었다. 전체 데이터를
    메모리에 올리지 않으므로 배치와 스트리밍 양쪽에서 동일하게 쓸 수 있다.

    Attributes:
        size: 윈도우에 담을 레코드 수.
        stride: 집계값을 내놓는 간격 (레코드 수).
        keys: 집계 대상 컬럼 이름 목록.
    """

    def __init__(self, size, stride, keys, agg="mean"):
        """윈도우를 초기화한다.

        Args:
            size: 윈도우 크기 (레코드 수).
            stride: 몇 건마다 집계값을 낼지 정한다.
            keys: 집계할 수치 컬럼 이름 목록.
            agg: mean, max, p95 중 하나.

        Raises:
            ValueError: agg가 지원하지 않는 값일 때.
        """
        if agg not in AGG_FUNCS:
            raise ValueError(f"지원하지 않는 집계 방식: {agg}")
        self.size = size
        self.stride = stride
        self.keys = list(keys)
        self._fn = AGG_FUNCS[agg]
        self._buf = deque(maxlen=size)
        self._since_emit = 0

    def push(self, record):
        """레코드 하나를 넣고, 집계 시점이면 결과를 돌려준다.

        윈도우가 아직 채워지지 않았거나 stride에 도달하지 않으면
        None을 돌려준다.

        Args:
            record: 수치 컬럼과 ts를 담은 dict.

        Returns:
            집계 결과 dict 또는 None.
            결과에는 윈도우 마지막 레코드의 ts와 각 컬럼의 집계값이 담긴다.
            label과 scenario는 윈도우 마지막 레코드의 값을 그대로 따른다.
        """
        self._buf.append(record)
        self._since_emit += 1

        if len(self._buf) < self.size or self._since_emit < self.stride:
            return None

        self._since_emit = 0
        last = self._buf[-1]
        out = {"ts": last["ts"]}
        for k in self.keys:
            out[k] = float(self._fn([r[k] for r in self._buf]))
        if "label" in last:
            out["label"] = last["label"]
        if "scenario" in last:
            out["scenario"] = last["scenario"]
        return out


def aggregate_frame(df, size, stride, keys, agg="mean"):
    """DataFrame 전체를 한 번에 집계한다.

    스트리밍이 아닌 배치 처리용이다. SlidingWindow와 같은 결과를 낸다.

    Args:
        df: ts 컬럼과 수치 컬럼을 담은 DataFrame.
        size: 윈도우 크기 (행 수).
        stride: 집계 간격 (행 수).
        keys: 집계할 컬럼 이름 목록.
        agg: mean, max, p95 중 하나.

    Returns:
        집계 결과 DataFrame. 원본보다 앞쪽 size-1 행이 줄어든다.
    """
    win = SlidingWindow(size, stride, keys, agg)
    rows = []
    for rec in df.to_dict("records"):
        out = win.push(rec)
        if out is not None:
            rows.append(out)
    return pd.DataFrame(rows)
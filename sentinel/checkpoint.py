"""재시작 지점 추적.

도메인 비의존. 스트림 소스의 위치를 (파티션, 정수 위치) 쌍으로만 다룬다.

윈도우 집계 컨슈머는 윈도우 버퍼를 메모리에 들고 있다. 적재가 끝난 레코드
바로 다음 위치를 커밋하면, 재시작 후 버퍼가 비어 있어 다음 윈도우를 다시
채우지 못하고 그 사이의 결과가 사라진다. 이 모듈은 버퍼를 복구할 수 있는
가장 늦은 위치, 즉 재시작 시 다시 읽어야 하는 첫 위치를 키마다 추적한다.
"""

from collections import defaultdict, deque


class ResumeTracker:
    """키별 윈도우 상태를 복구하기 위한 재시작 위치를 추적한다.

    키 k의 재시작 위치는 다음과 같이 정한다.
    - 아직 집계 결과를 낸 적이 없으면 k의 첫 레코드 위치.
    - 결과를 낸 적이 있으면, 마지막 결과를 만든 윈도우의 첫 레코드에서
      stride만큼 뒤의 레코드 위치. 여기서 다시 읽으면 원래와 같은 경계로
      다음 윈도우가 만들어진다.

    파티션의 안전한 커밋 위치는 그 파티션에 속한 키들의 재시작 위치 중
    가장 이른 값이다.

    한계: 레코드가 끊긴 키가 있으면 그 키의 재시작 위치가 더 이상 전진하지
    않아 파티션 전체의 커밋이 멈춘다. 재시작 시 다시 읽는 양이 늘어날 뿐
    결과가 틀어지지는 않는다.
    """

    def __init__(self, size, stride):
        """추적기를 초기화한다.

        Args:
            size: 윈도우 크기 (레코드 수).
            stride: 집계 간격 (레코드 수).

        Raises:
            ValueError: stride가 size보다 클 때. 윈도우 사이에 버려지는
                레코드가 생겨 재시작 위치를 레코드 위치만으로 정할 수 없다.
        """
        if stride > size:
            raise ValueError(f"stride({stride})는 size({size}) 이하여야 한다")
        self.size = size
        self.stride = stride
        self._positions = {}        # 키 -> 최근 size개 레코드 위치
        self._resume = {}           # 키 -> 재시작 위치
        self._keys_of = defaultdict(set)   # 파티션 -> 키 집합

    def observe(self, partition, key, position, emitted):
        """레코드 하나가 윈도우에 들어간 결과를 기록한다.

        Args:
            partition: 레코드가 온 파티션.
            key: 레코드의 키.
            position: 파티션 안에서의 레코드 위치 (오프셋).
            emitted: 이 레코드로 윈도우 결과가 나왔는지 여부.
        """
        buf = self._positions.get(key)
        if buf is None:
            buf = deque(maxlen=self.size)
            self._positions[key] = buf
            self._resume[key] = position
            self._keys_of[partition].add(key)
        buf.append(position)
        if emitted:
            if self.stride < self.size:
                self._resume[key] = buf[self.stride]
            else:
                self._resume[key] = position + 1

    def forget(self, partitions):
        """파티션을 반납할 때 그 파티션에 속한 키들의 기록을 지운다.

        지우지 않으면 넘겨준 파티션의 옛 위치가 safe_positions에 계속 남아,
        새 담당 컨슈머가 커밋한 위치를 이 컨슈머가 뒤로 되돌린다.

        Args:
            partitions: 반납하는 파티션 번호 목록.

        Returns:
            기록을 지운 키 집합.
        """
        removed = set()
        for p in partitions:
            keys = self._keys_of.pop(p, set())
            for k in keys:
                self._positions.pop(k, None)
                self._resume.pop(k, None)
            removed |= keys
        return removed

    def safe_positions(self):
        """파티션별로 커밋해도 윈도우 상태를 복구할 수 있는 위치를 돌려준다.

        Returns:
            파티션 -> 다음에 읽을 위치 dict.
        """
        return {p: min(self._resume[k] for k in keys)
                for p, keys in self._keys_of.items()}

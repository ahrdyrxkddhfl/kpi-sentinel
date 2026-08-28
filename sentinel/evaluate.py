"""탐지 성능 평가.

도메인 비의존. 라벨과 알람만 받아 성능 지표를 낸다.

point-wise가 아니라 event-wise로 채점한다. 이유는 이렇다.
장애 구간의 길이는 시나리오마다 8분에서 360분까지 차이가 크다.
시점 단위로 채점하면 긴 장애 하나가 짧은 장애 수십 개만큼의
가중치를 갖게 되어 점수가 왜곡된다. 운용 관점에서도 중요한 것은
"장애를 인지했는가"이지 "몇 분을 맞혔는가"가 아니다.

오탐도 같은 이유로 연속된 알람 덩어리 하나를 1건으로 센다.
운용자에게 5분간 이어진 알람은 5건이 아니라 1건이다.
"""

import numpy as np
import pandas as pd


def find_runs(flags):
    """연속된 True 구간의 시작과 끝을 찾는다.

    Args:
        flags: bool 배열.

    Returns:
        (start, end) 튜플 목록. end는 exclusive.
    """
    flags = np.asarray(flags).astype(bool)
    if not flags.any():
        return []
    padded = np.concatenate(([False], flags, [False]))
    diff = np.diff(padded.astype(int))
    starts = np.flatnonzero(diff == 1)
    ends = np.flatnonzero(diff == -1)
    return list(zip(starts.tolist(), ends.tolist()))


def extract_events(scenario_col, label_col):
    """시나리오 컬럼에서 이벤트 구간을 복원한다.

    같은 시나리오 이름이 연속된 구간을 하나의 이벤트로 본다.

    Args:
        scenario_col: 시나리오 이름 문자열 Series. 정상 구간은 빈 문자열.
        label_col: 정답 라벨 Series.

    Returns:
        {"scenario", "start", "end", "is_anomaly"} 딕셔너리 목록.
    """
    names = pd.Series(scenario_col).fillna("").to_numpy()
    labels = np.asarray(label_col)
    events = []
    i = 0
    while i < len(names):
        if names[i] == "":
            i += 1
            continue
        j = i
        while j < len(names) and names[j] == names[i]:
            j += 1
        events.append({
            "scenario": names[i],
            "start": i,
            "end": j,
            "is_anomaly": bool(labels[i:j].max()),
        })
        i = j
    return events


def evaluate(df, alert_col="alert", interval_min=1):
    """event-wise 성능 지표를 계산한다.

    Args:
        df: ts, scenario, label, 알람 컬럼을 담은 DataFrame.
        alert_col: 알람 컬럼 이름.
        interval_min: 한 행이 나타내는 시간 (분). 탐지 지연 계산에 쓴다.

    Returns:
        지표 dict. tp, fp, fn, precision, recall, f1, mttd_min,
        detected(시나리오별 탐지 결과)를 담는다.
    """
    alerts = np.asarray(df[alert_col]).astype(bool)
    events = extract_events(df["scenario"], df["label"])
    anomalies = [e for e in events if e["is_anomaly"]]

    tp, fn, delays = 0, 0, []
    per_scenario = {}
    for ev in anomalies:
        seg = alerts[ev["start"]:ev["end"]]
        hit = bool(seg.any())
        stat = per_scenario.setdefault(
            ev["scenario"], {"total": 0, "hit": 0, "delays": []}
        )
        stat["total"] += 1
        if hit:
            tp += 1
            stat["hit"] += 1
            delay = int(np.argmax(seg)) * interval_min
            delays.append(delay)
            stat["delays"].append(delay)
        else:
            fn += 1

    covered = np.zeros(len(df), dtype=bool)
    for ev in anomalies:
        covered[ev["start"]:ev["end"]] = True

    fp = 0
    fp_by_scenario = {}
    names = pd.Series(df["scenario"]).fillna("").to_numpy()
    for start, end in find_runs(alerts):
        if covered[start:end].any():
            continue
        fp += 1
        overlap = {n for n in names[start:end] if n}
        key = ",".join(sorted(overlap)) if overlap else "정상 구간"
        fp_by_scenario[key] = fp_by_scenario.get(key, 0) + 1

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "mttd_min": float(np.mean(delays)) if delays else float("nan"),
        "per_scenario": per_scenario,
        "fp_by_scenario": fp_by_scenario,
    }


def false_positive_reduction(baseline_fp, proposed_fp):
    """비교 기준선 대비 오탐 감소율을 구한다.

    Args:
        baseline_fp: 기준선의 오탐 건수.
        proposed_fp: 제안 방식의 오탐 건수.

    Returns:
        float. 백분율. 기준선의 오탐이 0이면 nan.
    """
    if baseline_fp == 0:
        return float("nan")
    return (1 - proposed_fp / baseline_fp) * 100
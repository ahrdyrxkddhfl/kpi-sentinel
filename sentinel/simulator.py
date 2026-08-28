"""시계열 시뮬레이터 골격.

도메인 비의존. KPI 이름이나 단위의 의미를 알지 못하고,
config에서 받은 수치 파라미터만으로 시계열과 라벨을 생성한다.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class AnomalyEvent:
    """주입된 이상 구간 하나를 나타낸다.

    Attributes:
        scenario: 시나리오 이름.
        start_idx: 시작 인덱스 (inclusive).
        end_idx: 종료 인덱스 (exclusive).
        kpis: 영향을 받은 KPI 이름 -> 강도 배수 매핑.
        is_anomaly: 정답 라벨 여부. False면 값은 변하되 라벨은 0이다.
    """
    scenario: str
    start_idx: int
    end_idx: int
    kpis: dict
    is_anomaly: bool


def build_time_index(start, periods, interval_sec):
    """샘플링 간격에 맞는 타임스탬프 인덱스를 만든다."""
    return pd.date_range(start, periods=periods, freq=f"{interval_sec}s")


def seasonal_curve(ts, peaks, weekend_scale):
    """0~1로 정규화된 일 단위 계절성 곡선을 만든다.

    가우시안 봉우리를 겹쳐 출퇴근 이중 피크 형태를 만든다.
    완만한 sine이 아니라 뾰족한 형태여야, 계절성을 반영하지 않는
    탐지기가 실제로 오탐을 낸다.

    Args:
        ts: DatetimeIndex.
        peaks: {center_hour, width, weight} 딕셔너리 목록.
        weekend_scale: 주말에 곱할 감쇠 계수.

    Returns:
        길이 len(ts)의 float 배열. 최대값이 1.0이 되도록 정규화된다.
    """
    hour = ts.hour.to_numpy() + ts.minute.to_numpy() / 60
    curve = np.zeros(len(ts), dtype=float)
    for p in peaks:
        curve += p["weight"] * np.exp(
            -0.5 * ((hour - p["center_hour"]) / p["width"]) ** 2
        )
    curve /= curve.max()
    is_weekend = ts.dayofweek.to_numpy() >= 5
    return curve * np.where(is_weekend, weekend_scale, 1.0)


def generate_normal(kpi_cfg, season, rng):
    """이상 주입 전의 정상 시계열을 만든다.

    Args:
        kpi_cfg: baseline, noise_std, season_amp, season_profile을 담은 dict.
        season: 0~1로 정규화된 계절성 곡선.
        rng: numpy Generator.

    Returns:
        float 배열.
    """
    profile = kpi_cfg.get("season_profile", "traffic")
    s = season if profile == "traffic" else -season
    return (
        kpi_cfg["baseline"]
        + kpi_cfg["season_amp"] * s
        + rng.normal(0, kpi_cfg["noise_std"], len(season))
    )


def _shape(kind, length):
    """시나리오별 시간축 강도 프로파일을 만든다.

    Returns:
        길이 length, 최대 1.0인 배열.
    """
    t = np.linspace(0, 1, length)
    if kind == "gradual_degradation":
        return t                       # 선형 증가
    if kind == "config_change":
        return np.ones(length)         # 계단형 유지
    if kind == "node_down":
        return np.ones(length)
    if kind == "traffic_surge":
        return np.sin(np.pi * t)       # 완만한 산 모양
    return np.exp(-((t - 0.3) / 0.25) ** 2)  # spike: 빠르게 튀고 감쇠


def inject(series, kpi_cfg, event, ratio, rng):
    """시계열 한 개에 이상 구간을 주입한다.

    direction에 따라 부호를 정한다. higher_is_worse면 위로,
    lower_is_worse면 아래로 민다.

    Args:
        series: 대상 배열. 제자리에서 수정된다.
        kpi_cfg: 해당 KPI의 config.
        event: AnomalyEvent.
        ratio: 계절성 진폭 대비 강도 배수.
        rng: numpy Generator.
    """
    length = event.end_idx - event.start_idx
    amp = kpi_cfg["season_amp"] * ratio
    sign = 1.0 if kpi_cfg["direction"] == "higher_is_worse" else -1.0
    delta = sign * amp * _shape(event.scenario, length)
    series[event.start_idx:event.end_idx] += delta

def _draw_count(count, rng):
    """시나리오 발생 횟수를 결정한다.

    정수면 그대로 쓰고, [lo, hi] 형태면 그 범위에서 뽑는다.
    발생 빈도가 고정되면 장애가 드문 조건에서의 거동을 확인할 수 없다.

    Args:
        count: 정수 또는 [최소, 최대] 목록.
        rng: numpy Generator.

    Returns:
        int.
    """
    if isinstance(count, (list, tuple)):
        return int(rng.integers(count[0], count[1] + 1))
    return int(count)

def plan_events(n, scen_cfg, kpi_names, rng, min_gap, warmup):
    """시나리오 설정에 따라 이상 구간 배치를 계획한다.

    구간이 겹치지 않도록 min_gap 이상 떨어뜨린다. 겹치면 어떤 시나리오가
    원인인지 평가 단계에서 구분할 수 없다.
    warmup 이전에는 아무것도 배치하지 않는다. baseline을 오염되지 않은
    구간에서 학습해야 하기 때문이다.
    count가 범위로 주어지면 실행마다 발생 횟수가 달라진다.


    Args:
        n: 전체 샘플 수.
        scen_cfg: scenarios.yaml의 scenarios 딕셔너리.
        kpi_names: 사용 가능한 KPI 이름 목록.
        rng: numpy Generator.
        min_gap: 구간 사이 최소 간격 (샘플 수).
        warmup: 이상을 주입하지 않는 앞쪽 구간 길이 (샘플 수).

    Returns:
        AnomalyEvent 목록. 시작 시각 순으로 정렬된다.
    """
    occupied = []
    events = []

    for name, cfg in scen_cfg.items():
        if not cfg.get("enabled", True):
            continue
        lo, hi = cfg["duration_min"]
        weights = cfg["affects"]
        for _ in range(_draw_count(cfg["count"], rng)):
            for _attempt in range(500):
                dur = int(rng.integers(lo, hi + 1))
                start = int(rng.integers(warmup, n - dur))
                end = start + dur
                clash = any(
                    start < o_end + min_gap and o_start - min_gap < end
                    for o_start, o_end in occupied
                )
                if not clash:
                    occupied.append((start, end))
                    events.append(
                        AnomalyEvent(
                            scenario=name,
                            start_idx=start,
                            end_idx=end,
                            kpis={k: w for k, w in weights.items()
                                  if k in kpi_names},
                            is_anomaly=cfg.get("is_anomaly", True),
                        )
                    )
                    break
    events.sort(key=lambda e: e.start_idx)
    return events


def simulate(kpi_yaml, scen_yaml):
    """전체 시계열과 정답 라벨을 생성한다.

    Args:
        kpi_yaml: kpi.yaml을 파싱한 dict.
        scen_yaml: scenarios.yaml을 파싱한 dict.

    Returns:
        (df, events) 튜플.
        df는 ts, 각 KPI 컬럼, label, scenario를 담는다.
        label은 is_anomaly가 True인 구간에서만 1이다.
    """
    smp = kpi_yaml["sampling"]
    n = int(24 * 60 * 60 / smp["interval_sec"] * smp["days"])
    ts = build_time_index(smp["start"], n, smp["interval_sec"])

    rng = np.random.default_rng(scen_yaml["seed"])
    season = seasonal_curve(
        ts,
        kpi_yaml["season"]["weekday_peaks"],
        kpi_yaml["season"]["weekend_scale"],
    )

    kpis = kpi_yaml["kpis"]
    data = {k: generate_normal(c, season, rng) for k, c in kpis.items()}

    min_gap = int(60 * 60 / smp["interval_sec"])  # 1시간
    warmup = int(24 * 60 * 60 / smp["interval_sec"] * smp.get("warmup_days", 0))
    events = plan_events(n, scen_yaml["scenarios"], list(kpis), rng,
                         min_gap, warmup)

    label = np.zeros(n, dtype=int)
    scenario_col = np.full(n, "", dtype=object)

    for ev in events:
        ratio_lo, ratio_hi = scen_yaml["scenarios"][ev.scenario]["magnitude_ratio"]
        for k, weight in ev.kpis.items():
            r = float(rng.uniform(ratio_lo, ratio_hi)) * weight
            inject(data[k], kpis[k], ev, r, rng)
        scenario_col[ev.start_idx:ev.end_idx] = ev.scenario
        if ev.is_anomaly:
            label[ev.start_idx:ev.end_idx] = 1

    for k, c in kpis.items():
        lo, hi = c["clip"]
        data[k] = np.clip(data[k], lo, hi)

    df = pd.DataFrame({"ts": ts, **data})
    df["label"] = label
    df["scenario"] = scenario_col
    return df, events
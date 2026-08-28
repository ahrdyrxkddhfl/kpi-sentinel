"""이상 탐지 엔진.

도메인 비의존. KPI가 무엇을 의미하는지 알지 못하고, 수치 시계열과
설정만으로 알람을 낸다.

세 단계로 구성된다.

1. baseline
   요일타입과 시간대로 나눈 버킷별 평균/표준편차를 학습 구간에서 구한다.
   요일을 빼면 주말과 평일의 트래픽 차이가 전부 이상으로 잡힌다.

2. EWMA
   각 시점의 z-score를 지수가중이동평균으로 평활한다. 단발성 노이즈의
   영향을 줄이면서 지속되는 이탈에는 반응한다.

3. 지속성 규칙
   연속 N회 위반한 경우에만 알람을 낸다. 정규분포 꼬리에서 나오는
   단발 이탈을 제거한다. 운용에서 알람 억제로 쓰는 방식이다.
"""

import numpy as np
import pandas as pd


def bucket_key(ts, bucket_minutes):
    """요일타입과 시간대를 결합한 버킷 키를 만든다.

    주중과 주말을 나누고, 하루를 bucket_minutes 단위로 쪼갠다.

    Args:
        ts: DatetimeIndex 또는 Series.
        bucket_minutes: 시간대 버킷의 폭 (분).

    Returns:
        정수 배열. 같은 값이면 같은 버킷이다.
    """
    ts = pd.to_datetime(ts)
    is_weekend = (ts.dt.dayofweek >= 5).to_numpy().astype(int)
    minute_of_day = (ts.dt.hour * 60 + ts.dt.minute).to_numpy()
    slot = minute_of_day // bucket_minutes
    n_slot = (24 * 60) // bucket_minutes
    return is_weekend * n_slot + slot


class Baseline:
    """버킷별 정상 통계를 학습하고 z-score를 계산한다.

    학습 구간에는 이상이 섞이지 않아야 한다. 이상값이 들어가면
    표준편차가 부풀어 탐지 감도가 떨어진다.
    """

    def __init__(self, bucket_minutes, min_samples):
        """통계 저장소를 초기화한다.

        Args:
            bucket_minutes: 시간대 버킷의 폭 (분).
            min_samples: 버킷당 최소 표본 수. 미만인 버킷은 전역 통계로 대체한다.
        """
        self.bucket_minutes = bucket_minutes
        self.min_samples = min_samples
        self._mu = {}
        self._sd = {}
        self._global = {}

    def fit(self, df, kpis):
        """학습 구간에서 버킷별 평균과 표준편차를 구한다.

        Args:
            df: ts 컬럼과 KPI 컬럼을 담은 DataFrame. 이상이 없어야 한다.
            kpis: 학습할 KPI 이름 목록.

        Returns:
            self.
        """
        keys = bucket_key(df["ts"], self.bucket_minutes)
        for kpi in kpis:
            values = df[kpi].to_numpy()
            self._global[kpi] = (float(values.mean()), float(values.std()))
            mu, sd = {}, {}
            for key in np.unique(keys):
                sample = values[keys == key]
                if len(sample) >= self.min_samples and sample.std() > 0:
                    mu[key] = float(sample.mean())
                    sd[key] = float(sample.std())
            self._mu[kpi] = mu
            self._sd[kpi] = sd
        return self

    def zscore(self, df, kpi):
        """버킷 통계를 기준으로 z-score를 계산한다.

        학습되지 않은 버킷은 전역 통계로 대체한다.

        Args:
            df: ts 컬럼과 해당 KPI 컬럼을 담은 DataFrame.
            kpi: KPI 이름.

        Returns:
            float 배열.
        """
        keys = bucket_key(df["ts"], self.bucket_minutes)
        g_mu, g_sd = self._global[kpi]
        mu = np.array([self._mu[kpi].get(k, g_mu) for k in keys])
        sd = np.array([self._sd[kpi].get(k, g_sd) for k in keys])
        sd = np.where(sd > 0, sd, 1.0)
        return (df[kpi].to_numpy() - mu) / sd


def ewma(values, alpha):
    """지수가중이동평균을 계산한다.

    alpha가 클수록 최근값에 민감하고 노이즈에 약하다.

    Args:
        values: 1차원 배열.
        alpha: 0과 1 사이의 평활 계수.

    Returns:
        입력과 같은 길이의 배열.
    """
    out = np.empty(len(values), dtype=float)
    acc = values[0]
    for i, v in enumerate(values):
        acc = alpha * v + (1 - alpha) * acc
        out[i] = acc
    return out


def apply_persistence(flags, n):
    """연속 n회 위반한 시점만 알람으로 남긴다.

    단발성 위반을 제거한다. n회째부터 알람이 서므로 탐지 지연이
    n-1 샘플만큼 늘어난다. 오탐 감소와 탐지 지연의 교환이다.

    Args:
        flags: 위반 여부 bool 배열.
        n: 필요한 연속 위반 횟수. 1이면 규칙을 적용하지 않은 것과 같다.

    Returns:
        bool 배열.
    """
    if n <= 1:
        return flags.astype(bool)
    run = np.zeros(len(flags), dtype=int)
    count = 0
    for i, f in enumerate(flags):
        count = count + 1 if f else 0
        run[i] = count
    return run >= n


def detect(df, baseline, kpis, cfg):
    """전체 KPI에 대해 알람을 판정한다.

    KPI 중 하나라도 조건을 만족하면 그 시점에 알람이 선다.
    어떤 KPI가 원인인지도 함께 기록해 알람의 근거를 남긴다.

    Args:
        df: ts 컬럼과 KPI 컬럼을 담은 DataFrame.
        baseline: 학습이 끝난 Baseline.
        kpis: 판정 대상 KPI 이름 목록.
        cfg: z_threshold, ewma_alpha, persistence를 담은 dict.

    Returns:
        원본에 다음 컬럼을 더한 DataFrame.
          z_<kpi>   : KPI별 EWMA 평활 z-score
          alert     : 알람 여부 (0/1)
          trigger   : 알람을 유발한 KPI 이름을 쉼표로 이은 문자열
    """
    out = df.copy()
    threshold = cfg["z_threshold"]
    alpha = cfg["ewma_alpha"]
    persistence = cfg["persistence"]

    per_kpi = {}
    for kpi in kpis:
        z = ewma(baseline.zscore(df, kpi), alpha)
        out[f"z_{kpi}"] = z
        per_kpi[kpi] = apply_persistence(np.abs(z) > threshold, persistence)

    alert = np.zeros(len(df), dtype=bool)
    for flags in per_kpi.values():
        alert |= flags

    triggers = []
    for i in range(len(df)):
        hit = [k for k, f in per_kpi.items() if f[i]]
        triggers.append(",".join(hit))

    out["alert"] = alert.astype(int)
    out["trigger"] = triggers
    return out


def detect_naive(df, kpis, train_df, cfg):
    """비교 기준선. 전역 통계에 고정 임계값만 적용한다.

    계절성과 지속성을 반영하지 않는다. 개선 효과를 측정할 대상이다.

    Args:
        df: 판정 대상 DataFrame.
        kpis: KPI 이름 목록.
        train_df: 통계를 학습할 구간의 DataFrame.
        cfg: z_threshold를 담은 dict.

    Returns:
        alert 컬럼을 더한 DataFrame.
    """
    out = df.copy()
    alert = np.zeros(len(df), dtype=bool)
    for kpi in kpis:
        mu = train_df[kpi].mean()
        sd = train_df[kpi].std()
        z = (df[kpi].to_numpy() - mu) / (sd if sd > 0 else 1.0)
        alert |= np.abs(z) > cfg["z_threshold"]
    out["alert"] = alert.astype(int)
    return out
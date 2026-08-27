"""
Walking skeleton: 데이터 생성 -> 탐지 -> 시각화 관통 확인용.
이후 sentinel/ 모듈로 재작성하면서 버릴 코드.
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

rng = np.random.default_rng(42)

# ---- 1. 데이터 생성 (RTT 1종, 1분 간격 3일) ----
N = 60 * 24 * 3
ts = pd.date_range("2026-08-01", periods=N, freq="1min")

hour = ts.hour.to_numpy() + ts.minute.to_numpy() / 60
daily = 8 * np.sin(2 * np.pi * (hour - 9) / 24)      # 일 단위 계절성
rtt = 40 + daily + rng.normal(0, 2.0, N)              # baseline 40ms

label = np.zeros(N, dtype=int)

# 스파이크 3회 주입 + ground truth 라벨
for start in (700, 1900, 3300):
    dur = 8
    rtt[start:start + dur] += rng.uniform(25, 40, dur)
    label[start:start + dur] = 1

df = pd.DataFrame({"ts": ts, "rtt_ms": rtt, "label": label})
df.to_csv("data/raw/quick_rtt.csv", index=False)

# ---- 2. 탐지 (전역 z-score) ----
W = 60
mu = df["rtt_ms"].rolling(W).mean()
sd = df["rtt_ms"].rolling(W).std()
df["z"] = (df["rtt_ms"] - mu) / sd
df["pred"] = (df["z"].abs() > 3).astype(int)

# ---- 3. 평가 (point-wise, 임시) ----
tp = int(((df.pred == 1) & (df.label == 1)).sum())
fp = int(((df.pred == 1) & (df.label == 0)).sum())
fn = int(((df.pred == 0) & (df.label == 1)).sum())
prec = tp / (tp + fp) if tp + fp else 0
rec = tp / (tp + fn) if tp + fn else 0
f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0
print(f"TP={tp} FP={fp} FN={fn}")
print(f"precision={prec:.3f} recall={rec:.3f} f1={f1:.3f}")

# ---- 4. 시각화 ----
fig, ax = plt.subplots(figsize=(14, 4))
ax.plot(df.ts, df.rtt_ms, lw=0.6, color="#444", label="RTT (ms)")
ax.scatter(df.ts[df.label == 1], df.rtt_ms[df.label == 1],
           s=14, color="tab:green", label="ground truth")
ax.scatter(df.ts[df.pred == 1], df.rtt_ms[df.pred == 1],
           s=45, facecolors="none", edgecolors="tab:red", label="detected")
ax.set_title(f"RTT anomaly detection (z>3)  |  F1={f1:.3f}")
ax.legend(loc="upper right")
fig.tight_layout()
fig.savefig("data/quick_e2e.png", dpi=120)
print("saved -> data/quick_e2e.png")
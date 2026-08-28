"""탐지 결과를 그림으로 남긴다.

도메인 비의존. 컬럼 이름의 의미를 알지 못하고, 주어진 KPI 컬럼과
라벨·알람 컬럼만으로 그린다.

Grafana 대신 정적 이미지를 쓴다. 문서에 그대로 삽입할 수 있고,
저장소를 받은 사람이 별도 실행 없이 결과를 확인할 수 있다.
"""

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use("Agg")

COLOR_LINE = "#333333"
COLOR_TRUTH = "#2e7d32"
COLOR_ALERT = "#c62828"
COLOR_TRAP = "#f9a825"


def _shade(ax, ts, mask, color, label, alpha=0.18):
    """마스크가 True인 연속 구간을 배경색으로 칠한다.

    Args:
        ax: 대상 축.
        ts: 시각 배열.
        mask: bool 배열.
        color: 칠할 색.
        label: 범례에 쓸 이름. 첫 구간에만 붙인다.
        alpha: 투명도.
    """
    mask = np.asarray(mask).astype(bool)
    if not mask.any():
        return
    padded = np.concatenate(([False], mask, [False]))
    diff = np.diff(padded.astype(int))
    starts = np.flatnonzero(diff == 1)
    ends = np.flatnonzero(diff == -1)
    for i, (s, e) in enumerate(zip(starts, ends)):
        ax.axvspan(ts[s], ts[min(e, len(ts) - 1)], color=color,
                   alpha=alpha, lw=0,
                   label=label if i == 0 else None)

def plot_timeline(df, kpis, path, figsize=(14, 4), dpi=130):
    """KPI 시계열과 정답·알람 구간을 그린다.

    정답과 알람을 같은 축에 겹쳐 칠하면 색이 섞여 판별이 어려우므로,
    맨 아래에 별도의 띠 축을 두어 두 구간을 위아래로 나누어 표시한다.

    Args:
        df: ts, KPI 컬럼, label, alert를 담은 DataFrame.
        kpis: 그릴 KPI 이름 목록.
        path: 저장 경로.
        figsize: 축 하나의 크기.
        dpi: 해상도.
    """
    ts = df["ts"].to_numpy()
    n = len(kpis)
    heights = [3] * n + [1]
    fig, axes = plt.subplots(
        n + 1, 1, sharex=True,
        figsize=(figsize[0], figsize[1] * n * 0.7 + 1.2),
        gridspec_kw={"height_ratios": heights},
    )

    for ax, kpi in zip(axes[:n], kpis):
        ax.plot(ts, df[kpi], lw=0.5, color=COLOR_LINE)
        ax.set_ylabel(kpi, fontsize=9)
        ax.tick_params(labelsize=8)
        ax.margins(x=0)

    band = axes[n]
    _shade_band(band, ts, df["label"] == 1, 0.55, 0.95,
                COLOR_TRUTH, "실제 장애")
    _shade_band(band, ts, df["alert"] == 1, 0.05, 0.45,
                COLOR_ALERT, "알람")
    band.set_ylim(0, 1)
    band.set_yticks([0.25, 0.75])
    band.set_yticklabels(["알람", "정답"], fontsize=8)
    band.set_xlabel("시각", fontsize=9)
    band.tick_params(labelsize=8)
    band.margins(x=0)

    fig.tight_layout()
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def _shade_band(ax, ts, mask, y0, y1, color, label):
    """띠 축의 지정한 높이 구간에 마스크를 칠한다.

    Args:
        ax: 대상 축.
        ts: 시각 배열.
        mask: bool 배열.
        y0: 칠할 영역의 아래쪽 좌표 (0~1).
        y1: 위쪽 좌표.
        color: 칠할 색.
        label: 범례 이름.
    """
    mask = np.asarray(mask).astype(bool)
    if not mask.any():
        return
    padded = np.concatenate(([False], mask, [False]))
    diff = np.diff(padded.astype(int))
    starts = np.flatnonzero(diff == 1)
    ends = np.flatnonzero(diff == -1)
    for i, (s, e) in enumerate(zip(starts, ends)):
        ax.axvspan(ts[s], ts[min(e, len(ts) - 1)], ymin=y0, ymax=y1,
                   color=color, lw=0, label=label if i == 0 else None)
    ax.legend(loc="upper left", bbox_to_anchor=(0, -0.6),
              fontsize=8, ncol=2, frameon=False)


def plot_comparison(df_naive, df_proposed, kpi, path,
                    figsize=(14, 4), dpi=130):
    """같은 구간에서 두 방식의 알람을 나란히 그린다.

    오탐 차이가 시각적으로 드러나게 한다.

    Args:
        df_naive: 비교 기준선 결과.
        df_proposed: 제안 방식 결과.
        kpi: 배경에 그릴 KPI 이름.
        path: 저장 경로.
        figsize: 축 하나의 크기.
        dpi: 해상도.
    """
    ts = df_naive["ts"].to_numpy()
    fig, axes = plt.subplots(2, 1, sharex=True, sharey=True,
                             figsize=(figsize[0], figsize[1] * 2))

    is_trap = (df_naive["scenario"] != "") & (df_naive["label"] == 0)

    for ax, data, title in (
        (axes[0], df_naive, "비교 기준선 (전역 z-score)"),
        (axes[1], df_proposed, "제안 방식 (요일x시간대 baseline + EWMA + 지속성)"),
    ):
        _shade(ax, ts, is_trap, COLOR_TRAP, "정상 트래픽 증가 (라벨 0)")
        _shade(ax, ts, data["label"] == 1, COLOR_TRUTH, "실제 장애 구간")
        _shade(ax, ts, data["alert"] == 1, COLOR_ALERT, "알람 구간")
        ax.plot(ts, data[kpi], lw=0.6, color=COLOR_LINE)
        ax.set_title(title, fontsize=10, loc="left")
        ax.set_ylabel(kpi, fontsize=9)
        ax.tick_params(labelsize=8)

    axes[0].legend(loc="upper right", fontsize=8, ncol=3)
    fig.tight_layout()
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def plot_sweep(sweep_df, param, path, figsize=(7, 4), dpi=130):
    """파라미터 변화에 따른 지표 추이를 그린다.

    F1과 오탐 건수, 탐지 지연을 함께 그려 교환 관계를 보인다.

    Args:
        sweep_df: run_sweep이 저장한 DataFrame.
        param: 그릴 파라미터 이름.
        path: 저장 경로.
        figsize: 그림 크기.
        dpi: 해상도.
    """
    sub = sweep_df[sweep_df["param"] == param].sort_values("value")
    if sub.empty:
        return

    fig, ax1 = plt.subplots(figsize=figsize)
    ax2 = ax1.twinx()

    ax1.plot(sub["value"], sub["f1"], "o-", color="#1565c0", label="F1")
    ax1.plot(sub["value"], sub["recall"], "s--", color="#2e7d32",
             label="Recall")
    ax1.set_xlabel(param, fontsize=9)
    ax1.set_ylabel("F1 / Recall", fontsize=9)
    ax1.set_ylim(0, 1.05)

    ax2.plot(sub["value"], sub["mttd_min"], "^:", color="#c62828",
             label="탐지 지연(분)")
    ax2.set_ylabel("탐지 지연 (분)", fontsize=9)

    lines = ax1.get_lines() + ax2.get_lines()
    ax1.legend(lines, [l.get_label() for l in lines],
               loc="lower right", fontsize=8)
    ax1.grid(alpha=0.25)
    ax1.tick_params(labelsize=8)
    ax2.tick_params(labelsize=8)

    fig.tight_layout()
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
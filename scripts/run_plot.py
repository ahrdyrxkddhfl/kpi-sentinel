"""탐지 결과 시각화를 생성한다."""

import sys
from pathlib import Path

import matplotlib
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel.evaluate import find_runs
from sentinel.plotting import plot_comparison, plot_sweep, plot_timeline

ROOT = Path(__file__).resolve().parents[1]


def set_korean_font():
    """한글이 깨지지 않도록 폰트를 지정한다.

    설치된 폰트 중 사용 가능한 것을 찾는다. 없으면 경고만 남기고
    기본 폰트를 쓴다.
    """
    from matplotlib import font_manager
    candidates = ["AppleGothic", "Malgun Gothic", "NanumGothic",
                  "Noto Sans CJK KR"]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in candidates:
        if name in available:
            matplotlib.rcParams["font.family"] = name
            matplotlib.rcParams["axes.unicode_minus"] = False
            return name
    print("경고: 한글 폰트를 찾지 못했다. 축 라벨이 깨질 수 있다.")
    return None

def main():
    cfg = yaml.safe_load((ROOT / "config/plot.yaml").read_text())
    set_korean_font()

    alerts = ROOT / "data/alerts"
    if not (alerts / "proposed.csv").exists():
        print("탐지 결과가 없다. scripts/run_detect.py를 먼저 실행할 것.")
        return 1

    out_dir = ROOT / cfg["output_dir"]
    out_dir.mkdir(parents=True, exist_ok=True)

    proposed = pd.read_csv(alerts / "proposed.csv")
    naive = pd.read_csv(alerts / "naive.csv")
    for df in (proposed, naive):
        df["ts"] = pd.to_datetime(df["ts"])
        df["scenario"] = df["scenario"].fillna("")

    figsize = tuple(cfg["figsize"])
    dpi = cfg["dpi"]

    p1 = out_dir / "timeline.png"
    plot_timeline(proposed, cfg["timeline_kpis"], p1, figsize, dpi)
    print(f"-> {p1}")

    # 기준선은 오탐을 내지만 제안 방식은 내지 않는 구간을 찾는다.
    # 이 차이가 오탐 감소의 실체이므로 그 구간을 비교 대상으로 삼는다.
    covered = (proposed["label"] == 1).to_numpy()
    naive_fp = ((naive["alert"] == 1) & ~covered).to_numpy()
    prop_alert = (proposed["alert"] == 1).to_numpy()

    best = None
    for s, e in find_runs(naive_fp):
        if prop_alert[s:e].any():
            continue
        if best is None or (e - s) > (best[1] - best[0]):
            best = (s, e)

    if best is not None:
        margin = cfg["compare"]["margin_min"]
        lo = max(0, best[0] - margin)
        hi = min(len(proposed), best[1] + margin)
        p2 = out_dir / "comparison.png"
        plot_comparison(
            naive.iloc[lo:hi].reset_index(drop=True),
            proposed.iloc[lo:hi].reset_index(drop=True),
            cfg["timeline_kpis"][0], p2, figsize, dpi,
        )
        print(f"-> {p2}  (기준선 오탐 {best[1] - best[0]}분, "
              f"제안 방식 알람 없음)")
    else:
        print("경고: 기준선만 오탐을 낸 구간이 없어 비교 그림을 건너뛴다.")

    sweep = ROOT / "data/sweep.csv"
    if sweep.exists():
        sweep_df = pd.read_csv(sweep)
        for param in sweep_df["param"].unique():
            p3 = out_dir / f"sweep_{param}.png"
            plot_sweep(sweep_df, param, p3)
            print(f"-> {p3}")
    else:
        print("경고: data/sweep.csv가 없어 스윕 그림을 건너뛴다.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
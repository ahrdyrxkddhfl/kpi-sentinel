"""컨슈머 강제 종료 실험. 커밋 전략별 유실과 중복을 측정한다.

전략과 종료 시점의 조합마다 다음을 반복한다.
1. 적재 테이블을 비우고 새 컨슈머 그룹으로 처음부터 읽다가 강제 종료한다.
2. 같은 그룹으로 다시 실행해 끝까지 처리한다.
3. 원본 기준 기대 건수와 실제 적재 건수를 비교한다.

조합마다 새 그룹 이름을 써서 이전 실험의 커밋 위치가 섞이지 않게 한다.
결과는 표로 출력하고 마크다운 파일로도 남긴다.
"""

import csv
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONSUMER = ROOT / "scripts/run_consume_cells.py"
CRASH_EXIT_CODE = 3

STRATEGY_DESC = {
    "before_write": "읽자마자 커밋",
    "after_write": "적재 후 마지막 위치 커밋",
    "window_safe": "적재 후 윈도우 복구 가능 위치까지 커밋",
}
STAGE_DESC = {
    "pre_write": "적재 전 종료",
    "pre_commit": "적재 후 커밋 전 종료",
}


def count_records(path):
    """원본 파일의 레코드 수를 센다.

    Args:
        path: CSV 경로.

    Returns:
        헤더를 제외한 행 수.
    """
    with Path(path).open(newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def run_consumer(args, stats_path):
    """컨슈머를 별도 프로세스로 실행하고 통계를 읽는다.

    강제 종료가 부모 프로세스까지 죽이지 않도록 서브프로세스로 띄운다.

    Args:
        args: run_consume_cells.py에 넘길 인자 목록.
        stats_path: 통계 JSON 경로.

    Returns:
        (returncode, stats) 튜플.

    Raises:
        RuntimeError: 통계 파일이 만들어지지 않았을 때.
    """
    stats_path.unlink(missing_ok=True)
    proc = subprocess.run(
        [sys.executable, str(CONSUMER), *args, "--stats-out", str(stats_path)],
        capture_output=True, text=True,
    )
    if not stats_path.exists():
        print(proc.stdout)
        print(proc.stderr)
        raise RuntimeError(f"컨슈머가 통계를 남기지 않았다 (exit {proc.returncode})")
    return proc.returncode, json.loads(stats_path.read_text())


def run_case(stage, strategy, crash_after, total, tmp):
    """전략과 종료 시점 한 조합을 실험한다.

    Args:
        stage: 강제 종료 시점.
        strategy: 커밋 전략.
        crash_after: 강제 종료까지 처리할 레코드 수.
        total: 원본 레코드 수.
        tmp: 임시 파일을 둘 디렉토리.

    Returns:
        loss, reread, ignored를 담은 dict.
        reread가 음수면 그만큼의 레코드를 읽지 않고 건너뛴 것이다.

    Raises:
        RuntimeError: 첫 실행이 강제 종료 코드로 끝나지 않았을 때.
    """
    group = f"crash-exp-{stage}-{strategy}-{int(time.time())}"
    common = ["--strategy", strategy, "--group", group]

    code, first = run_consumer(
        [*common, "--reset", "--crash-after", str(crash_after),
         "--crash-stage", stage], tmp / "first.json")
    if code != CRASH_EXIT_CODE:
        raise RuntimeError(f"강제 종료되지 않았다 (exit {code})")

    _, second = run_consumer(common, tmp / "second.json")
    return {
        "loss": second["expected_total"] - second["stored_total"],
        "reread": first["received"] + second["received"] - total,
        "ignored": second["ignored"],
    }


def to_markdown(rows, crash_after, total, expected_total):
    """결과를 마크다운 표로 만든다.

    Args:
        rows: (stage, strategy, result) 튜플 목록.
        crash_after: 강제 종료까지 처리한 레코드 수.
        total: 원본 레코드 수.
        expected_total: 기대 적재 건수.

    Returns:
        마크다운 문자열.
    """
    lines = [
        "# 컨슈머 강제 종료 실험",
        "",
        f"원본 {total:,}건(기대 적재 {expected_total:,}건) 중 {crash_after:,}번째 "
        "레코드 처리 시점에 컨슈머를 정리 코드 없이 종료하고, 같은 그룹으로 "
        "재시작해 끝까지 처리했다.",
        "",
        "| 종료 시점 | 커밋 전략 | 유실 | 다시 읽은 레코드 | 중복 무시 |",
        "|---|---|---:|---:|---:|",
    ]
    for stage, strategy, r in rows:
        reread = (f"{r['reread']:,}" if r["reread"] >= 0
                  else f"{-r['reread']:,}건 건너뜀")
        lines.append(
            f"| {STAGE_DESC[stage]} | {STRATEGY_DESC[strategy]} "
            f"| {r['loss']:,} | {reread} | {r['ignored']:,} |")
    lines += [
        "",
        "유실은 기대 적재 건수와 실제 적재 건수의 차이다. 중복 무시는 재시작 후 "
        "다시 만들어진 결과 중 기본키 충돌로 건너뛴 건수로, 멱등 적재가 없었다면 "
        "그대로 중복 행이 되었을 양이다.",
        "",
    ]
    return "\n".join(lines)


def main():
    stream_cfg = yaml.safe_load((ROOT / "config/stream.yaml").read_text())
    me = stream_cfg["multi_entity"]
    exp = stream_cfg["crash_experiment"]
    total = count_records(ROOT / me["path"])

    rows = []
    expected_total = None
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        for stage in exp["stages"]:
            for strategy in exp["strategies"]:
                print(f"실행 중: {STAGE_DESC[stage]} / {STRATEGY_DESC[strategy]}")
                r = run_case(stage, strategy, exp["crash_after"], total, tmp)
                print(f"  유실 {r['loss']:,}  다시 읽음 {r['reread']:+,}  "
                      f"중복 무시 {r['ignored']:,}")
                rows.append((stage, strategy, r))
                if expected_total is None:
                    second = json.loads((tmp / "second.json").read_text())
                    expected_total = second["expected_total"]

    md = to_markdown(rows, exp["crash_after"], total, expected_total)
    out = ROOT / exp["report_path"]
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md)
    print(f"\n{md}\n-> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

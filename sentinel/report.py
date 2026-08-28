"""알람 구간에 대한 장애 리포트를 생성한다.

도메인 비의존. KPI가 무엇을 의미하는지 알지 못하고, 저장소에서
뽑은 수치 요약을 그대로 언어 모델에 넘긴다. 도메인 지식은 프롬프트
템플릿으로 주입한다.

전체 시계열을 넘기지 않고 집계된 요약만 넘긴다. 토큰 비용을 줄이는
목적도 있지만, 더 중요한 이유는 모델이 원시 데이터에서 통계를 다시
계산하게 두면 그 계산을 신뢰할 수 없기 때문이다. 수치는 SQL로 확정하고
모델은 서술만 담당한다.
"""

import json
import os

from dotenv import load_dotenv

SYSTEM_PROMPT = """당신은 운용 담당자를 위한 장애 리포트를 작성한다.

주어진 것은 이상탐지 시스템이 감지한 알람 구간의 수치 요약이다.
이 수치만 사용하고, 주어지지 않은 값을 만들어내지 않는다.

판단 규칙:
- 각 지표의 '알람_유발' 값이 true인 것만 실제로 임계를 넘은 지표다.
- '알람_유발'이 false인 지표는 이탈폭이 0이 아니더라도 정상 범위 안이다.
  이런 지표를 근거로 이상을 주장하지 않는다.
- 결론은 임계를 넘은 지표의 조합만으로 내린다.

리포트는 다음 순서로 작성한다.
1. 요약: 무슨 일이 언제 발생했는지 두 문장 이내
2. 근거: 임계를 넘은 지표를 먼저 쓰고, 정상 범위인 지표는 정상임을 밝힌다
3. 추정 원인: 임계를 넘은 지표의 조합에서 유추되는 가능성. 단정하지 않는다
4. 권고 조치: 운용자가 확인할 항목

한국어로 쓰고, 문장은 '~한다' 형태의 평서문으로 한다.
과장하지 않고 수치에 근거해 서술한다.
확실하지 않은 것은 확실하지 않다고 쓴다."""

def summarize_window(store, metrics_table, alerts_table, start, end, kpis,
                     z_threshold=None):
    """알람 구간 하나를 수치 요약으로 압축한다.

    원시 시계열 대신 집계값만 뽑는다. 통계 계산은 SQL이 담당하고
    언어 모델은 서술만 담당한다.

    z_threshold가 주어지면 KPI별로 임계 초과 여부를 함께 기록한다.
    이탈폭만 제시하면 임계 미만인 미세한 변동도 이상으로 서술될 수
    있으므로, 판정에 사용된 기준을 명시한다.

    Args:
        store: Store 인스턴스.
        metrics_table: 지표 테이블 이름.
        alerts_table: 알람 테이블 이름.
        start: 구간 시작 시각.
        end: 구간 종료 시각.
        kpis: KPI 이름 목록.
        z_threshold: 탐지기가 사용한 z 임계값. None이면 초과 여부를
            기록하지 않는다.

    Returns:
        직렬화 가능한 요약 dict.
    """
    agg = ", ".join(
        f"AVG({k}) AS {k}_avg, MAX({k}) AS {k}_max, MIN({k}) AS {k}_min"
        for k in kpis
    )
    window = store.query(
        f"SELECT COUNT(*) AS n, {agg} FROM {metrics_table} "
        f"WHERE ts >= ? AND ts < ?", [start, end]
    ).iloc[0]

    normal = store.query(
        f"SELECT {', '.join(f'AVG({k}) AS {k}_avg, STDDEV({k}) AS {k}_sd' for k in kpis)} "
        f"FROM {metrics_table} WHERE scenario = ''"
    ).iloc[0]

    alerts = store.query(
        f"SELECT SUM(alert) AS alert_rows, "
        f"       MIN(CASE WHEN alert = 1 THEN ts END) AS first_alert "
        f"FROM {alerts_table} WHERE ts >= ? AND ts < ?", [start, end]
    ).iloc[0]

    triggers = store.query(
        f"SELECT trigger, COUNT(*) AS n FROM {alerts_table} "
        f"WHERE ts >= ? AND ts < ? AND trigger <> '' "
        f"GROUP BY trigger ORDER BY n DESC LIMIT 5", [start, end]
    )

    fired = set()
    for combo in triggers["trigger"]:
        fired.update(x for x in combo.split(",") if x)

    deviations = []
    for k in kpis:
        sd = float(normal[f"{k}_sd"]) or 1.0
        mu = float(normal[f"{k}_avg"])
        avg_z = (float(window[f"{k}_avg"]) - mu) / sd
        max_z = max(
            (float(window[f"{k}_max"]) - mu) / sd,
            (float(window[f"{k}_min"]) - mu) / sd,
            key=abs,
        )
        item = {
            "kpi": k,
            "정상_평균": round(mu, 3),
            "구간_평균": round(float(window[f"{k}_avg"]), 3),
            "평균_이탈_sd": round(avg_z, 2),
            "최대_이탈_sd": round(max_z, 2),
            "알람_유발": k in fired,
        }
        deviations.append(item)
    deviations.sort(key=lambda d: -abs(d["최대_이탈_sd"]))

    out = {
        "구간_시작": str(start),
        "구간_종료": str(end),
        "구간_길이_분": int(window["n"]),
        "알람_발생_행수": int(alerts["alert_rows"] or 0),
        "최초_알람_시각": str(alerts["first_alert"]),
        "알람_유발_지표": triggers.to_dict("records"),
        "지표별_이탈": deviations,
    }
    if z_threshold is not None:
        out["임계값_sd"] = z_threshold
        out["임계_초과_지표"] = sorted(fired)
        out["임계_미달_지표"] = sorted(set(kpis) - fired)
    return out

def build_report(summary, model="claude-haiku-4-5", max_tokens=1200,
                 domain_hint=""):
    """수치 요약으로부터 장애 리포트 문장을 생성한다.

    Args:
        summary: summarize_window가 만든 dict.
        model: 사용할 모델 이름.
        max_tokens: 응답 최대 길이.
        domain_hint: 도메인 지식을 담은 추가 지시문. KPI가 무엇을
            의미하는지 설명하는 문자열이며, 도메인이 바뀌면 이 값만
            교체한다.

    Returns:
        생성된 리포트 문자열.

    Raises:
        RuntimeError: API 키가 없을 때.
    """
    load_dotenv()
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY가 없다. .env 파일에 설정할 것.")
    from anthropic import Anthropic

    system = SYSTEM_PROMPT
    if domain_hint:
        system = f"{SYSTEM_PROMPT}\n\n[도메인 정보]\n{domain_hint}"

    client = Anthropic()
    resp = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{
            "role": "user",
            "content": json.dumps(summary, ensure_ascii=False, indent=2),
        }],
    )
    return "".join(b.text for b in resp.content if b.type == "text")
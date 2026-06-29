"""
BigQuery에 누적된 주간 평가 이력 데이터를 분석하는 모듈.

주요 분석:
  rising    : 최근 4주간 점수 상승폭 Top 종목
  falling   : 최근 4주간 점수 하락폭 Top 종목
  consistent: 4주 연속 A/B 등급을 유지한 종목 (가치투자 관점 핵심 지표)
  market    : 시장별(KR/US) 평균 점수 및 등급 분포 비교

사용법:
  python3 bq_analytics.py rising [--market kr|us] [--top N]
  python3 bq_analytics.py falling [--market kr|us] [--top N]
  python3 bq_analytics.py consistent [--market kr|us]
  python3 bq_analytics.py market
"""

import os
import sys
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET   = "holdit"
TABLE     = "stock_scores"
FULL_TABLE = f"{PROJECT_ID}.{DATASET}.{TABLE}"


def _client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def _run(query: str) -> list[dict]:
    rows = _client().query(query).result()
    return [dict(row) for row in rows]


# ─────────────────────────────────────────────
# 분석 1: 점수 상승/하락폭 Top N (Window Function)
# ─────────────────────────────────────────────
_SCORE_TREND_QUERY = """
WITH weekly_scores AS (
    -- 종목별 · 주차별 최신 점수 1행으로 정규화 (동일 주에 중복 적재된 경우 최신 1건만)
    SELECT
        ticker,
        name,
        market,
        score_date,
        total_score,
        ROW_NUMBER() OVER (PARTITION BY ticker, score_date ORDER BY export_timestamp DESC) AS rn
    FROM `{table}`
    WHERE scorer_version = 'v2'
),
deduped AS (
    SELECT ticker, name, market, score_date, total_score
    FROM weekly_scores
    WHERE rn = 1
),
ranked_weeks AS (
    -- 종목별로 최근 4주를 역순 번호로 매김 (1 = 이번 주, 4 = 4주 전)
    SELECT
        ticker,
        name,
        market,
        score_date,
        total_score,
        RANK() OVER (PARTITION BY ticker ORDER BY score_date DESC) AS week_rank
    FROM deduped
),
pivot AS (
    -- 이번 주 점수와 4주 전 점수를 같은 행으로 합침
    SELECT
        ticker,
        MAX(name)                                                      AS name,
        MAX(market)                                                    AS market,
        MAX(IF(week_rank = 1, total_score, NULL))                      AS score_latest,
        MAX(IF(week_rank = 4, total_score, NULL))                      AS score_4w_ago,
        MAX(IF(week_rank = 1, score_date,  NULL))                      AS latest_date,
        COUNT(DISTINCT score_date)                                     AS weeks_available
    FROM ranked_weeks
    WHERE week_rank <= 4
    GROUP BY ticker
)
SELECT
    ticker,
    name,
    market,
    score_latest,
    score_4w_ago,
    score_latest - score_4w_ago                                        AS score_delta,
    latest_date
FROM pivot
WHERE score_4w_ago IS NOT NULL  -- 4주 전 데이터가 없는 신규 종목 제외
  {market_filter}
ORDER BY score_delta {direction}
LIMIT {top_n}
"""

def analyze_score_trend(direction: str = "DESC", market: str | None = None, top_n: int = 10):
    """최근 4주 점수 변화폭 기준 상위 종목 출력."""
    label = "상승" if direction == "DESC" else "하락"
    market_filter = f"AND market IN ({_market_condition(market)})" if market else ""

    query = _SCORE_TREND_QUERY.format(
        table=FULL_TABLE,
        direction=direction,
        market_filter=market_filter,
        top_n=top_n,
    )
    rows = _run(query)

    print(f"\n{'─'*60}")
    print(f"  최근 4주 점수 {label}폭 Top {top_n}" + (f"  [{market.upper()}]" if market else ""))
    print(f"{'─'*60}")
    print(f"  {'종목코드':<12} {'종목명':<20} {'현재':>5} {'4주전':>5} {'변화':>6}  시장")
    print(f"{'─'*60}")
    for r in rows:
        delta_str = f"{r['score_delta']:+d}"
        print(f"  {r['ticker']:<12} {str(r['name']):<20} {r['score_latest']:>5} {r['score_4w_ago']:>5} {delta_str:>6}  {r['market']}")
    print(f"{'─'*60}\n")


# ─────────────────────────────────────────────
# 분석 2: 연속 고등급 유지 종목 (CTE + HAVING)
# ─────────────────────────────────────────────
_CONSISTENT_QUERY = """
WITH weekly_scores AS (
    SELECT
        ticker,
        name,
        market,
        score_date,
        grade,
        total_score,
        ROW_NUMBER() OVER (PARTITION BY ticker, score_date ORDER BY export_timestamp DESC) AS rn
    FROM `{table}`
    WHERE scorer_version = 'v2'
),
deduped AS (
    SELECT ticker, name, market, score_date, grade, total_score
    FROM weekly_scores
    WHERE rn = 1
),
recent_4w AS (
    -- 종목별 최근 4주 데이터만 추출
    SELECT
        ticker,
        name,
        market,
        score_date,
        grade,
        total_score,
        RANK() OVER (PARTITION BY ticker ORDER BY score_date DESC) AS week_rank
    FROM deduped
),
summary AS (
    SELECT
        ticker,
        MAX(name)        AS name,
        MAX(market)      AS market,
        COUNT(*)         AS weeks_in_top,   -- 4주 중 몇 주나 고등급이었는지
        AVG(total_score) AS avg_score,
        MIN(total_score) AS min_score,
        MAX(total_score) AS max_score
    FROM recent_4w
    WHERE week_rank <= 4
      AND grade IN ('A', 'B')              -- A/B 등급만 카운트
    GROUP BY ticker
)
SELECT *
FROM summary
WHERE weeks_in_top = 4                     -- 4주 연속 A/B 등급 유지
  {market_filter}
ORDER BY avg_score DESC
"""

def analyze_consistent_grade(market: str | None = None):
    """최근 4주 연속 A/B 등급 유지 종목 출력."""
    market_filter = f"AND market IN ({_market_condition(market)})" if market else ""
    query = _CONSISTENT_QUERY.format(table=FULL_TABLE, market_filter=market_filter)
    rows = _run(query)

    print(f"\n{'─'*65}")
    print(f"  4주 연속 A/B 등급 유지 종목" + (f"  [{market.upper()}]" if market else ""))
    print(f"{'─'*65}")
    print(f"  {'종목코드':<12} {'종목명':<20} {'평균':>5} {'최저':>5} {'최고':>5}  시장")
    print(f"{'─'*65}")
    for r in rows:
        print(f"  {r['ticker']:<12} {str(r['name']):<20} {r['avg_score']:>5.1f} {r['min_score']:>5} {r['max_score']:>5}  {r['market']}")
    if not rows:
        print("  해당 조건을 만족하는 종목이 없습니다.")
    print(f"{'─'*65}\n")


# ─────────────────────────────────────────────
# 분석 3: 시장별 평균 점수 및 등급 분포 (GROUP BY + PIVOT)
# ─────────────────────────────────────────────
_MARKET_COMPARE_QUERY = """
WITH latest_week AS (
    -- 전체에서 가장 최근 score_date 기준으로 종목당 1행
    SELECT
        ticker,
        name,
        market,
        grade,
        total_score,
        per,
        roe,
        fcf_yield,
        dividend_yield,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY score_date DESC, export_timestamp DESC) AS rn
    FROM `{table}`
    WHERE scorer_version = 'v2'
),
deduped AS (
    SELECT * FROM latest_week WHERE rn = 1
)
SELECT
    market,
    COUNT(*)                                            AS stock_count,
    ROUND(AVG(total_score), 1)                         AS avg_score,
    ROUND(AVG(per),         2)                         AS avg_per,
    ROUND(AVG(roe),         2)                         AS avg_roe,
    ROUND(AVG(fcf_yield),   2)                         AS avg_fcf_yield,
    ROUND(AVG(dividend_yield), 2)                      AS avg_div_yield,
    COUNTIF(grade = 'A')                               AS grade_a,
    COUNTIF(grade = 'B')                               AS grade_b,
    COUNTIF(grade = 'C')                               AS grade_c,
    COUNTIF(grade = 'D')                               AS grade_d
FROM deduped
GROUP BY market
ORDER BY avg_score DESC
"""

def analyze_market_compare():
    """시장별 평균 지표 및 등급 분포 출력."""
    rows = _run(_MARKET_COMPARE_QUERY.format(table=FULL_TABLE))

    print(f"\n{'─'*70}")
    print("  시장별 평균 지표 및 등급 분포 (최근 평가 기준)")
    print(f"{'─'*70}")
    print(f"  {'시장':<8} {'종목수':>5} {'평균점':>6} {'PER':>6} {'ROE':>6} {'FCF%':>6} {'배당%':>6}  A  B  C  D")
    print(f"{'─'*70}")
    for r in rows:
        print(
            f"  {r['market']:<8} {r['stock_count']:>5} {r['avg_score']:>6} "
            f"{r['avg_per']:>6} {r['avg_roe']:>6} {r['avg_fcf_yield']:>6} {r['avg_div_yield']:>6}"
            f"  {r['grade_a']:>2} {r['grade_b']:>2} {r['grade_c']:>2} {r['grade_d']:>2}"
        )
    print(f"{'─'*70}\n")


# ─────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────
_KR_MARKETS = ("KOSPI", "KOSDAQ")

def _market_condition(market: str | None) -> str:
    if market == "kr":
        return ", ".join(f"'{m}'" for m in _KR_MARKETS)
    if market == "us":
        return "'NYSE', 'NASDAQ', 'NMS', 'NYQ'"
    return ""


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def _parse_args():
    args = sys.argv[1:]
    report  = args[0] if args else None
    market  = None
    top_n   = 10

    i = 1
    while i < len(args):
        if args[i] == "--market" and i + 1 < len(args):
            market = args[i + 1].lower()
            i += 2
        elif args[i] == "--top" and i + 1 < len(args):
            top_n = int(args[i + 1])
            i += 2
        else:
            i += 1

    return report, market, top_n


if __name__ == "__main__":
    report, market, top_n = _parse_args()

    if report == "rising":
        analyze_score_trend(direction="DESC", market=market, top_n=top_n)
    elif report == "falling":
        analyze_score_trend(direction="ASC", market=market, top_n=top_n)
    elif report == "consistent":
        analyze_consistent_grade(market=market)
    elif report == "market":
        analyze_market_compare()
    else:
        print(__doc__)

"""
Cloud SQL의 평가 결과를 BigQuery에 적재하는 스크립트.
Cloud Run Job 파이프라인의 마지막 단계에서 실행됨.

BigQuery 테이블: holdit.stock_scores
- companies + processed_financial_data + scoring_results를 조인한 비정규화 테이블
- 매주 실행 시 해당 주 데이터를 append (누적 이력 보존)
"""
import os
from datetime import datetime, timezone

from google.cloud import bigquery
from sqlalchemy import text
from database import SessionLocal

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-d8b33f7e-68af-4f6b-978")
DATASET_ID = "holdit"
TABLE_ID = "stock_scores"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# BigQuery 테이블 스키마 정의
SCHEMA = [
    bigquery.SchemaField("export_timestamp", "TIMESTAMP", description="BigQuery 적재 일시"),
    bigquery.SchemaField("score_date",        "DATE",      description="평가 일자"),
    bigquery.SchemaField("record_date",       "DATE",      description="재무 데이터 기준일"),
    bigquery.SchemaField("ticker",            "STRING",    description="종목코드"),
    bigquery.SchemaField("name",              "STRING",    description="종목명"),
    bigquery.SchemaField("market",            "STRING",    description="상장 시장"),
    bigquery.SchemaField("scorer_version",    "STRING",    description="scorer 버전"),
    # 가공 지표
    bigquery.SchemaField("per",               "FLOAT64",   description="주가수익비율"),
    bigquery.SchemaField("roe",               "FLOAT64",   description="자기자본이익률 (%)"),
    bigquery.SchemaField("fcf_yield",         "FLOAT64",   description="잉여현금흐름 수익률 (%)"),
    bigquery.SchemaField("pbr",               "FLOAT64",   description="주가순자산비율"),
    bigquery.SchemaField("peg_ratio",         "FLOAT64",   description="PEG 비율"),
    bigquery.SchemaField("debt_ratio",        "FLOAT64",   description="부채비율 (%)"),
    bigquery.SchemaField("dividend_yield",    "FLOAT64",   description="배당수익률 (%)"),
    # 점수
    bigquery.SchemaField("total_score",       "INT64",     description="총점 (/100)"),
    bigquery.SchemaField("grade",             "STRING",    description="등급 (A/B/C/D)"),
    bigquery.SchemaField("score_per",         "INT64",     description="PER 점수 (/10)"),
    bigquery.SchemaField("score_roe",         "INT64",     description="ROE 점수 (/15)"),
    bigquery.SchemaField("score_fcf",         "INT64",     description="FCF 점수 (/10)"),
    bigquery.SchemaField("score_pbr",         "INT64",     description="PBR 점수 (/5)"),
    bigquery.SchemaField("score_peg",         "INT64",     description="PEG 점수 (/10)"),
    bigquery.SchemaField("score_debt_ratio",  "INT64",     description="부채비율 점수 (/10)"),
    bigquery.SchemaField("score_div_yield",   "INT64",     description="배당수익률 점수 (/10)"),
    bigquery.SchemaField("score_div_growth",  "INT64",     description="배당성장 점수 (/10)"),
    bigquery.SchemaField("score_cancel",      "INT64",     description="자사주 소각 점수 (/10)"),
]

QUERY = """
    SELECT
        sr.score_date,
        pfd.record_date,
        c.ticker,
        c.name,
        c.market,
        sr.scorer_version,
        pfd.per,
        pfd.roe,
        pfd.fcf_yield,
        pfd.pbr,
        pfd.peg_ratio,
        pfd.debt_ratio,
        pfd.dividend_yield,
        sr.total_score,
        sr.grade,
        sr.score_per,
        sr.score_roe,
        sr.score_fcf,
        sr.score_pbr,
        sr.score_peg,
        sr.score_debt_ratio,
        sr.score_div_yield,
        sr.score_div_growth,
        sr.score_cancel
    FROM scoring_results sr
    JOIN companies c ON sr.ticker = c.ticker
    JOIN processed_financial_data pfd ON sr.ticker = pfd.ticker
        AND pfd.record_date = (
            SELECT MAX(record_date) FROM processed_financial_data
            WHERE ticker = sr.ticker
        )
    WHERE sr.score_date = (
        SELECT MAX(score_date) FROM scoring_results WHERE ticker = sr.ticker
        AND scorer_version = sr.scorer_version
    )
    AND sr.scorer_version = 'v2'
    ORDER BY sr.total_score DESC
"""


def ensure_table_exists(client: bigquery.Client):
    """테이블이 없으면 스키마로 새로 생성, 있으면 그대로 사용."""
    table_ref = bigquery.Table(FULL_TABLE_ID, schema=SCHEMA)
    try:
        client.get_table(FULL_TABLE_ID)
    except Exception:
        client.create_table(table_ref)
        print(f"BigQuery 테이블 생성: {FULL_TABLE_ID}")


def export_to_bigquery():
    db = SessionLocal()
    bq_client = bigquery.Client(project=PROJECT_ID)

    try:
        ensure_table_exists(bq_client)

        rows = db.execute(text(QUERY)).fetchall()
        if not rows:
            print("BigQuery로 적재할 데이터가 없습니다.")
            return

        export_ts = datetime.now(timezone.utc)
        records = []
        for row in rows:
            record = dict(row._mapping)
            record["export_timestamp"] = export_ts.isoformat()
            # date 객체 → ISO 문자열 변환 (JSON 직렬화 대응)
            for key in ("score_date", "record_date"):
                if record.get(key) is not None:
                    record[key] = record[key].isoformat()
            records.append(record)

        # WRITE_APPEND: 기존 데이터를 유지하고 이번 주 데이터를 추가 (누적 이력)
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = bq_client.load_table_from_json(records, FULL_TABLE_ID, job_config=job_config)
        job.result()  # 완료 대기

        print(f"BigQuery 적재 완료: {len(records)}개 종목 → {FULL_TABLE_ID}")
        print(f"적재 일시: {export_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    finally:
        db.close()


if __name__ == "__main__":
    export_to_bigquery()

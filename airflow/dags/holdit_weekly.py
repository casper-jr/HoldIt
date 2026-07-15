"""holdit_weekly — the ingest -> load-Bronze path, keyed on {{ ds }}.

Step 3 scope: US yf quote + price_history. Each endpoint is fetched by the
ingestion CLI (writes verbatim payloads to GCS, clearing the {ds} prefix first)
then loaded into its Bronze partition with WRITE_TRUNCATE, which makes a re-run of
a date idempotent. dbt (Silver/Gold) tasks join this DAG in Steps 4-5; DART
endpoints in Step 6.

Every task is keyed on {{ ds }} and nothing calls date.today(), so any date is
re-runnable via `airflow dags backfill`. price_history's range is derived from ds
(the prior 7 days), never hardcoded.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT = "project-d8b33f7e-68af-4f6b-978"
LOCATION = "asia-northeast3"
BUCKET = "holdit-raw"
LIMIT = 20  # proof slice; scale to the full universe once the path is green


def load_bronze(endpoint, **context):
    """GCS NDJSON -> holdit_bronze.raw_yf_{endpoint}, overwriting the ds partition."""
    from google.cloud import bigquery

    ds = context["ds"]
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    uri = f"gs://{BUCKET}/yf/{endpoint}/snapshot_date={ds}/*.json"
    table = f"{PROJECT}.holdit_bronze.raw_yf_{endpoint}${ds.replace('-', '')}"
    job = client.load_table_from_uri(
        uri,
        table,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    print(f"loaded {job.output_rows} rows into {table}")


def _ingest(endpoint, extra=""):
    return (
        "cd /opt/airflow && python -m ingestion.main "
        f"--source yf --endpoint {endpoint} --snapshot-date {{{{ ds }}}} "
        f"--limit {LIMIT} {extra}"
    ).strip()


with DAG(
    dag_id="holdit_weekly",
    start_date=datetime(2026, 6, 15),
    schedule="0 8 * * 5",       # 17:00 KST Friday
    catchup=False,              # backfill is explicit, never automatic
    tags=["holdit"],
) as dag:
    ingest_quote = BashOperator(
        task_id="ingest_us_quote", bash_command=_ingest("quote")
    )
    load_quote = PythonOperator(
        task_id="load_bronze_quote",
        python_callable=load_bronze,
        op_kwargs={"endpoint": "quote"},
    )

    ingest_price = BashOperator(
        task_id="ingest_us_price_history",
        bash_command=_ingest(
            "price_history", "--start {{ macros.ds_add(ds, -7) }} --end {{ ds }}"
        ),
    )
    load_price = PythonOperator(
        task_id="load_bronze_price_history",
        python_callable=load_bronze,
        op_kwargs={"endpoint": "price_history"},
    )

    # financials + dividends are point/history fetches — no date range needed.
    ingest_financials = BashOperator(
        task_id="ingest_us_financials", bash_command=_ingest("financials")
    )
    load_financials = PythonOperator(
        task_id="load_bronze_financials",
        python_callable=load_bronze,
        op_kwargs={"endpoint": "financials"},
    )

    ingest_dividends = BashOperator(
        task_id="ingest_us_dividends", bash_command=_ingest("dividends")
    )
    load_dividends = PythonOperator(
        task_id="load_bronze_dividends",
        python_callable=load_bronze,
        op_kwargs={"endpoint": "dividends"},
    )

    # Silver: build then test as a hard gate. dbt_run_silver refreshes the seed and
    # SCD2 snapshot, then rebuilds the Silver models; dbt_test_silver runs every Silver
    # test (grain uniqueness, the lookahead / reconstruction-labelling asserts). A test
    # failure fails the task and stops the DAG before Gold — stale-but-correct, never
    # fresh-but-wrong. Runs against prod (holdit_silver) via the mounted host ADC.
    DBT = "cd /opt/airflow/dbt && dbt"
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"{DBT} seed --target prod && "
            f"{DBT} snapshot --target prod && "
            f"{DBT} run --target prod --select silver"
        ),
    )
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"{DBT} test --target prod --select silver",
    )

    ingest_quote >> load_quote
    ingest_price >> load_price
    ingest_financials >> load_financials
    ingest_dividends >> load_dividends

    [load_quote, load_price, load_financials, load_dividends] >> dbt_run_silver
    dbt_run_silver >> dbt_test_silver

"""Trivial smoke DAG — proves the Airflow stack runs before any real task exists.

Step 2 exit criteria: get one trivial DAG green. It touches no GCP, no dbt, no
ingestion — it exists only to confirm the scheduler picks up a DAG and runs it.
The real orchestrator (holdit_weekly) lands in later steps.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _say_hello():
    print("holdit airflow stack is green")


with DAG(
    dag_id="holdit_smoke",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # triggered manually for the smoke test
    catchup=False,
    tags=["holdit", "smoke"],
) as dag:
    PythonOperator(task_id="say_hello", python_callable=_say_hello)

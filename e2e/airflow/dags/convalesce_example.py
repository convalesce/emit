"""The example workflow: one task that works, one that does not."""

from datetime import datetime

try:
    from airflow.sdk import DAG, task  # Airflow 3
except ImportError:
    from airflow import DAG
    from airflow.decorators import task


with DAG(
    dag_id="convalesce_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def extract():
        return 42

    @task
    def load(rows):
        raise RuntimeError(f"load failed on purpose after extracting {rows} rows")

    load(extract())

"""
The example workflow: one task that works, one that does not.

Lives in the Airflow container's DAG folder. Nothing here mentions the
plugin: it is discovered through the `airflow.plugins` entry point, which
is the point being tested.
"""

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
):

    @task
    def extract() -> int:
        """Return a row count."""
        return 42

    @task
    def load(rows: int) -> None:
        """Fail, so the failure path is exercised."""
        raise RuntimeError(
            f"load failed on purpose after extracting {rows} rows"
        )

    load(extract())

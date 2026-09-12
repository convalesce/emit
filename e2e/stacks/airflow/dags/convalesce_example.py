"""
The example workflow: two plain tasks (one that works, one that does not),
a SQLite-backed SQL task with real lineage, a task with declared
inlets/outlets, and, on Airflow 3, a task that writes through an
AssetAlias.

Lives in the Airflow container's DAG folder. Nothing here mentions the
plugin: it is discovered through the `airflow.plugins` entry point, which
is the point being tested.
"""

import sqlite3
from datetime import datetime

try:
    from airflow.sdk import DAG, Asset, AssetAlias, task  # Airflow 3

    AIRFLOW3 = True
except ImportError:
    from airflow import DAG
    from airflow.datasets import Dataset as Asset
    from airflow.decorators import task

    AssetAlias = None
    AIRFLOW3 = False

try:
    from airflow.providers.common.sql.operators.sql import (
        SQLExecuteQueryOperator,
    )
except ImportError:
    SQLExecuteQueryOperator = None

DB_PATH = "/opt/airflow/convalesce_example.db"
# Set as an env-var connection (`AIRFLOW_CONN_CONVALESCE_SQLITE` in the
# compose file) rather than written into the metadata DB at parse time:
# Airflow 3's dag-processor only reaches the API server over HTTP, so a
# direct-session write that works on Airflow 2 fails there.
CONN_ID = "convalesce_sqlite"

RAW_EVENTS = Asset(f"sqlite://{DB_PATH}/raw_events")
DAILY_TOTALS = Asset(f"sqlite://{DB_PATH}/daily_totals")


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

    @task(outlets=[RAW_EVENTS])
    def seed_raw_events() -> None:
        """Create and seed the table the SQL task reads."""
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_events (id INTEGER, amount INTEGER)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS daily_totals (id INTEGER, amount INTEGER)"
        )
        conn.execute("DELETE FROM raw_events")
        conn.execute("DELETE FROM daily_totals")
        conn.executemany(
            "INSERT INTO raw_events (id, amount) VALUES (?, ?)",
            [(1, 10), (2, 20), (3, 30)],
        )
        conn.commit()
        conn.close()

    if SQLExecuteQueryOperator is not None:
        summarize = SQLExecuteQueryOperator(
            task_id="summarize",
            conn_id=CONN_ID,
            sql="insert into daily_totals select id, amount from raw_events",
            inlets=[RAW_EVENTS],
            outlets=[DAILY_TOTALS],
        )
        seed_raw_events() >> summarize  # pylint: disable=expression-not-assigned

    if AIRFLOW3 and AssetAlias is not None:
        ALIAS = AssetAlias("convalesce_example_alias")

        @task(outlets=[ALIAS])
        def publish_via_alias(*, outlet_events=None) -> None:
            """Write the daily total through an alias, so the alias
            resolves to a real asset only once the run has happened."""
            if outlet_events is not None:
                outlet_events[ALIAS].add(DAILY_TOTALS)

        publish_via_alias()

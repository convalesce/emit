"""
The example workflow: a job that works, a job that does not, and the two
run-status sensors a customer writes to forward them.

Loaded by `dagster dev` from `workspace.yaml`.
"""

from typing import Any

import dagster
from dagster import DagsterRunStatus, DefaultSensorStatus, run_status_sensor

from convalesce_emit_dagster import convalesce_sensor


@dagster.op
def extract() -> int:
    """Return a row count."""
    return 42


@dagster.op
def load(rows: int) -> None:
    """Fail, so the failure path is exercised."""
    raise RuntimeError(f"load failed on purpose after extracting {rows} rows")


@dagster.op
def publish(rows: int) -> int:
    """Pass the count through."""
    return rows


@dagster.job
def nightly_ok() -> None:
    """The job that succeeds."""
    publish(extract())


@dagster.job
def nightly_broken() -> None:
    """The job that fails."""
    load(extract())


# Started rather than waiting to be switched on in the UI, and ticking
# every two seconds rather than thirty, so the test is not a wait.
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    minimum_interval_seconds=2,
    default_status=DefaultSensorStatus.RUNNING,
)
def convalesce_on_success(context: Any) -> None:
    """Forward a run that succeeded."""
    convalesce_sensor(context)


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    minimum_interval_seconds=2,
    default_status=DefaultSensorStatus.RUNNING,
)
def convalesce_on_failure(context: Any) -> None:
    """Forward a run that failed."""
    convalesce_sensor(context)


defs = dagster.Definitions(
    jobs=[nightly_ok, nightly_broken],
    sensors=[convalesce_on_success, convalesce_on_failure],
)

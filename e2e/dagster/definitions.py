"""The example workflow: a job that works, a job that does not, and the two
run-status sensors a customer writes to forward them."""

import dagster
from dagster import DagsterRunStatus, DefaultSensorStatus, run_status_sensor

from convalesce_emit_dagster import convalesce_sensor


@dagster.op
def extract():
    return 42


@dagster.op
def load(rows):
    raise RuntimeError(f"load failed on purpose after extracting {rows} rows")


@dagster.op
def publish(rows):
    return rows


@dagster.job
def nightly_ok():
    publish(extract())


@dagster.job
def nightly_broken():
    load(extract())


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    minimum_interval_seconds=2,
    default_status=DefaultSensorStatus.RUNNING,
)
def convalesce_on_success(context):
    convalesce_sensor(context)


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    minimum_interval_seconds=2,
    default_status=DefaultSensorStatus.RUNNING,
)
def convalesce_on_failure(context):
    convalesce_sensor(context)


defs = dagster.Definitions(
    jobs=[nightly_ok, nightly_broken],
    sensors=[convalesce_on_success, convalesce_on_failure],
)

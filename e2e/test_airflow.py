"""
A real Airflow runs the example DAG with the plugin installed.

The in-process probe in CI proves the hookspecs match. This proves the rest:
the plugin is discovered through the `airflow.plugins` entry point, fires in
the scheduler's own task processes, and what it sends actually reaches the
endpoint with the key.
"""

import json
import os
import subprocess
import time
from typing import Dict, List

import harness

TOOL = "airflow"


def compose_env(version: str) -> Dict[str, str]:
    major, minor = (int(x) for x in version.split(".")[:2])
    # The bare tag's Python moves with the release, and 2.5's is 3.7, below
    # the floor the packages declare. Name the Python the CI matrix uses.
    if (major, minor) >= (3, 0):
        python = "3.12"
    elif (major, minor) >= (2, 9):
        python = "3.11"
    else:
        python = "3.10"
    return {"EMIT_E2E_AIRFLOW_IMAGE": f"apache/airflow:{version}-python{python}"}


def _wait_for_dag(stack: harness.Stack, dag_id: str, timeout: float = 240) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = stack.exec("airflow", "airflow", "dags", "list", check=False)
        if result.returncode == 0 and dag_id in result.stdout:
            return
        time.sleep(3)
    raise AssertionError(f"{dag_id} never appeared in `airflow dags list`\n{stack.logs('airflow')[-4000:]}")


def _trigger(stack: harness.Stack, dag_id: str) -> None:
    stack.exec("airflow", "airflow", "dags", "unpause", dag_id)
    stack.exec("airflow", "airflow", "dags", "trigger", dag_id)


def test_example_dag_reaches_the_endpoint(stack: harness.Stack) -> None:
    version = os.environ["EMIT_E2E_AIRFLOW"]
    stack.up()
    _wait_for_dag(stack, "convalesce_example")
    _trigger(stack, "convalesce_example")

    wanted = {"on_task_instance_running", "on_task_instance_success", "on_task_instance_failed"}
    observations = stack.wait_for(
        lambda obs: wanted <= set(harness.events(obs)),
        timeout=300,
        what=f"task events {sorted(wanted)}",
    )

    for obs in observations:
        assert obs["tool"] == "airflow", obs
        assert obs["tool_version"] == version, obs["tool_version"]
        assert obs["client_version"], obs
        assert "workspace" not in obs, obs
    assert not stack.received()["rejected"], stack.received()["rejected"]

    wire = harness.dump(observations)
    assert "convalesce_example" in wire

    # Airflow hands listeners the failure only from 2.10; before that the
    # message is in the task log and nowhere a listener can reach.
    major, minor = (int(x) for x in version.split(".")[:2])
    failed = [o for o in observations if o["event"] == "on_task_instance_failed"]
    if (major, minor) >= (2, 10):
        assert any(o["payload"].get("error") for o in failed), "error was declared but arrived empty"
        assert "load failed on purpose" in wire, "the failure message never reached the wire"

    # The dag-run hooks fire in the scheduler, a different process from the
    # task hooks; both must have found their way out.
    observations = stack.wait_for(
        lambda obs: "on_dag_run_failed" in harness.events(obs),
        timeout=120,
        what="on_dag_run_failed from the scheduler",
    )
    print("events:", harness.events(observations))
    print("batches:", sorted({row["batch_size"] for row in stack.received()["observations"]}))

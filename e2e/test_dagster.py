"""
A real Dagster daemon ticks the run-status sensors that call the plugin.

The probe calls `convalesce_sensor` by hand. This launches two jobs through
the web server, the way the UI does, and lets the daemon's sensor
evaluation, in its own code-server process, find the runs and forward them.
"""

import json
import os
import time
import urllib.request
from typing import Any, Dict

import harness

TOOL = "dagster"


def compose_env(version: str) -> Dict[str, str]:
    major, minor = (int(x) for x in version.split(".")[:2])
    # Dagster before 1.9 does not resolve on Python 3.12.
    python = "3.12" if (major, minor) >= (1, 9) else "3.11"
    return {"EMIT_E2E_DAGSTER": version, "EMIT_E2E_PYTHON_IMAGE": f"python:{python}-slim"}


def _graphql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    port = os.environ.get("EMIT_E2E_DAGSTER_PORT", "13000")
    body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/graphql", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as resp:
        return json.load(resp)


def _wait_for_webserver(timeout: float = 180) -> Dict[str, str]:
    """Wait until the code location is loaded, and return its selector."""
    query = """
      { repositoriesOrError { ... on RepositoryConnection {
          nodes { name location { name } } } } }
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            nodes = _graphql(query, {})["data"]["repositoriesOrError"]["nodes"]
        except Exception:  # not up yet
            nodes = []
        if nodes:
            return {"repositoryLocationName": nodes[0]["location"]["name"], "repositoryName": nodes[0]["name"]}
        time.sleep(3)
    raise AssertionError("the code location never loaded")


def _launch(selector: Dict[str, str], job: str) -> str:
    mutation = """
      mutation($params: ExecutionParams!) {
        launchRun(executionParams: $params) {
          __typename
          ... on LaunchRunSuccess { run { runId } }
          ... on PythonError { message }
          ... on RunConfigValidationInvalid { errors { message } }
        }
      }
    """
    params = {"selector": {**selector, "jobName": job}, "runConfigData": {}}
    result = _graphql(mutation, {"params": params})["data"]["launchRun"]
    assert result["__typename"] == "LaunchRunSuccess", result
    return str(result["run"]["runId"])


def test_run_status_sensors_reach_the_endpoint(stack: harness.Stack) -> None:
    version = os.environ["EMIT_E2E_DAGSTER"]
    stack.up()
    selector = _wait_for_webserver()
    print("launched", _launch(selector, "nightly_ok"), _launch(selector, "nightly_broken"))

    def both_runs(obs):
        wire = harness.dump(obs)
        return "nightly_ok" in wire and "nightly_broken" in wire

    observations = stack.wait_for(both_runs, timeout=240, what="run_status for both jobs")

    for obs in observations:
        assert obs["tool"] == "dagster", obs
        assert obs["event"] == "run_status", obs
        assert obs["tool_version"] == version, obs["tool_version"]
        run = obs["payload"]["dagster_run"]
        assert isinstance(run, dict) and run.get("job_name"), obs["payload"]
        assert isinstance(obs["payload"]["dagster_event"], dict), obs["payload"]
    assert not stack.received()["rejected"], stack.received()["rejected"]
    wire = harness.dump(observations)
    assert "load failed on purpose" in wire, "the failure message never reached the wire"
    print("events:", harness.events(observations), "count:", len(observations))

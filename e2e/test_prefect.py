"""
Real flows run against a real Prefect server, with the hooks attached.

The probe calls one hook by hand. This lets Prefect itself call them, by
keyword on 2.x and positionally on 3.x, for flows and tasks, on completion
and on failure.
"""

import os
from typing import Dict

import harness

TOOL = "prefect"


def compose_env(version: str) -> Dict[str, str]:
    major = int(version.split(".")[0])
    python = "3.12" if major >= 3 else "3.11"
    return {"EMIT_E2E_PREFECT_IMAGE": f"prefecthq/prefect:{version}-python{python}"}


def test_flow_and_task_hooks_reach_the_endpoint(stack: harness.Stack) -> None:
    version = os.environ["EMIT_E2E_PREFECT"]
    stack.up("receiver", "server")
    stack.run("flows")

    def all_four(obs):
        wire = harness.dump(obs)
        return (
            {"flow_run", "task_run"} <= set(harness.events(obs))
            and "nightly_ok" in wire
            and "nightly_broken" in wire
            and "load failed on purpose" in wire
        )

    observations = stack.wait_for(all_four, timeout=60, what="flow and task hooks for both flows")

    for obs in observations:
        assert obs["tool"] == "prefect", obs
        assert obs["tool_version"] == version, obs["tool_version"]
    assert not stack.received()["rejected"], stack.received()["rejected"]
    flow_runs = [o for o in observations if o["event"] == "flow_run"]
    states = sorted(str(o["payload"]["state"].get("type") or o["payload"]["state"].get("name")) for o in flow_runs)
    print("flow states:", states, "observations:", len(observations))
    assert any("COMPLETED" in s.upper() for s in states), states
    assert any("FAILED" in s.upper() for s in states), states

"""
A real checkpoint on a real Great Expectations, with the action listed the
way a customer lists it, on both majors.

The probe calls the action's run method by hand. This lets GX construct the
action from its class path, hand it the result, and shows the wire carries
counts and never rows.
"""

import os
from typing import Dict

import harness

TOOL = "gx"


def compose_env(version: str) -> Dict[str, str]:
    major = int(version.split(".")[0])
    python = "3.12" if major >= 1 else "3.11"
    return {"EMIT_E2E_GX": version, "EMIT_E2E_PYTHON_IMAGE": f"python:{python}-slim"}


def test_checkpoint_action_reaches_the_endpoint(stack: harness.Stack) -> None:
    version = os.environ["EMIT_E2E_GX"]
    stack.up("receiver")
    stack.run("checkpoint")

    observations = stack.wait_for(lambda obs: "validation_result" in harness.events(obs), timeout=30, what="the validation result")

    for obs in observations:
        assert obs["tool"] == "great_expectations", obs
        assert obs["tool_version"] == version, obs["tool_version"]
    assert not stack.received()["rejected"], stack.received()["rejected"]
    wire = harness.dump(observations)
    assert "alice@example.com" not in wire, "a row value reached the wire"
    assert "unexpected_count" in wire, "the counts detection reads were dropped"
    print("events:", harness.events(observations), "bytes:", len(wire))

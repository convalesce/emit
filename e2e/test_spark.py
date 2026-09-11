"""
A real spark-submit on the official image, with the listener attached and
the endpoint reachable.

CI already runs this job in dry-run mode. This is the same job sending for
real: the jar has to reach the receiver, with the key, from inside the
driver, and flush what it batched before the driver exits.
"""

import glob
import os
from typing import Dict

import pytest

import harness

TOOL = "spark"


def compose_env(version: str) -> Dict[str, str]:
    return {"EMIT_E2E_SPARK": version}


def test_listener_reaches_the_endpoint(stack: harness.Stack) -> None:
    version = os.environ["EMIT_E2E_SPARK"]
    jars = glob.glob(str(harness.REPO / "java" / "emit-spark" / "build" / "libs" / "*.jar"))
    if not jars:
        pytest.fail("build the jars first: cd java && ./gradlew build -x test")
    stack.up("receiver")
    stack.run("spark")

    wanted = {"SparkListenerApplicationStart", "SparkListenerJobEnd", "SparkListenerApplicationEnd"}
    observations = stack.wait_for(
        lambda obs: wanted <= set(harness.events(obs)), timeout=30, what=f"events {sorted(wanted)}"
    )
    for obs in observations:
        assert obs["tool"] == "spark", obs
        assert obs["tool_version"] == version, obs["tool_version"]
        assert isinstance(obs["payload"], dict), "Spark's own JSON should arrive embedded, not as a string"
    assert not stack.received()["rejected"], stack.received()["rejected"]
    print("events:", len(harness.events(observations)), "observations:", len(observations))

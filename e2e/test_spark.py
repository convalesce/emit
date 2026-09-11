"""
A real spark-submit on the official image, with the listener attached and
the endpoint reachable.

CI already runs this job in dry-run mode. This is the same job sending for
real: the jar has to reach the receiver, with the key, from inside the
driver, and flush what it batched before the driver exits.

Run with `EMIT_E2E_SPARK=<version> pytest test_spark.py`, after
`cd java && ./gradlew build -x test`.
"""

import logging
from typing import Dict

import harness as e2eharn

_LOG = logging.getLogger(__name__)

JARS = e2eharn.REPO / "java" / "emit-spark" / "build" / "libs"
WANTED = {
    "SparkListenerApplicationStart",
    "SparkListenerJobEnd",
    "SparkListenerApplicationEnd",
}


# #############################################################################
# Test_spark1
# #############################################################################


class Test_spark1(e2eharn.StackCase):
    """
    Test that the listener's events reach the endpoint from a real driver.
    """

    TOOL = "spark"

    @classmethod
    def compose_env(cls, version: str) -> Dict[str, str]:
        return {"EMIT_E2E_SPARK": version}

    def test1(self) -> None:
        """
        Test that application, job and shutdown events all arrive embedded.
        """
        if not list(JARS.glob("*.jar")):
            self.fail("build the jars first: cd java && ./gradlew build -x test")
        with self.logs_on_failure():
            self.stack.up("receiver")
            self.stack.run("spark")
            observations = self.stack.wait_for(
                lambda obs: WANTED <= set(e2eharn.events(obs)),
                timeout=30,
                what=f"events {sorted(WANTED)}",
            )
            self.assert_envelopes(observations, "spark")
            for observation in observations:
                # Spark's own JSON must arrive as an object, not a string.
                self.assertIsInstance(observation["payload"], dict)
            _LOG.info("events: %d", len(e2eharn.events(observations)))

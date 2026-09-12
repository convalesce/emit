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
from typing import Dict, List

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
            self._assert_application_is_named(observations)
            self._assert_only_the_run_arrived(observations)
            _LOG.info("events: %s", e2eharn.events(observations))

    def _assert_application_is_named(
        self, observations: List[e2eharn.Observation]
    ) -> None:
        """
        Check every event says which application it came from.

        Spark names it on the application-start event and in a job's
        properties and nowhere else, so a job end or an application end
        said nothing, and a receiver seeing two drivers at once had to
        guess which one an event belonged to.

        :param observations: what the receiver recorded
        :return: nothing
        """
        ids = set()
        for observation in observations:
            app_id = observation["payload"].get("App ID")
            self.assertTrue(
                app_id,
                f"{observation['event']} named no application: "
                f"{sorted(observation['payload'])}",
            )
            ids.add(app_id)
        self.assertEqual(len(ids), 1, f"one driver, several ids: {ids}")

    def _assert_only_the_run_arrived(
        self, observations: List[e2eharn.Observation]
    ) -> None:
        """
        Check the inside of a job stayed in the driver.

        One event per task, about 190 values each, is what a job with ten
        thousand tasks would have sent.

        :param observations: what the receiver recorded
        :return: nothing
        """
        events = set(e2eharn.events(observations))
        for unwanted in (
            "SparkListenerTaskEnd",
            "SparkListenerStageCompleted",
            "SparkListenerSQLAdaptiveExecutionUpdate",
        ):
            self.assertNotIn(unwanted, events)

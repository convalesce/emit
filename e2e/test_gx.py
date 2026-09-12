"""
A real checkpoint on a real Great Expectations, with the action listed the
way a customer lists it, on both majors.

The probe calls the action's run method by hand. This lets GX construct the
action from its class path, hand it the result, and shows the wire carries
counts and never rows.

Run with `EMIT_E2E_GX=<version> pytest test_gx.py`.
"""

import json
import logging
from typing import Dict, List

import harness as e2eharn

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_gx1
# #############################################################################


class Test_gx1(e2eharn.StackCase):
    """
    Test that a checkpoint's validation result reaches the endpoint.
    """

    TOOL = "gx"

    @classmethod
    def compose_env(cls, version: str) -> Dict[str, str]:
        major, _ = e2eharn.find_version_parts(version)
        python = "3.12" if major >= 1 else "3.11"
        return {
            "EMIT_E2E_GX": version,
            "EMIT_E2E_PYTHON_IMAGE": f"python:{python}-slim",
        }

    def test1(self) -> None:
        """
        Test that counts cross and row values do not.
        """
        with self.logs_on_failure():
            self.stack.up("receiver")
            self.stack.run("checkpoint")
            observations = self.stack.wait_for(
                lambda obs: "validation_result" in e2eharn.events(obs),
                timeout=30,
                what="the validation result",
            )
            self.assert_envelopes(observations, "great_expectations")
            text = e2eharn.wire(observations)
            self.assertNotIn("alice@example.com", text, "a row reached the wire")
            self.assertIn("unexpected_count", text, "the counts were dropped")
            self._assert_runtime_left_behind(observations)

    def _assert_runtime_left_behind(
        self, observations: List[e2eharn.Observation]
    ) -> None:
        """
        Check the tool's runtime did not travel with the result.

        On 0.x the action is handed a live validator, which owns the data
        context, every store's configuration and the frame that was
        validated: most of the payload, none of it read, and the shortest
        path to the customer's rows.

        :param observations: what the receiver recorded
        :return: nothing
        """
        for observation in observations:
            payload = observation["payload"]
            kwargs = payload.get("kwargs") or {}
            self.assertNotIn("data_asset", kwargs)
            text = json.dumps(payload, default=str)
            self.assertNotIn("InMemoryStoreBackend", text, "a store crossed")
            self.assertNotIn("execution_engine", text, "the engine crossed")
            # What names the batch is still there, on the result's own meta.
            self.assertIn("orders", text)
            _LOG.info("wire bytes: %d", len(text))

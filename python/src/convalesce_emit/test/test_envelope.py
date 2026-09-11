"""
Tests for the wrapper an observation travels in.

Run with `make test`.
"""

import logging
import unittest

import convalesce_emit.envelope as ceenvelo

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_envelope1
# #############################################################################


class Test_envelope1(unittest.TestCase):
    """
    Test that a tool's payload crosses the wrapper untouched.
    """

    def test1(self) -> None:
        """
        Test that the payload is neither copied nor reshaped.

        This is the whole claim the package makes: if anything here starts
        rewriting a payload, the receiver's mapping silently sees different
        input than the tool produced.
        """
        payload = {"nested": [1, {"a": None}], "raw": "as-is"}
        observation = ceenvelo.build(
            tool="airflow", event="task_finished", payload=payload
        )
        self.assertEqual(observation.payload, payload)
        self.assertIs(observation.payload, payload)

    def test2(self) -> None:
        """
        Test that the wrapper carries what the receiver needs to route it.
        """
        observation = ceenvelo.build(
            tool="gx",
            event="validation",
            payload={},
            tool_version="1.2",
        )
        self.assertEqual(observation.tool, "gx")
        self.assertEqual(observation.event, "validation")
        self.assertEqual(observation.tool_version, "1.2")
        self.assertEqual(observation.envelope_version, ceenvelo.ENVELOPE_VERSION)

    def test3(self) -> None:
        """
        Test that every observation is identifiable and timestamped.

        A redelivered batch is deduplicated on the id, so two observations
        must never share one.
        """
        first = ceenvelo.build(tool="t", event="e", payload={})
        second = ceenvelo.build(tool="t", event="e", payload={})
        self.assertNotEqual(first.observation_id, second.observation_id)
        self.assertTrue(first.emitted_at)
        self.assertTrue(first.client_version)

    def test4(self) -> None:
        """
        Test that an observation flattens to a JSON-encodable dict.
        """
        observation = ceenvelo.build(tool="t", event="e", payload={"a": 1})
        as_dict = observation.to_dict()
        self.assertEqual(as_dict["payload"], {"a": 1})
        self.assertEqual(as_dict["tool"], "t")

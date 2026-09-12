"""
Tests for forwarding a Great Expectations validation result.

The version dispatch itself is exercised against real installs of both
majors; these cover the parts that hold whichever is present.

Run with `make test`.
"""

import json
import logging
import os
import unittest
import unittest.mock
from typing import Any, List

import convalesce_emit_gx._common as cegxcom
import convalesce_emit_gx.action as cegxact

_LOG = logging.getLogger(__name__)

_RESULT = {
    "success": False,
    "result": {
        "element_count": 100,
        "unexpected_count": 2,
        "partial_unexpected_list": ["alice@x.com", "bob@x.com"],
    },
}


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Any] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


# #############################################################################
# Test_gx_forward1
# #############################################################################


class Test_gx_forward1(unittest.TestCase):
    """
    Test that row values do not leave the process by default.
    """

    def test1(self) -> None:
        """
        Test that sample values are redacted before sending.

        This is the test that keeps the handbook honest: a GX result carries
        real failing rows, and we promise not to send them.
        """
        recorder = _Recorder()
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            outcome = cegxcom.forward(_RESULT, recorder)
        wire = json.dumps(recorder.sent, default=str)
        self.assertNotIn("alice@x.com", wire)
        self.assertIn("100", wire)
        self.assertTrue(outcome["convalesce_emitted"])
        self.assertTrue(outcome["redacted"])

    def test2(self) -> None:
        """
        Test that samples can be sent when that is chosen deliberately.
        """
        recorder = _Recorder()
        env = {"CONVALESCE_GX_SEND_SAMPLES": "true"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            outcome = cegxcom.forward(_RESULT, recorder)
        self.assertIn("alice@x.com", json.dumps(recorder.sent, default=str))
        self.assertFalse(outcome["redacted"])

    def test3(self) -> None:
        """
        Test that a checkpoint does not fail when we cannot send.
        """

        class _Broken:
            """Stands in for an unreachable endpoint."""

            def emit(self, **_kwargs: Any) -> None:
                """Fail the way a dead endpoint does."""
                raise RuntimeError("down")

            def flush(self) -> None:
                """Nothing is queued, so nothing to send."""

        outcome = cegxcom.forward(_RESULT, _Broken())
        self.assertFalse(outcome["convalesce_emitted"])


# #############################################################################
# Test_gx_dispatch1
# #############################################################################


class Test_gx_dispatch1(unittest.TestCase):
    """
    Test that the right action class is chosen for the installed GX.
    """

    def test1(self) -> None:
        """
        Test that no Great Expectations means no action class.

        Importing the plugin must not fail where GX is absent; it simply has
        nothing to offer.
        """
        self.assertIsNone(cegxact.gx_major())
        self.assertIsNone(cegxact.ConvalesceValidationAction)


# #############################################################################
# Test_gx_runtime1
# #############################################################################


class _Frame:
    """Stands in for the pandas frame a validator owns."""

    shape = (4, 2)

    def to_dict(self) -> Any:
        """Return every row, the way pandas does."""
        return {"email": {"0": "alice@x.com"}}


class _Engine:
    """Stands in for a GX execution engine holding the batch."""

    def __init__(self) -> None:
        self.batch_cache = {"orders": {"data": _Frame()}}


class _Validator:
    """Stands in for the 0.x `data_asset`: a live validator."""

    def __init__(self) -> None:
        self.interactive_evaluation = True
        self.execution_engine = _Engine()
        self.data_context = {"stores": {"expectations_store": {"class": "X"}}}


class Test_gx_runtime1(unittest.TestCase):
    """
    Test that the tool's runtime is not forwarded with the result.
    """

    def test1(self) -> None:
        """
        Test that the 0.x validator is left behind by name.

        It owns the data context, every store's configuration and the frame
        that was validated: most of the payload, none of it read, and the
        shortest path to the customer's rows.
        """
        recorder = _Recorder()
        cegxcom.forward(
            {
                "args": [],
                "kwargs": {
                    "validation_result_suite": {"results": [_RESULT]},
                    "data_asset": _Validator(),
                    "expectation_suite_identifier": "orders",
                },
            },
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("data_asset", payload["kwargs"])
        self.assertIn("validation_result_suite", payload["kwargs"])
        self.assertEqual(
            payload["kwargs"]["expectation_suite_identifier"], "orders"
        )
        self.assertNotIn("alice@x.com", json.dumps(payload))

    def test2(self) -> None:
        """
        Test that a runtime arriving positionally is left behind too.

        GX 0.x has changed which arguments it passes positionally across
        point releases, so the name alone is not enough.
        """
        recorder = _Recorder()
        cegxcom.forward(
            {"args": [{"results": [_RESULT]}, _Validator()], "kwargs": {}},
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(len(payload["args"]), 1)
        self.assertIn("results", payload["args"][0])
        self.assertNotIn("alice@x.com", json.dumps(payload))

    def test3(self) -> None:
        """
        Test that the 1.x checkpoint result is not mistaken for runtime.

        It is the whole point of the observation, and it names no engine or
        context of its own.
        """

        class CheckpointResult:
            """Stands in for a 1.x CheckpointResult."""

            def __init__(self) -> None:
                self.success = False
                self.run_results = {"orders": {"results": [_RESULT]}}

        recorder = _Recorder()
        cegxcom.forward(
            {"args": [], "kwargs": {"checkpoint_result": CheckpointResult()}},
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertIn("checkpoint_result", payload["kwargs"])
        self.assertFalse(payload["kwargs"]["checkpoint_result"]["success"])

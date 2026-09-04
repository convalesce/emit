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

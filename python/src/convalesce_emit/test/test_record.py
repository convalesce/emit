"""
Tests for the recording operator.

Run with `make test`.
"""

import logging
import unittest
from typing import Any, Dict, List, Optional

import convalesce_emit.record as cerecord

_LOG = logging.getLogger(__name__)


# #############################################################################
# _FakeEmitter
# #############################################################################


class _FakeEmitter:
    """
    Records what it was handed, so a test can read it back without a network.
    """

    def __init__(self) -> None:
        self.emitted: List[Dict[str, Any]] = []
        self.flushed = False

    def emit(
        self,
        *,
        tool: str,
        event: str,
        payload: Any,
        tool_version: Optional[str] = None,
        excluded: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        """Record one observation."""
        self.emitted.append(
            {
                "tool": tool,
                "event": event,
                "payload": payload,
                "tool_version": tool_version,
                "excluded": excluded,
            }
        )

    def flush(self) -> None:
        """Record that a flush happened."""
        self.flushed = True


# #############################################################################
# Test_record1
# #############################################################################


class Test_record1(unittest.TestCase):
    """
    Test that recording forwards exactly what the author supplied.
    """

    def test1(self) -> None:
        """
        Test that an operation outcome is sent under the "operation" tool.
        """
        emitter = _FakeEmitter()
        cerecord.record(
            dataset="orders",
            event=cerecord.OPERATION,
            outcome="SUCCESS",
            emitter=emitter,
        )
        self.assertEqual(len(emitter.emitted), 1)
        sent = emitter.emitted[0]
        self.assertEqual(sent["tool"], "operation")
        self.assertEqual(
            sent["payload"], {"dataset": "orders", "outcome": "SUCCESS"}
        )
        self.assertTrue(emitter.flushed)

    def test2(self) -> None:
        """
        Test that an assertion outcome is sent under the "assertion" tool.
        """
        emitter = _FakeEmitter()
        cerecord.record(
            dataset="orders",
            event=cerecord.ASSERTION,
            outcome="FAILURE",
            emitter=emitter,
        )
        sent = emitter.emitted[0]
        self.assertEqual(sent["tool"], "assertion")
        self.assertEqual(sent["payload"]["outcome"], "FAILURE")

    def test3(self) -> None:
        """
        Test that extra details are forwarded verbatim, never inspected.
        """
        emitter = _FakeEmitter()
        details = {"severity": "high", "rows_affected": 12}
        cerecord.record(
            dataset="orders",
            event=cerecord.OPERATION,
            outcome="SUCCESS",
            details=details,
            emitter=emitter,
        )
        sent = emitter.emitted[0]
        self.assertEqual(sent["payload"]["details"], details)

    def test4(self) -> None:
        """
        Test that no "details" key is sent when none was given.

        A key that is always present, even empty, is one more thing a
        translator has to special-case; omitting it entirely when the
        author supplied nothing keeps the payload's shape honest.
        """
        emitter = _FakeEmitter()
        cerecord.record(
            dataset="orders",
            event=cerecord.OPERATION,
            outcome="SUCCESS",
            emitter=emitter,
        )
        self.assertNotIn("details", emitter.emitted[0]["payload"])

    def test5(self) -> None:
        """
        Test that an unrecognized event is refused before anything is sent.

        This is a caller mistake, not a transport failure, so it is not
        swallowed the way a network problem would be: the rest of this
        package protects a pipeline from *our* endpoint having a bad
        minute, not from the caller's own typo.
        """
        emitter = _FakeEmitter()
        with self.assertRaises(ValueError):
            cerecord.record(
                dataset="orders",
                event="not-a-real-event",
                outcome="SUCCESS",
                emitter=emitter,
            )
        self.assertEqual(emitter.emitted, [])

    def test6(self) -> None:
        """
        Test that the dataset name crosses exactly as given, never touched.

        The operator builds no urns: whatever string the author passes is
        exactly the string that arrives in the payload.
        """
        emitter = _FakeEmitter()
        cerecord.record(
            dataset="urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)",
            event=cerecord.OPERATION,
            outcome="SUCCESS",
            emitter=emitter,
        )
        self.assertEqual(
            emitter.emitted[0]["payload"]["dataset"],
            "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)",
        )

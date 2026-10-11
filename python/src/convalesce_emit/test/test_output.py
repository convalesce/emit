"""
Tests for the tail of a failed unit's output.

Run with `make test`.
"""

import json
import logging
import pathlib
import unittest
import unittest.mock as umock
from typing import Any, List

import convalesce_emit.output as ceoutput

_LOG = logging.getLogger(__name__)

_OFF = {"CONVALESCE_SEND_OUTPUT": "false"}
_VECTORS = (
    pathlib.Path(__file__).resolve().parents[4]
    / "testdata"
    / "output_vectors.json"
)
_PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n"
    "-----END PRIVATE KEY-----"
)


def _reasons(excluded: List[Any]) -> List[str]:
    """The reasons of a list of exclusions, in order."""
    return [item["reason"] for item in excluded]


# #############################################################################
# Test_enabled1
# #############################################################################


class Test_enabled1(unittest.TestCase):
    """
    Test that output is sent unless the setting says not to.
    """

    def test1(self) -> None:
        """
        Test that output is sent when the setting is absent.
        """
        self.assertTrue(ceoutput.enabled({}))

    def test2(self) -> None:
        """
        Test that each way of saying no turns it off.
        """
        for value in ("false", "0", "no", "OFF", " False "):
            self.assertFalse(
                ceoutput.enabled({"CONVALESCE_SEND_OUTPUT": value}), value
            )

    def test3(self) -> None:
        """
        Test that the setting is read under the platform's prefix too.
        """
        self.assertFalse(
            ceoutput.enabled({"CUSTOMER_CONVALESCE_SEND_OUTPUT": "false"})
        )

    def test4(self) -> None:
        """
        Test that nothing is made of an output while the setting is off.
        """
        with umock.patch.dict("os.environ", _OFF):
            self.assertEqual(ceoutput.tail("it failed"), ("", []))
            payload: dict = {}
            self.assertEqual(ceoutput.add(payload, ["it failed"]), [])
            self.assertEqual(payload, {})
            self.assertIsNone(ceoutput.watch("convalesce.test.off"))


# #############################################################################
# Test_tail1
# #############################################################################


class Test_tail1(unittest.TestCase):
    """
    Test what is sent of an output.
    """

    def test1(self) -> None:
        """
        Test that the shared vectors hold, as the Java library's must.
        """
        vectors = json.loads(_VECTORS.read_text(encoding="utf-8"))
        for row in vectors["tails"]:
            text, excluded = ceoutput.tail(row["output"])
            self.assertEqual(text, row["tail"], row["name"])
            self.assertEqual(_reasons(excluded), row["excluded"], row["name"])
            for item in excluded:
                self.assertEqual(item["path"], "output_tail")

    def test2(self) -> None:
        """
        Test that the end is kept when there are too many lines.
        """
        lines = [f"line {i}" for i in range(500)]
        text, excluded = ceoutput.tail(lines)
        self.assertEqual(text.split("\n"), lines[-ceoutput.MAX_LINES :])
        self.assertEqual(_reasons(excluded), ["limited to 60 lines"])

    def test3(self) -> None:
        """
        Test that the end is kept, in whole lines, when there are too many
        bytes.
        """
        lines = [f"{i:04d} " + "x" * 195 for i in range(60)]
        text, excluded = ceoutput.tail(lines)
        self.assertLessEqual(len(text.encode("utf-8")), ceoutput.MAX_BYTES)
        kept = text.split("\n")
        self.assertEqual(kept, lines[-len(kept) :])
        self.assertEqual(len(kept), 20)
        self.assertEqual(_reasons(excluded), ["limited to 4096 bytes"])

    def test4(self) -> None:
        """
        Test that no seeded credential is in what is sent.
        """
        output = [
            "starting",
            "export DB_PASSWORD=hunter2",
            "connecting to postgresql://svc:hunter2@db.internal/app",
            _PEM,
            "Database Error: invalid identifier 'SALES_AMOUNT'",
        ]
        text, excluded = ceoutput.tail(output)
        self.assertNotIn("hunter2", text)
        self.assertNotIn("MIIEvQIBADANBgkqhkiG9w0BAQEFAASC", text)
        self.assertNotIn("PRIVATE KEY", text)
        self.assertIn("svc:***@db.internal", text)
        self.assertIn("invalid identifier 'SALES_AMOUNT'", text)
        self.assertEqual(
            excluded, [{"path": "output_tail", "reason": "credential masked"}]
        )

    def test5(self) -> None:
        """
        Test that a credential that only the cut would have hidden is
        masked first, so no half of one is left.
        """
        output = "password=" + "h" * 6000
        text, excluded = ceoutput.tail(output)
        self.assertEqual(text, "password=***")
        self.assertEqual(_reasons(excluded), ["credential masked"])

    def test6(self) -> None:
        """
        Test that the path a caller names is the one declared.
        """
        _, excluded = ceoutput.tail("token=abc", path="steps[2].output_tail")
        self.assertEqual(excluded[0]["path"], "steps[2].output_tail")

    def test7(self) -> None:
        """
        Test that no output, and output that cannot be read, is nothing.
        """
        self.assertEqual(ceoutput.tail(None), ("", []))
        self.assertEqual(ceoutput.tail([]), ("", []))
        with self.assertLogs("convalesce_emit.output", "WARNING") as logs:
            self.assertEqual(ceoutput.tail([1, 2]), ("", []))  # type: ignore[list-item]
        self.assertIn("TypeError", logs.output[0])

    def test8(self) -> None:
        """
        Test that a character that cannot be encoded does not lose the
        output.
        """
        text, _ = ceoutput.tail("bad \udc80 byte")
        self.assertEqual(text, "bad ? byte")

    def test9(self) -> None:
        """
        Test that an output is put on a payload only when there is one.
        """
        payload: dict = {"event": "x"}
        self.assertEqual(ceoutput.add(payload, []), [])
        self.assertEqual(payload, {"event": "x"})
        excluded = ceoutput.add(payload, ["secret: abc", "failed"])
        self.assertEqual(payload["output_tail"], "secret: ***\nfailed")
        self.assertEqual(_reasons(excluded), ["credential masked"])


# #############################################################################
# Test_ring1
# #############################################################################


class Test_ring1(unittest.TestCase):
    """
    Test the handler that holds the last records.
    """

    def setUp(self) -> None:
        self.logger = logging.getLogger("convalesce.test.ring")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        self.ring = ceoutput.Ring(capacity=3)
        self.logger.addHandler(self.ring)
        self.addCleanup(self.logger.removeHandler, self.ring)

    def test1(self) -> None:
        """
        Test that only the last records are held, oldest first.
        """
        for i in range(10):
            self.logger.info("record %d", i)
        self.assertEqual(self.ring.lines(), ["record 7", "record 8", "record 9"])

    def test2(self) -> None:
        """
        Test that a record handed over twice is held once.
        """
        child = logging.getLogger("convalesce.test.ring.child")
        child.addHandler(self.ring)
        self.addCleanup(child.removeHandler, self.ring)
        child.warning("once")
        self.assertEqual(self.ring.lines(), ["once"])

    def test3(self) -> None:
        """
        Test that a record that cannot be read never raises into the code
        that logged it.
        """
        # Handed straight to the ring: a test runner's own handler, on the
        # same logger, raises for such a record.
        record = logging.LogRecord(
            "x", logging.INFO, __file__, 1, "%d items", ("not a number",), None
        )
        self.ring.handle(record)
        self.logger.info("after")
        self.assertEqual(self.ring.lines(), ["after"])

    def test4(self) -> None:
        """
        Test that a handler that breaks never raises into the code that
        logged.
        """
        with umock.patch.object(
            ceoutput, "text_of", side_effect=RuntimeError("password=hunter2")
        ):
            self.logger.error("lost")
        self.assertEqual(self.ring.lines(), [])

    def test5(self) -> None:
        """
        Test that a traceback logged with a record is not part of its
        text: the failure's own crosses as `error_detail`.
        """
        try:
            raise ValueError("no such column")
        except ValueError:
            self.logger.exception("query failed")
        self.assertEqual(self.ring.lines(), ["query failed"])

    def test6(self) -> None:
        """
        Test that a very long record keeps its end, from a line break.
        """
        long = "head " * 4000 + "\nthe end"
        self.logger.info(long)
        self.assertEqual(self.ring.lines(), ["the end"])
        self.logger.info("x" * 20000)
        self.assertEqual(len(self.ring.lines()[-1]), ceoutput.MAX_RECORD_CHARS)

    def test7(self) -> None:
        """
        Test that forgetting leaves nothing held.
        """
        self.logger.info("held")
        self.ring.clear()
        self.assertEqual(self.ring.lines(), [])


# #############################################################################
# Test_watch1
# #############################################################################


class Test_watch1(unittest.TestCase):
    """
    Test putting a ring on loggers and taking it off.
    """

    def test1(self) -> None:
        """
        Test that a ring is put on each logger once and taken off cleanly.
        """
        name = "convalesce.test.watch"
        loose = logging.Logger("in no hierarchy")
        before = list(logging.getLogger(name).handlers)
        ring = ceoutput.watch(name, loggers=[loose])
        assert ring is not None
        self.assertIs(ceoutput.watch(name, ring=ring, loggers=[loose]), ring)
        self.assertEqual(logging.getLogger(name).handlers.count(ring), 1)
        self.assertEqual(loose.handlers.count(ring), 1)
        logging.getLogger(name).error("seen")
        loose.error("and this")
        self.assertEqual(ring.lines(), ["seen", "and this"])
        ceoutput.unwatch(ring, name, loggers=[loose])
        self.assertEqual(logging.getLogger(name).handlers, before)
        self.assertEqual(loose.handlers, [])

    def test2(self) -> None:
        """
        Test that a ring that could not be put in place is reported by
        the error's class and nothing is raised.
        """
        broken = umock.Mock()
        broken.handlers = []
        broken.addHandler.side_effect = RuntimeError("password=hunter2")
        with self.assertLogs("convalesce_emit.output", "WARNING") as logs:
            self.assertIsNone(ceoutput.watch(loggers=[broken]))
        self.assertIn("RuntimeError", logs.output[0])
        self.assertNotIn("hunter2", logs.output[0])
        broken.removeHandler.side_effect = RuntimeError("password=hunter2")
        with self.assertLogs("convalesce_emit.output", "WARNING") as logs:
            ceoutput.unwatch(ceoutput.Ring(), loggers=[broken])
        self.assertNotIn("hunter2", logs.output[0])
        ceoutput.unwatch(None, "convalesce.test.watch")

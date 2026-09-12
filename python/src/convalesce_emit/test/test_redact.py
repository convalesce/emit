"""
Tests for stripping sample data values before they are sent.

Run with `make test`.
"""

import json
import logging
import unittest

import convalesce_emit.redact as ceredact

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_redact1
# #############################################################################


class Test_redact1(unittest.TestCase):
    """
    Test that row values never survive redaction while counts do.
    """

    def test1(self) -> None:
        """
        Test that sample values are replaced by their count.

        The handbook promises we read "run outcomes, row counts, lineage.
        Not the rows themselves", and a Great Expectations result carries
        real failing rows, so this is the test that keeps that true.
        """
        payload = {
            "success": False,
            "result": {
                "element_count": 1000,
                "unexpected_count": 3,
                "partial_unexpected_list": ["alice@x.com", "bob@x.com"],
            },
        }
        out, _ = ceredact.redact_samples(payload)
        self.assertEqual(out["result"]["element_count"], 1000)
        self.assertEqual(out["result"]["unexpected_count"], 3)
        self.assertEqual(
            out["result"]["partial_unexpected_list"],
            {"redacted": True, "count": 2},
        )
        self.assertNotIn("alice@x.com", json.dumps(out))

    def test2(self) -> None:
        """
        Test that samples nested inside lists are found too.
        """
        payload = {"runs": [{"result": {"unexpected_list": [1, 2, 3]}}]}
        out, _ = ceredact.redact_samples(payload)
        self.assertEqual(
            out["runs"][0]["result"]["unexpected_list"],
            {"redacted": True, "count": 3},
        )

    def test3(self) -> None:
        """
        Test that everything else is left exactly as it was.

        `observed_value` is deliberately not redacted: for most expectations
        it is an aggregate and it is the substance of the result.
        """
        payload = {"observed_value": 42, "keep": ["a", "b"]}
        out, excluded = ceredact.redact_samples(payload)
        self.assertEqual(out, payload)
        self.assertEqual(excluded, [])


# #############################################################################
# Test_redact_excluded1
# #############################################################################


class Test_redact_excluded1(unittest.TestCase):
    """
    Test that every redaction is declared, by path and reason.
    """

    def test1(self) -> None:
        """
        Test that a top-level redaction is declared with its own key as the
        path.
        """
        payload = {"partial_unexpected_list": ["alice@x.com"]}
        _, excluded = ceredact.redact_samples(payload)
        self.assertEqual(
            excluded,
            [{"path": "partial_unexpected_list", "reason": "sample redacted"}],
        )

    def test2(self) -> None:
        """
        Test that a nested redaction's path names where it happened, and a
        caller's own root path is honoured as the prefix.
        """
        payload = {"runs": [{"result": {"unexpected_list": [1, 2, 3]}}]}
        _, excluded = ceredact.redact_samples(payload, path="kwargs")
        self.assertEqual(
            excluded,
            [
                {
                    "path": "kwargs.runs[0].result.unexpected_list",
                    "reason": "sample redacted",
                }
            ],
        )

    def test3(self) -> None:
        """
        Test that more than one sample field produces more than one entry.
        """
        payload = {
            "unexpected_list": [1],
            "unexpected_index_list": [0],
        }
        _, excluded = ceredact.redact_samples(payload)
        self.assertEqual(len(excluded), 2)
        self.assertEqual(
            {entry["path"] for entry in excluded},
            {"unexpected_list", "unexpected_index_list"},
        )


# #############################################################################
# Test_redact_extra_keys1
# #############################################################################


class Test_redact_extra_keys1(unittest.TestCase):
    """
    Test that a caller can redact its own field names too, without widening
    what every other caller redacts.
    """

    def test1(self) -> None:
        """
        Test that a name passed as `extra_keys` is redacted like a sample.
        """
        payload = {"metadata": {"note": "customer email: alice@x.com"}}
        out, excluded = ceredact.redact_samples(
            payload, extra_keys=frozenset({"metadata"})
        )
        self.assertEqual(out["metadata"], {"redacted": True, "count": 1})
        self.assertEqual(
            excluded, [{"path": "metadata", "reason": "sample redacted"}]
        )

    def test2(self) -> None:
        """
        Test that a name not passed as `extra_keys` is untouched -- the
        default `SAMPLE_KEYS` set is not widened for every caller.
        """
        payload = {"metadata": {"row_count": 100}}
        out, excluded = ceredact.redact_samples(payload)
        self.assertEqual(out, payload)
        self.assertEqual(excluded, [])

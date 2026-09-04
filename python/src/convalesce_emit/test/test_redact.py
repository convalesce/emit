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
        out = ceredact.redact_samples(payload)
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
        out = ceredact.redact_samples(payload)
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
        self.assertEqual(ceredact.redact_samples(payload), payload)

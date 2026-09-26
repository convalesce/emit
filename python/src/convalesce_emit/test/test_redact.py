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


# #############################################################################
# Test_redact_secrets1
# #############################################################################


class Test_redact_secrets1(unittest.TestCase):
    """
    Test that credentials never survive secret redaction while the text
    around them does.
    """

    def test1(self) -> None:
        """
        Test that values under credential-named keys are replaced, in any
        casing or separator style, and declared.
        """
        payload = {
            "conn": {
                "password": "hunter2",
                "clientSecret": "s3",
                "API-KEY": "k",
                "access_token": "t",
                "host": "db",
            }
        }
        out, excluded = ceredact.redact_secrets(payload, path="run_event")
        self.assertEqual(out["conn"]["host"], "db")
        for key in ("password", "clientSecret", "API-KEY", "access_token"):
            self.assertEqual(out["conn"][key], {"redacted": True})
        text = json.dumps(out)
        for secret in ("hunter2", '"s3"', '"k"', '"t"'):
            self.assertNotIn(secret, text)
        self.assertIn(
            {"path": "run_event.conn.password", "reason": "secret redacted"},
            excluded,
        )
        self.assertEqual(len(excluded), 4)

    def test2(self) -> None:
        """
        Test that a password inside a URI is masked and the user, host and
        path are kept.
        """
        payload = {
            "facets": [{"uri": "postgresql://etl:pa55@db.internal:5432/orders"}]
        }
        out, excluded = ceredact.redact_secrets(payload)
        self.assertEqual(
            out["facets"][0]["uri"],
            "postgresql://etl:***@db.internal:5432/orders",
        )
        self.assertEqual(
            excluded,
            [{"path": "facets[0].uri", "reason": "credential masked"}],
        )

    def test3(self) -> None:
        """
        Test that `password=` in a connection string is masked in place.
        """
        text = "jdbc:sqlserver://h;user=etl;password=pa55;db=x"
        out, _ = ceredact.redact_secrets({"url": text})
        self.assertEqual(
            out["url"], "jdbc:sqlserver://h;user=etl;password=***;db=x"
        )

    def test4(self) -> None:
        """
        Test that SQL and names that only resemble a credential cross
        untouched, and an empty credential is left as it was.
        """
        payload = {
            "sql": {"query": "SELECT id FROM orders WHERE total > 5"},
            "tokenizer": "bpe",
            "uri": "postgres://db:5432/orders",
            "password": None,
        }
        out, excluded = ceredact.redact_secrets(payload)
        self.assertEqual(out, payload)
        self.assertEqual(excluded, [])

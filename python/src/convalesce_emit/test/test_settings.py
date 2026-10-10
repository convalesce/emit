"""
Tests for sending a run's settings, with secrets as keyed hashes.

Run with `make test`.
"""

import json
import logging
import pathlib
import unittest

import convalesce_emit.settings as cesettin

_LOG = logging.getLogger(__name__)

_ON = {"CONVALESCE_SEND_SETTINGS": "true"}
_VECTORS = (
    pathlib.Path(__file__).resolve().parents[4]
    / "testdata"
    / "settings_vectors.json"
)


# #############################################################################
# Test_enabled1
# #############################################################################


class Test_enabled1(unittest.TestCase):
    """
    Test that settings are sent only when asked for.
    """

    def test1(self) -> None:
        """
        Test that nothing is sent when the setting is absent.
        """
        self.assertFalse(cesettin.enabled({}))
        out, excluded = cesettin.collect(
            "ingest", {"environment": {"A": "1"}}, environ={}
        )
        self.assertEqual(out, {})
        self.assertEqual(excluded, [])

    def test2(self) -> None:
        """
        Test that the setting is read under the platform's prefix too.
        """
        self.assertTrue(
            cesettin.enabled({"CUSTOMER_CONVALESCE_SEND_SETTINGS": "on"})
        )

    def test3(self) -> None:
        """
        Test that a value that is not a yes leaves it off.
        """
        self.assertFalse(cesettin.enabled({"CONVALESCE_SEND_SETTINGS": "0"}))


# #############################################################################
# Test_of1
# #############################################################################


class Test_of1(unittest.TestCase):
    """
    Test that a hash tells two values apart and gives neither away.
    """

    def test1(self) -> None:
        """
        Test that the same three always give the same sixteen hex characters.
        """
        key = cesettin.derive("ingest")
        one = cesettin.of(key, "environment", "A", "value")
        self.assertEqual(one, cesettin.of(key, "environment", "A", "value"))
        self.assertRegex(one, r"^[0-9a-f]{16}$")

    def test2(self) -> None:
        """
        Test that the kind, the name, the value and the key each matter.
        """
        key = cesettin.derive("ingest")
        base = cesettin.of(key, "environment", "A", "value")
        self.assertNotEqual(base, cesettin.of(key, "variable", "A", "value"))
        self.assertNotEqual(base, cesettin.of(key, "environment", "B", "value"))
        self.assertNotEqual(base, cesettin.of(key, "environment", "A", "other"))
        other = cesettin.derive("another")
        self.assertNotEqual(
            base, cesettin.of(other, "environment", "A", "value")
        )

    def test3(self) -> None:
        """
        Test that a name and a value cannot be re-cut into another pair.
        """
        key = cesettin.derive("ingest")
        self.assertNotEqual(
            cesettin.of(key, "environment", "AB", "C"),
            cesettin.of(key, "environment", "A", "BC"),
        )

    def test4(self) -> None:
        """
        Test that the shared vectors hold, as the Java library's must.
        """
        vectors = json.loads(_VECTORS.read_text(encoding="utf-8"))
        for row in vectors["hashes"]:
            key = cesettin.derive(row["secret"])
            self.assertEqual(
                cesettin.of(key, row["kind"], row["name"], row["value"]),
                row["fingerprint"],
            )
        for row in vectors["secret"]:
            self.assertEqual(
                cesettin.is_secret(row["name"], row["value"]),
                row["is_secret"],
                row["name"],
            )


# #############################################################################
# Test_is_secret1
# #############################################################################


class Test_is_secret1(unittest.TestCase):
    """
    Test which settings cross as a hash.
    """

    def test1(self) -> None:
        """
        Test that an ordinary name with an ordinary value crosses plainly.
        """
        for name, value in (
            ("ENVIRONMENT", "production"),
            ("AIRFLOW__CORE__EXECUTOR", "CeleryExecutor"),
            ("BATCH_SIZE", "500"),
            ("TZ", "Europe/London"),
            ("AIRFLOW_HOME", "/opt/airflow"),
        ):
            self.assertFalse(cesettin.is_secret(name, value), name)

    def test2(self) -> None:
        """
        Test that a name that reads as a credential is hashed.
        """
        for name in (
            "DB_PASSWORD",
            "API_KEY",
            "apiKey",
            "GITHUB_TOKEN",
            "AWS_SECRET_ACCESS_KEY",
            "SESSION_COOKIE",
            "AZURE_SAS",
            "AUTH_HEADER",
        ):
            self.assertTrue(cesettin.is_secret(name, "x"), name)

    def test3(self) -> None:
        """
        Test that a value that reads as a credential is hashed under any name.
        """
        for value in (
            "postgresql://user:hunter2@db:5432/app",
            "Server=db;Password=hunter2",
            "-----BEGIN PRIVATE KEY-----\nabc",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig",
            "Zq9aF3kQ7zL2mX8vB4nC6tR1yU0p",
            "x" * 301,
        ):
            self.assertTrue(cesettin.is_secret("SOMETHING", value), value[:20])


# #############################################################################
# Test_collect1
# #############################################################################


class Test_collect1(unittest.TestCase):
    """
    Test what is sent for a run's settings.
    """

    def test1(self) -> None:
        """
        Test that ordinary settings carry values and secrets carry hashes.
        """
        out, excluded = cesettin.collect(
            "ingest",
            {"environment": {"STAGE": "prod", "DB_PASSWORD": "hunter2"}},
            environ=_ON,
        )
        self.assertEqual(excluded, [])
        by_name = {item["name"]: item for item in out["items"]}
        self.assertEqual(by_name["STAGE"]["value"], "prod")
        self.assertNotIn("value", by_name["DB_PASSWORD"])
        self.assertRegex(by_name["DB_PASSWORD"]["fingerprint"], "^[0-9a-f]{16}$")
        self.assertRegex(out["keyed_by"], "^[0-9a-f]{16}$")
        self.assertNotIn("hunter2", json.dumps(out))
        self.assertNotIn("ingest", json.dumps(out))

    def test2(self) -> None:
        """
        Test that the operator's own key is used before the ingest key.
        """
        kinds = {"environment": {"DB_PASSWORD": "hunter2"}}
        mine = dict(_ON, CONVALESCE_FINGERPRINT_KEY="mine")
        with_mine, _ = cesettin.collect("ingest", kinds, environ=mine)
        with_ingest, _ = cesettin.collect("ingest", kinds, environ=_ON)
        other_ingest, _ = cesettin.collect("rotated", kinds, environ=mine)
        self.assertNotEqual(with_mine["keyed_by"], with_ingest["keyed_by"])
        self.assertEqual(with_mine, other_ingest)

    def test3(self) -> None:
        """
        Test that with no key a secret is left out and said to be.
        """
        out, excluded = cesettin.collect(
            None,
            {"environment": {"STAGE": "prod", "DB_PASSWORD": "hunter2"}},
            environ=_ON,
        )
        self.assertEqual(
            out,
            {
                "items": [
                    {"kind": "environment", "name": "STAGE", "value": "prod"}
                ]
            },
        )
        self.assertEqual(
            excluded,
            [
                {
                    "path": "settings",
                    "reason": "no key to fingerprint 1 settings with",
                }
            ],
        )

    def test4(self) -> None:
        """
        Test that this package's own keys and skipped names are never sent.
        """
        environ = dict(_ON, CONVALESCE_SETTINGS_SKIP="NOISY, OTHER")
        out, _ = cesettin.collect(
            "ingest",
            {
                "environment": {
                    "CONVALESCE_INGEST_KEY": "ingest",
                    "CUSTOMER_CONVALESCE_INGEST_KEY": "ingest",
                    "CONVALESCE_FINGERPRINT_KEY": "mine",
                    "NOISY": "1",
                    "KEPT": "1",
                }
            },
            environ=environ,
        )
        self.assertEqual([item["name"] for item in out["items"]], ["KEPT"])

    def test5(self) -> None:
        """
        Test that one kind is cut at the limit and said to be.
        """
        many = {f"N{i:04d}": "v" for i in range(cesettin.MAX_ITEMS + 5)}
        out, excluded = cesettin.collect(
            "ingest", {"environment": many}, environ=_ON
        )
        self.assertEqual(len(out["items"]), cesettin.MAX_ITEMS)
        self.assertEqual(
            excluded,
            [{"path": "settings.environment", "reason": "limited to 500 names"}],
        )

    def test6(self) -> None:
        """
        Test that settings come out in one order whatever order went in.
        """
        one, _ = cesettin.collect(
            "ingest",
            {"variable": {"b": "1", "a": "2"}, "environment": {"Z": "1"}},
            environ=_ON,
        )
        self.assertEqual(
            [(i["kind"], i["name"]) for i in one["items"]],
            [("environment", "Z"), ("variable", "a"), ("variable", "b")],
        )


# #############################################################################
# Test_note1
# #############################################################################


class Test_note1(unittest.TestCase):
    """
    Test remembering what a run read for itself.
    """

    def setUp(self) -> None:
        cesettin.noted()

    def test1(self) -> None:
        """
        Test that nothing is remembered while settings are not sent.
        """
        cesettin.note("variable", "a", "1")
        self.assertEqual(cesettin.noted(), {})

    def test2(self) -> None:
        """
        Test that a read is remembered once and forgotten when asked for.
        """
        import os
        from unittest import mock

        with mock.patch.dict(os.environ, _ON):
            cesettin.note("variable", "a", "1")
            cesettin.note("variable", "a", "2")
            cesettin.note("variable", "missing", None)
            cesettin.note("variable", "parsed", {"k": 1})
        self.assertEqual(
            cesettin.noted(), {"variable": {"a": "2", "parsed": "{'k': 1}"}}
        )
        self.assertEqual(cesettin.noted(), {})

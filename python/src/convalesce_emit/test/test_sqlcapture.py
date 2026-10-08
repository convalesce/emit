"""
Tests for noting the SQL a step runs.

Import as:

import convalesce_emit.test.test_sqlcapture as cetestsqlc
"""

# pylint: disable=protected-access

import os
import sqlite3
import unittest
import unittest.mock
from typing import Any, Dict, List

import convalesce_emit.sqlcapture as cesqlcap


class _Case(unittest.TestCase):
    """Starts each test with nothing noted and nothing ignored."""

    def setUp(self) -> None:
        ignored: List[Any] = []
        patch = unittest.mock.patch.object(cesqlcap, "_ignored", ignored)
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(cesqlcap.drain)
        cesqlcap.start()

    @staticmethod
    def _texts() -> List[str]:
        taken = cesqlcap.drain()
        return [row["statement"] for row in (taken or {}).get("statements", [])]


class Test_record1(_Case):
    """
    Test which statements are kept.
    """

    def test1(self) -> None:
        """Test that a statement that moves data is kept, with its facts."""
        cesqlcap.record(
            "insert into a.t select * from b.s",
            dialect="postgres",
            database="shop",
            via="test",
        )
        taken = cesqlcap.drain()
        assert taken is not None
        self.assertEqual(
            taken["statements"],
            [
                {
                    "statement": "insert into a.t select * from b.s",
                    "dialect": "postgres",
                    "database": "shop",
                    "schema": None,
                    "via": "test",
                    "count": 1,
                }
            ],
        )

    def test2(self) -> None:
        """Test that session chatter and catalogue lookups are left out."""
        for text in (
            "SET search_path TO x",
            "BEGIN",
            "select 1",
            "SELECT version()",
            "select relname from pg_catalog.pg_class",
            "SELECT name FROM sqlite_master",
            "",
        ):
            cesqlcap.record(text, via="test")
        self.assertEqual(self._texts(), [])

    def test3(self) -> None:
        """
        Test that one statement seen by two hooks is kept once, and one
        run many times through one hook is counted.
        """
        for via in ("sqlalchemy", "psycopg2", "psycopg2"):
            cesqlcap.record("delete from a.t", via=via, dialect=None)
        taken = cesqlcap.drain()
        assert taken is not None
        self.assertEqual(len(taken["statements"]), 1)
        self.assertEqual(taken["statements"][0]["via"], "sqlalchemy")

    def test4(self) -> None:
        """Test that only so many are kept, and the rest are counted."""
        for i in range(cesqlcap.MAX_STATEMENTS + 5):
            cesqlcap.record(f"insert into t{i} select 1", via="test")
        taken = cesqlcap.drain()
        assert taken is not None
        self.assertEqual(len(taken["statements"]), cesqlcap.MAX_STATEMENTS)
        self.assertEqual(taken["dropped"], 5)

    def test5(self) -> None:
        """Test that a long statement is cut, keeping its head."""
        cesqlcap.record("select " + "x, " * cesqlcap.MAX_CHARS, via="test")
        self.assertEqual(len(self._texts()[0]), cesqlcap.MAX_CHARS)

    def test6(self) -> None:
        """Test that nothing is kept before `start` or after `drain`."""
        cesqlcap.drain()
        cesqlcap.record("delete from a.t", via="test")
        self.assertIsNone(cesqlcap.drain())

    def test7(self) -> None:
        """Test that a tool's own database can be told apart and left out."""
        cesqlcap.ignore(lambda row: "metadata" in (row.get("url") or ""))
        cesqlcap.record(
            "delete from a.t", url="sqlite:///metadata.db", via="test"
        )
        cesqlcap.record(
            "delete from b.t", url="postgres://u:***@h/shop", via="test"
        )
        self.assertEqual(self._texts(), ["delete from b.t"])

    def test8(self) -> None:
        """Test that something that cannot be read as text is not an error."""

        class Broken:
            """Raises when asked for its text."""

            def __str__(self) -> str:
                raise RuntimeError("no")

        cesqlcap.record(Broken(), via="test")
        self.assertIsNone(cesqlcap.drain())

    def test9(self) -> None:
        """Test that the setting turns it off."""
        with unittest.mock.patch.dict(
            os.environ, {"CONVALESCE_SQL_CAPTURE": "false"}
        ):
            cesqlcap.start()
            cesqlcap.record("delete from a.t", via="test")
            self.assertIsNone(cesqlcap.drain())
            self.assertEqual(cesqlcap.install(), [])


class Test_where1(_Case):
    """
    Test asking a connection which database it is on.
    """

    def test1(self) -> None:
        """
        Test that a DuckDB-style connection is asked once, that each
        statement carries the answer, and that one that cannot be asked is
        left alone.
        """
        asked: List[str] = []

        class Connection:
            """A stand-in for a connection that answers where it is."""

            def __init__(self, answers: bool) -> None:
                self.answers = answers

            def execute(self, operation: Any, params: Any = None) -> Any:
                """Run a statement."""
                del params
                asked.append(operation)
                if "current_database" in operation and not self.answers:
                    raise RuntimeError("closed")
                return self

            def fetchone(self) -> Any:
                """The row the last statement gave."""
                return ("probe", "main")

        cesqlcap._ANSWERED.clear()
        self.assertTrue(
            cesqlcap._wrap_method(Connection, "execute", "duckdb", "duckdb")
        )
        connection = Connection(answers=True)
        connection.execute("insert into shop.orders select 1")
        connection.execute("insert into shop.orders select 2")
        self.assertEqual(
            [text for text in asked if "current_database" in text],
            [cesqlcap._ASKED["duckdb"]],
        )
        silent = Connection(answers=False)
        silent.execute("insert into shop.refunds select 3")
        silent.execute("insert into shop.refunds select 4")
        self.assertEqual(len([t for t in asked if "current_database" in t]), 2)
        taken = cesqlcap.drain()
        assert taken is not None
        where = {
            one["statement"]: (one["database"], one["schema"])
            for one in taken["statements"]
        }
        self.assertEqual(
            where["insert into shop.orders select 1"], ("probe", "main")
        )
        self.assertEqual(
            where["insert into shop.refunds select 3"], (None, None)
        )


class Test_wrap_method1(_Case):
    """
    Test wrapping a driver's `execute`.
    """

    def test1(self) -> None:
        """
        Test that the wrapped method notes the statement and still does
        what it did, once however often it is wrapped.
        """
        calls: List[Any] = []

        class Cursor:
            """A stand-in for a Python-level driver cursor."""

            def execute(self, operation: Any, params: Any = None) -> str:
                """Run a statement."""
                calls.append((operation, params))
                return "done"

        self.assertTrue(cesqlcap._wrap_method(Cursor, "execute", "mysql", "drv"))
        self.assertFalse(
            cesqlcap._wrap_method(Cursor, "execute", "mysql", "drv")
        )
        self.assertEqual(Cursor().execute("update a.t set x = %s", (1,)), "done")
        self.assertEqual(calls, [("update a.t set x = %s", (1,))])
        taken = cesqlcap.drain()
        assert taken is not None
        row: Dict[str, Any] = taken["statements"][0]
        self.assertEqual(
            (row["statement"], row["dialect"]),
            ("update a.t set x = %s", "mysql"),
        )
        # The value bound to it is never kept.
        self.assertNotIn("1,", str(row))

    def test2(self) -> None:
        """Test that a class written in C, which takes no attributes, is left alone."""
        self.assertFalse(
            cesqlcap._wrap_method(sqlite3.Cursor, "execute", "sqlite", "sqlite3")
        )


class Test_scope_by1(_Case):
    """
    Test keeping statements apart by the step that ran them.
    """

    def setUp(self) -> None:
        super().setUp()
        self.step: List[Any] = [None]
        cesqlcap.scope_by(lambda: self.step[0])
        self.addCleanup(cesqlcap.scope_by, None)

    def test1(self) -> None:
        """
        Test that each step is handed what it ran and nothing else, with
        no bracket around it, and only once.
        """
        cesqlcap.drain()
        for step, table in (("a", "t1"), ("b", "t2"), ("a", "t3")):
            self.step[0] = step
            cesqlcap.record(f"delete from {table}", dialect="pg", via="test")
        taken = cesqlcap.drain(scope="a")
        assert taken is not None
        self.assertEqual(
            [row["statement"] for row in taken["statements"]],
            ["delete from t1", "delete from t3"],
        )
        self.assertIsNone(cesqlcap.drain(scope="a"))
        other = cesqlcap.drain(scope="b")
        assert other is not None
        self.assertEqual(len(other["statements"]), 1)

    def test2(self) -> None:
        """
        Test that a statement outside any step is kept only inside the
        bracket, as without a resolver, and taking a step's leaves it be.
        """
        cesqlcap.record("delete from in_bracket", via="test")
        self.step[0] = "a"
        cesqlcap.record("delete from in_step", via="test")
        taken = cesqlcap.drain(scope="a")
        assert taken is not None
        self.assertEqual(len(taken["statements"]), 1)
        self.step[0] = None
        self.assertEqual(self._texts(), ["delete from in_bracket"])
        cesqlcap.record("delete from outside", via="test")
        self.assertIsNone(cesqlcap.drain())

    def test3(self) -> None:
        """
        Test that each step has its own limit and its own count of the
        rest, and that only so many steps are held.
        """
        self.step[0] = "a"
        for i in range(cesqlcap.MAX_STATEMENTS + 3):
            cesqlcap.record(f"insert into t{i} select 1", via="test")
        self.step[0] = "b"
        cesqlcap.record("delete from t", via="test")
        taken = cesqlcap.drain(scope="a")
        assert taken is not None
        self.assertEqual(len(taken["statements"]), cesqlcap.MAX_STATEMENTS)
        self.assertEqual(taken["dropped"], 3)
        other = cesqlcap.drain(scope="b")
        assert other is not None
        self.assertEqual(other["dropped"], 0)
        for i in range(cesqlcap.MAX_SCOPES + 1):
            self.step[0] = f"s{i}"
            cesqlcap.record("delete from t", via="test")
        self.assertIsNone(cesqlcap.drain(scope="s0"))
        self.assertIsNotNone(cesqlcap.drain(scope="s1"))

    def test4(self) -> None:
        """
        Test that a resolver that raises is a statement outside any step,
        not an error, and that the setting turns scoped noting off too.
        """

        def broken() -> str:
            raise RuntimeError("no context")

        cesqlcap.scope_by(broken)
        cesqlcap.record("delete from t", via="test")
        self.assertEqual(self._texts(), ["delete from t"])
        self.step[0] = "a"
        cesqlcap.scope_by(lambda: self.step[0])
        with unittest.mock.patch.dict(
            os.environ, {"CONVALESCE_SQL_CAPTURE": "false"}
        ):
            cesqlcap.record("delete from t", via="test")
        self.assertIsNone(cesqlcap.drain(scope="a"))

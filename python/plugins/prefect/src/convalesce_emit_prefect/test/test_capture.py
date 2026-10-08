"""
Tests for what a flow or task ran: source, arguments and SQL.

Run with `make test`.
"""

import concurrent.futures
import contextvars
import os
import sys
import threading
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest import mock

import convalesce_emit as cemit
import convalesce_emit.sqlcapture as cesqlcap
import convalesce_emit_prefect._capture as cecap

# Restated rather than read off the module under test, so these stay tests
# of the documented switches' names.
_SEND_SOURCE_ENV = "CONVALESCE_SEND_SOURCE"
_SEND_ARGUMENTS_ENV = "CONVALESCE_SEND_ARGUMENTS"
_SQL_CAPTURE_ENV = "CONVALESCE_SQL_CAPTURE"

_TASK_RUN: "contextvars.ContextVar[Any]" = contextvars.ContextVar(
    "task_run", default=None
)
_FLOW_RUN: "contextvars.ContextVar[Any]" = contextvars.ContextVar(
    "flow_run", default=None
)


class _Run:
    """Stands in for a task run or a flow run: all that is read is its id."""

    # pylint: disable-next=redefined-builtin
    def __init__(self, id: str) -> None:
        self.id = id


class _State:
    """Stands in for a Prefect state that is, or is not, still running."""

    def __init__(self, running: bool) -> None:
        self._running = running

    def is_running(self) -> bool:
        """Whether the run is still going."""
        return self._running


def _context_module() -> Any:
    """
    A stand-in `prefect.context` backed by real context variables, which is
    what makes Prefect's own follow the code into a thread.

    :return: a module-like object with both run contexts on it
    """

    class _TaskRunContext:
        """Stands in for Prefect's task run context."""

        @staticmethod
        def get() -> Any:
            """The running task's context, or None outside one."""
            return _TASK_RUN.get()

    class _FlowRunContext:
        """Stands in for Prefect's flow run context."""

        @staticmethod
        def get() -> Any:
            """The running flow's context, or None outside one."""
            return _FLOW_RUN.get()

    return types.SimpleNamespace(
        TaskRunContext=_TaskRunContext, FlowRunContext=_FlowRunContext
    )


def _task_context(run_id: str, **parameters: Any) -> Any:
    """
    A live task run context.

    :param run_id: the task run's id
    :param parameters: what the task was called with
    :return: the context
    """
    return types.SimpleNamespace(task_run=_Run(run_id), parameters=parameters)


class _Case(unittest.TestCase):
    """Runs each test under a stand-in Prefect, with nothing noted."""

    def setUp(self) -> None:
        modules = {"prefect.context": _context_module()}
        patch = mock.patch.dict(sys.modules, modules)
        patch.start()
        self.addCleanup(patch.stop)
        ignored: List[Any] = []
        patch_ignored = mock.patch.object(cesqlcap, "_ignored", ignored)
        patch_ignored.start()
        self.addCleanup(patch_ignored.stop)
        cesqlcap.scope_by(cecap.running_scope)
        self.addCleanup(cesqlcap.scope_by, None)


# #############################################################################
# Test_source_of1
# #############################################################################


def _extract(since: str) -> str:
    """A task body whose text is looked for below."""
    return since


class Test_source_of1(unittest.TestCase):
    """
    Test reading the source of the function a flow or task runs.
    """

    def test1(self) -> None:
        """
        Test that the function Prefect keeps as `fn` is read, with a hash
        of it and of its file.
        """
        source = cecap.source_of(types.SimpleNamespace(fn=_extract))
        assert source is not None
        self.assertIn("def _extract(since: str) -> str:", source["text"])
        self.assertEqual(source["language"], "python")
        self.assertEqual(len(source["sha256"]), 64)
        self.assertEqual(len(source["file_sha256"]), 64)

    def test2(self) -> None:
        """
        Test that the switch keeps the text behind.
        """
        with mock.patch.dict(os.environ, {_SEND_SOURCE_ENV: "false"}):
            self.assertFalse(cecap.send_source())
            self.assertIsNone(
                cecap.source_of(types.SimpleNamespace(fn=_extract))
            )

    def test3(self) -> None:
        """
        Test that an object keeping no function has no source, and is not
        an error.
        """
        self.assertIsNone(cecap.source_of("T"))
        self.assertIsNone(cecap.source_of(None))
        self.assertIsNone(cecap.source_of(types.SimpleNamespace(fn=len)))


# #############################################################################
# Test_arguments_of1
# #############################################################################


class Test_arguments_of1(_Case):
    """
    Test reading what the running task was called with.
    """

    def test1(self) -> None:
        """
        Test that the values come off the task run's own context.
        """
        token = _TASK_RUN.set(_task_context("t-1", table="orders", minimum=5))
        self.addCleanup(_TASK_RUN.reset, token)
        self.assertEqual(
            cecap.arguments_of(_Run("t-1")), {"table": "orders", "minimum": 5}
        )

    def test2(self) -> None:
        """
        Test that another task run's context is never read as this one's.
        """
        token = _TASK_RUN.set(_task_context("t-2", table="payments"))
        self.addCleanup(_TASK_RUN.reset, token)
        self.assertIsNone(cecap.arguments_of(_Run("t-1")))

    def test3(self) -> None:
        """
        Test that the switch sends nothing.
        """
        token = _TASK_RUN.set(_task_context("t-1", table="orders"))
        self.addCleanup(_TASK_RUN.reset, token)
        with mock.patch.dict(os.environ, {_SEND_ARGUMENTS_ENV: "off"}):
            self.assertFalse(cecap.send_arguments())
            self.assertIsNone(cecap.arguments_of(_Run("t-1")))

    def test4(self) -> None:
        """
        Test that outside a task run, and without Prefect, there is nothing.
        """
        self.assertIsNone(cecap.arguments_of(_Run("t-1")))
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            self.assertIsNone(cecap.arguments_of(_Run("t-1")))


# #############################################################################
# Test_shape1
# #############################################################################


class _Frame:
    """Stands in for a data frame: it has a shape, and rows not to read."""

    shape = (3, 2)

    def to_dict(self) -> Dict[str, Any]:
        """Every row, which must never be asked for."""
        raise AssertionError("the rows were read")

    def __str__(self) -> str:
        raise AssertionError("the rows were printed")


class Test_shape1(unittest.TestCase):
    """
    Test that arguments cross small, and every cut is declared.
    """

    def test1(self) -> None:
        """
        Test that plain values cross as they are.
        """
        values = {"table": "orders", "minimum": 5, "dry_run": False}
        self.assertEqual(cecap.shape(values), (values, []))

    def test2(self) -> None:
        """
        Test that a long list keeps its head, and the cut says how long it
        was.
        """
        shaped, excluded = cecap.shape({"rows": list(range(5000))})
        self.assertEqual(shaped["rows"], list(range(cecap.MAX_ITEMS)))
        self.assertEqual(
            excluded,
            [
                {
                    "path": "arguments.rows",
                    "reason": "argument cut: first 50 of 5000",
                }
            ],
        )

    def test3(self) -> None:
        """
        Test that long text is cut and bytes are named, not sent.
        """
        shaped, excluded = cecap.shape(
            {"query": "x" * (cecap.MAX_CHARS + 10), "blob": b"\x00" * 9}
        )
        self.assertEqual(len(shaped["query"]), cecap.MAX_CHARS)
        self.assertEqual(shaped["blob"], "<bytes: 9 bytes>")
        self.assertEqual(
            [entry["path"] for entry in excluded],
            ["arguments.query", "arguments.blob"],
        )

    def test4(self) -> None:
        """
        Test that a frame is named by the core serialiser and never read.
        """
        shaped, excluded = cecap.shape({"frame": _Frame(), "day": "monday"})
        self.assertEqual(shaped, {"frame": "<_Frame [3, 2]>", "day": "monday"})
        self.assertEqual(
            excluded, [{"path": "arguments.frame", "reason": "bulk data"}]
        )

    def test5(self) -> None:
        """
        Test that a value nested without end stops at a depth, and one
        still large after every cut is named instead.
        """
        loop: List[Any] = []
        loop.append(loop)
        wide = {
            str(row): ["y" * cecap.MAX_CHARS] * cecap.MAX_ITEMS
            for row in range(cecap.MAX_ITEMS)
        }
        shaped, excluded = cecap.shape({"loop": loop, "wide": wide})
        self.assertEqual(shaped["wide"], "<dict>")
        self.assertIn(
            {"path": "arguments.wide", "reason": "argument too large"}, excluded
        )
        # The arguments themselves are the first level.
        nested = shaped["loop"]
        for _ in range(cecap.MAX_DEPTH - 1):
            nested = nested[0]
        self.assertEqual(nested, "<list>")

    def test6(self) -> None:
        """
        Test that an object is walked only so far.
        """

        class _Node:
            """One link of a long chain of objects."""

            def __init__(self, depth: int) -> None:
                self.depth = depth
                self.children = [
                    _Node(depth - 1) for _ in range(3 if depth else 0)
                ]

        with mock.patch.object(cecap, "_ARGUMENT_NODES", 20):
            _, excluded = cecap.shape({"tree": _Node(6)})
        self.assertIn("node backstop", [entry["reason"] for entry in excluded])


# #############################################################################
# Test_shape2
# #############################################################################


class ResultRecord:
    """Stands in for Prefect 3's `ResultRecord`: the value is in memory."""

    __module__ = "prefect._internal.result_records"

    def __init__(self, result: Any) -> None:
        self.result = result


class ResultRecordMetadata:
    """Stands in for where Prefect 3 stored a result it no longer holds."""

    __module__ = "prefect.results"

    def __init__(self) -> None:
        self.storage_key = "s3://bucket/key"

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"read {name} from storage")


class _FutureState:
    """Stands in for the state a task run ended in."""

    def __init__(self, completed: bool, data: Any) -> None:
        self._completed = completed
        self.data = data

    def is_completed(self) -> bool:
        """Whether the task completed."""
        return self._completed

    def result(self) -> Any:
        """What would load the value, which must never be asked for."""
        raise AssertionError("the result was loaded")


class PrefectFuture:
    """Stands in for Prefect 3's future of a task run."""

    __module__ = "prefect.futures"

    def __init__(
        self,
        task_run_id: Optional[str],
        final_state: Any = None,
        wrapped: Any = None,
    ) -> None:
        self._task_run_id = task_run_id
        self._final_state = final_state
        self._wrapped_future = wrapped

    def result(self) -> Any:
        """What would wait for the task, which must never be asked for."""
        raise AssertionError("the future was waited on")

    def wait(self) -> None:
        """As `result`."""
        raise AssertionError("the future was waited on")


class PrefectFutureList(List[Any]):
    """Stands in for the list `task.map()` returns on Prefect 3."""

    __module__ = "prefect.futures"


def _done(value: Any) -> "concurrent.futures.Future[Any]":
    """
    A concurrent future that finished with a value.

    :param value: what it finished with
    :return: the future
    """
    future: "concurrent.futures.Future[Any]" = concurrent.futures.Future()
    future.set_result(value)
    return future


class Test_shape2(unittest.TestCase):
    """
    Test that a future among the arguments crosses as the value its task
    returned, and never as where it lives in memory.
    """

    def test1(self) -> None:
        """
        Test that the futures of a mapped task, as Prefect 3 hands them to
        the task after it, are the values the mapped task returned: from
        the future's final state, or from the future it wraps.
        """
        values = PrefectFutureList(
            [
                PrefectFuture("t-1", _FutureState(True, ResultRecord(1))),
                PrefectFuture(
                    "t-2", wrapped=_done(_FutureState(True, ResultRecord(4)))
                ),
                PrefectFuture("t-3", _FutureState(True, 9)),
            ]
        )
        shaped, excluded = cecap.shape({"values": values})
        self.assertEqual(shaped, {"values": [1, 4, 9]})
        self.assertEqual(excluded, [])

    def test2(self) -> None:
        """
        Test that a future that has not finished, failed, was cancelled,
        or whose value is in storage is a reference to its task run, and
        is never waited on or loaded.
        """
        running: "concurrent.futures.Future[Any]" = concurrent.futures.Future()
        cancelled: "concurrent.futures.Future[Any]" = concurrent.futures.Future()
        cancelled.cancel()
        raised: "concurrent.futures.Future[Any]" = concurrent.futures.Future()
        raised.set_exception(ValueError("bad row"))
        futures = [
            PrefectFuture("t-1"),
            PrefectFuture("t-2", wrapped=running),
            PrefectFuture("t-3", wrapped=cancelled),
            PrefectFuture("t-4", wrapped=raised),
            PrefectFuture(
                "t-5", _FutureState(False, ResultRecord(ValueError()))
            ),
            PrefectFuture("t-6", _FutureState(True, ResultRecordMetadata())),
            PrefectFuture("t-7", _FutureState(True, None)),
            PrefectFuture(
                "t-8", _FutureState(True, ResultRecord.__new__(ResultRecord))
            ),
        ]
        shaped, _ = cecap.shape({"values": futures, "one": futures[0]})
        self.assertEqual(
            shaped["values"],
            [{"task_run_id": f"t-{n}"} for n in range(1, 9)],
        )
        self.assertNotIn("s3://bucket/key", str(shaped))
        self.assertEqual(shaped["one"], {"task_run_id": "t-1"})

    def test3(self) -> None:
        """
        Test that the value a future stands for is cut and masked as any
        other argument is.
        """
        rows = PrefectFuture(
            "t-1", _FutureState(True, ResultRecord(list(range(5000))))
        )
        login = PrefectFuture(
            "t-2", _FutureState(True, ResultRecord({"password": "hunter2"}))
        )
        shaped, excluded = cecap.shape({"rows": rows, "login": login})
        self.assertEqual(shaped["rows"], list(range(cecap.MAX_ITEMS)))
        self.assertEqual(
            excluded[0],
            {
                "path": "arguments.rows",
                "reason": "argument cut: first 50 of 5000",
            },
        )
        # A plain mapping by now, which is what the payload's masking reads.
        masked, secrets = cemit.redact_secrets({"arguments": shaped})
        self.assertNotIn("hunter2", str(masked))
        self.assertEqual(secrets[0]["path"], "arguments.login.password")

    def test4(self) -> None:
        """
        Test that a concurrent future passed bare is what it finished
        with, and its type's name when it has not finished.
        """
        running: "concurrent.futures.Future[Any]" = concurrent.futures.Future()
        shaped, _ = cecap.shape(
            {
                "done": _done([1, 2]),
                "state": _done(_FutureState(True, ResultRecord("a"))),
                "running": running,
            }
        )
        self.assertEqual(
            shaped, {"done": [1, 2], "state": "a", "running": "<Future>"}
        )

    def test5(self) -> None:
        """
        Test that a future naming no task run, and one that cannot be
        read, is its type's name.
        """

        class Broken(PrefectFuture):
            """A future whose state raises when asked anything."""

            __module__ = "prefect.futures"

        class Raises:
            """A state that cannot say how it ended."""

            def is_completed(self) -> bool:
                """Raise, as a state from a closed client might."""
                raise RuntimeError("closed")

        class Slotted(PrefectFuture):
            """A future that keeps nothing of its own."""

            __module__ = "prefect.futures"
            __slots__ = ()

            # pylint: disable-next=super-init-not-called
            def __init__(self) -> None:
                pass

            def __getattribute__(self, name: str) -> Any:
                raise RuntimeError(name)

        shaped, _ = cecap.shape(
            {
                "unnamed": PrefectFuture(None),
                "broken": Broken("t-1", Raises()),
            }
        )
        self.assertEqual(
            shaped, {"unnamed": "<PrefectFuture>", "broken": "<Broken>"}
        )
        self.assertEqual(cecap.bound(Slotted(), "arguments.x", []), "<Slotted>")

    def test6(self) -> None:
        """
        Test that no argument says where an object lives in memory: the
        text is cut to the type's name, wherever in the arguments it is.
        """

        class Client:
            """An object with the default repr and nothing to walk."""

            __slots__ = ()

        shaped, _ = cecap.shape(
            {
                "client": Client(),
                "nested": {"hooks": [len, Client()]},
                "text": "ran <Future at 0x7f3a state=finished returned State>",
                "hook": "<function flow.<locals>.<lambda> at 0x7f3a>",
                "method": "<bound method A.run of <a.A object at 0x7f3a>>",
                "plain": "at 0x10 nothing here",
            }
        )
        self.assertRegex(shaped["client"], r"^<\S*\.Client>$")
        self.assertRegex(shaped["nested"]["hooks"][1], r"^<\S*\.Client>$")
        self.assertEqual(shaped["text"], "ran <Future>")
        self.assertEqual(shaped["hook"], "<function>")
        self.assertEqual(shaped["method"], "<bound method A.run of <a.A>>")
        self.assertEqual(shaped["plain"], "at 0x10 nothing here")
        self.assertNotRegex(str(shaped), r" at 0x[0-9a-f]+[ >]")
        self.assertEqual(cecap.shape("<Lock at 0x1f>"), ("<Lock>", []))


# #############################################################################
# Test_running_scope1
# #############################################################################


class Test_running_scope1(_Case):
    """
    Test naming the run a statement belongs to.
    """

    def test1(self) -> None:
        """
        Test that inside a task it is the task run, inside a flow but
        outside any task the flow run, and outside both nothing.
        """
        self.assertIsNone(cecap.running_scope())
        flow_token = _FLOW_RUN.set(types.SimpleNamespace(flow_run=_Run("f-1")))
        self.addCleanup(_FLOW_RUN.reset, flow_token)
        self.assertEqual(cecap.running_scope(), "f-1")
        task_token = _TASK_RUN.set(_task_context("t-1"))
        self.addCleanup(_TASK_RUN.reset, task_token)
        self.assertEqual(cecap.running_scope(), "t-1")

    def test2(self) -> None:
        """
        Test that without Prefect loaded there is no run, and no error.
        """
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            self.assertIsNone(cecap.running_scope())


# #############################################################################
# Test_take_sql1
# #############################################################################


class Test_take_sql1(_Case):
    """
    Test that each run is handed the statements it sent, and only those.
    """

    def test1(self) -> None:
        """
        Test that two tasks running at the same moment, in two threads,
        each keep their own statements.
        """
        ready = threading.Barrier(2)

        def body(run_id: str, table: str) -> None:
            _TASK_RUN.set(_task_context(run_id))
            ready.wait(timeout=5)
            for step in ("insert into", "delete from"):
                cesqlcap.record(f"{step} {table}", dialect="postgres", via="t")
                ready.wait(timeout=5)

        threads = [
            threading.Thread(target=body, args=("t-1", "shop.orders")),
            threading.Thread(target=body, args=("t-2", "shop.payments")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        for run_id, table in (("t-1", "shop.orders"), ("t-2", "shop.payments")):
            taken = cecap.take_sql(_Run(run_id), _State(running=False))
            assert taken is not None
            self.assertEqual(
                [row["statement"] for row in taken["statements"]],
                [f"insert into {table}", f"delete from {table}"],
            )

    def test2(self) -> None:
        """
        Test that a run still running keeps its statements for the hook
        that ends it, and that they are handed over once.
        """
        token = _TASK_RUN.set(_task_context("t-1"))
        self.addCleanup(_TASK_RUN.reset, token)
        cesqlcap.record("delete from shop.orders", via="t")
        run = _Run("t-1")
        self.assertIsNone(cecap.take_sql(run, _State(running=True)))
        taken = cecap.take_sql(run, _State(running=False))
        assert taken is not None
        self.assertEqual(len(taken["statements"]), 1)
        self.assertIsNone(cecap.take_sql(run, _State(running=False)))

    def test3(self) -> None:
        """
        Test that a statement the flow itself sent, outside any task, is
        the flow run's, and one sent outside any run is nobody's.
        """
        cesqlcap.record("delete from shop.nobody", via="t")
        token = _FLOW_RUN.set(types.SimpleNamespace(flow_run=_Run("f-1")))
        self.addCleanup(_FLOW_RUN.reset, token)
        cesqlcap.record("truncate shop.staging", via="t")
        taken = cecap.take_sql(_Run("f-1"), "Completed")
        assert taken is not None
        self.assertEqual(
            [row["statement"] for row in taken["statements"]],
            ["truncate shop.staging"],
        )

    def test4(self) -> None:
        """
        Test that a run with no id, or one that sent nothing, has nothing.
        """
        self.assertIsNone(cecap.take_sql(None, "Completed"))
        self.assertIsNone(cecap.take_sql(_Run("t-9"), "Completed"))


# #############################################################################
# Test_own_statement1
# #############################################################################


def _settings(url: Optional[Any]) -> Any:
    """
    A stand-in `prefect.settings` naming Prefect's own database.

    :param url: what the setting answers
    :return: a module-like object
    """
    setting = types.SimpleNamespace(value=lambda: url)
    return types.SimpleNamespace(PREFECT_API_DATABASE_CONNECTION_URL=setting)


class Test_own_statement1(unittest.TestCase):
    """
    Test telling Prefect's own database traffic from a task's.
    """

    def test1(self) -> None:
        """
        Test that a statement on the engine of Prefect's database is its
        own, whatever stands in for the password, and any other is not.
        """
        own = "postgresql+asyncpg://prefect:s3cret@db:5432/prefect"
        modules = {"prefect.settings": _settings(own)}
        with mock.patch.dict(sys.modules, modules):
            self.assertTrue(
                cecap.own_statement(
                    {"url": "postgresql+asyncpg://prefect:***@db:5432/prefect"}
                )
            )
            self.assertFalse(
                cecap.own_statement(
                    {"url": "postgresql+psycopg2://app:***@db:5432/shop"}
                )
            )
            # Seen at the driver, where there is no engine to tell by.
            self.assertFalse(cecap.own_statement({"url": None}))

    def test2(self) -> None:
        """
        Test that a secret-typed setting is read, and the default SQLite
        file matched by its path.
        """

        class _Secret:
            """Stands in for a pydantic secret string."""

            def get_secret_value(self) -> str:
                """The text behind the mask."""
                return "sqlite+aiosqlite:////home/app/.prefect/prefect.db"

        modules = {"prefect.settings": _settings(_Secret())}
        row = {"url": "sqlite+aiosqlite:////home/app/.prefect/prefect.db"}
        with mock.patch.dict(sys.modules, modules):
            self.assertTrue(cecap.own_statement(row))
            self.assertFalse(
                cecap.own_statement({"url": "sqlite:////data/shop.db"})
            )

    def test3(self) -> None:
        """
        Test that with no Prefect, no setting, or a setting that raises,
        nothing is taken for Prefect's own.
        """
        row = {"url": "sqlite:////data/shop.db"}

        def _raise() -> str:
            raise RuntimeError("no profile")

        broken = types.SimpleNamespace(
            PREFECT_API_DATABASE_CONNECTION_URL=types.SimpleNamespace(
                value=_raise
            )
        )
        for module in (None, _settings(None), broken):
            with mock.patch.dict(sys.modules, {"prefect.settings": module}):
                self.assertFalse(cecap.own_statement(row))


# #############################################################################
# Test_watch1
# #############################################################################


class Test_watch1(unittest.TestCase):
    """
    Test what importing the package switches on.
    """

    def test1(self) -> None:
        """
        Test that statements are kept per run, Prefect's own are left out,
        and the hooks go in.
        """
        with mock.patch.object(cecap, "cesqlcap") as capture:
            capture.enabled.return_value = True
            cecap.watch()
        capture.ignore.assert_called_once_with(cecap.own_statement)
        capture.scope_by.assert_called_once_with(cecap.running_scope)
        capture.install.assert_called_once_with()

    def test2(self) -> None:
        """
        Test that the switch leaves the process untouched, and that a
        failure to hook is not the flow's failure.
        """
        with mock.patch.object(cecap, "cesqlcap") as capture:
            capture.enabled.return_value = False
            cecap.watch()
            capture.install.assert_not_called()
            capture.enabled.return_value = True
            capture.install.side_effect = RuntimeError("no")
            cecap.watch()

    def test3(self) -> None:
        """
        Test that with the switch off a statement in a run is not kept.
        """
        modules = {"prefect.context": _context_module()}
        cesqlcap.scope_by(cecap.running_scope)
        self.addCleanup(cesqlcap.scope_by, None)
        token = _TASK_RUN.set(_task_context("t-1"))
        self.addCleanup(_TASK_RUN.reset, token)
        with mock.patch.dict(sys.modules, modules):
            with mock.patch.dict(os.environ, {_SQL_CAPTURE_ENV: "false"}):
                cesqlcap.record("delete from shop.orders", via="t")
            self.assertIsNone(cecap.take_sql(_Run("t-1"), "Completed"))

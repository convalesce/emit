"""
Tests for attaching to Airflow's listener API safely.

Run with `make test`.
"""

# The Dag/Task/DagRun stand-ins in Test_listener_dag_run1.test5 are the same
# shape as serialize's own cross-call $ref test, deliberately: both prove the
# same real scenario (emit issue #34) at a different layer, one against the
# serializer directly and one against the listener that has to share a
# budget across two `dump()` calls for it to matter.
# pylint: disable=duplicate-code

import logging
import sys
import types
import unittest
import unittest.mock
from typing import Any, Dict, List

import convalesce_emit_airflow.listener as cealist

_LOG = logging.getLogger(__name__)

# Taken from the module rather than restated, so the test cannot drift from
# the shape the plugin actually generates against.
_FAILED = "on_task_instance_failed"
_FAILED_SPEC = {_FAILED: cealist.FALLBACK_SPECS[_FAILED]}


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []
        self.flushes = 0

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Count the flush; nothing is queued, so nothing to send."""
        self.flushes += 1


# #############################################################################
# Test_hook_generation1
# #############################################################################


class Test_hook_generation1(unittest.TestCase):
    """
    Test that hooks match the running Airflow's hookspec.
    """

    def test1(self) -> None:
        """
        Test that a generated hook takes no defaulted parameters.

        pluggy reads a hook's required positional parameters and passes only
        those. A signature where every parameter had a default was accepted
        by Airflow, fired on every task, and delivered None for all of them
        -- including `error`, the failure message detection reads.
        """
        hook = cealist.make_hook(_FAILED, _FAILED_SPEC[_FAILED])
        params = hook.__code__.co_varnames[: hook.__code__.co_argcount]
        self.assertEqual(
            params,
            ("self", "previous_state", "task_instance", "error", "session"),
        )
        self.assertIsNone(hook.__defaults__)

    def test2(self) -> None:
        """
        Test that only hooks in the given spec are declared.

        Declaring a hook Airflow does not specify makes pluggy reject the
        plugin at registration, taking the scheduler down at startup.
        """
        cls = cealist.build_listener_class(_FAILED_SPEC)
        self.assertTrue(hasattr(cls, "on_task_instance_failed"))
        self.assertFalse(hasattr(cls, "on_dag_run_running"))

    def test3(self) -> None:
        """
        Test that an Airflow whose specs cannot be read still yields hooks.
        """
        self.assertTrue(cealist.FALLBACK_SPECS)
        cls = cealist.build_listener_class(cealist.FALLBACK_SPECS)
        for name in cealist.FALLBACK_SPECS:
            self.assertTrue(hasattr(cls, name), name)


# #############################################################################
# Test_hook_generation2
# #############################################################################


class Test_hook_generation2(unittest.TestCase):
    """
    Test that the hooks Phase 2 adds are wired the same generic way.
    """

    def test1(self) -> None:
        """
        Test that both dag-run completion hooks build and forward.

        `on_dag_run_success`/`on_dag_run_failed` were already in `WANTED`;
        this is the confirmation the plan asked for, against a spec shaped
        exactly like Airflow's own `airflow.listeners.spec.dagrun`
        (`on_dag_run_success(dag_run, msg)`), not assumed.
        """
        specs = {
            "on_dag_run_success": ("dag_run", "msg"),
            "on_dag_run_failed": ("dag_run", "msg"),
        }
        recorder = _Recorder()
        cls = cealist.build_listener_class(specs)
        listener = cls(emitter=recorder)
        for name in specs:
            self.assertTrue(hasattr(listener, name))
            getattr(listener, name)({"dag_id": "orders"}, "done")
        events = [call["event"] for call in recorder.sent]
        self.assertEqual(events, ["on_dag_run_success", "on_dag_run_failed"])
        self.assertEqual(
            recorder.sent[0]["payload"]["dag_run"], {"dag_id": "orders"}
        )

    def test2(self) -> None:
        """
        Test that an asset hook forwards the asset it was handed whole.

        The in-tool listener's bodies for `on_asset_created` and
        `on_asset_changed` are empty; here the payload is the asset.
        """
        specs = {"on_asset_created": ("asset",)}
        recorder = _Recorder()
        cls = cealist.build_listener_class(specs)
        listener = cls(emitter=recorder)
        hook = getattr(listener, "on_asset_created")
        hook({"name": "orders", "uri": "s3://orders"})
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["asset"]["uri"], "s3://orders")

    def test3(self) -> None:
        """
        Test that reading specs never returns a partial asset pair.

        Where Airflow is absent, as in this test environment, nothing
        resolves and the fallback specs are used instead, which carry
        neither hook -- never one without the other.
        """
        specs = cealist.read_specs()
        self.assertEqual(
            "on_asset_created" in specs, "on_asset_changed" in specs
        )


# #############################################################################
# Test_listener_forwarding1
# #############################################################################


class Test_listener_forwarding1(unittest.TestCase):
    """
    Test that a hook forwards every argument Airflow passed.
    """

    def test1(self) -> None:
        """
        Test that the failure message is forwarded, not dropped.
        """
        recorder = _Recorder()
        cls = cealist.build_listener_class(_FAILED_SPEC)
        listener = cls(emitter=recorder)
        # The class is assembled at runtime from Airflow's hookspecs, so the
        # attribute cannot be known statically.
        hook = getattr(listener, "on_task_instance_failed")
        hook("running", "TI", "boom: credentials expired", None)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["error"], "boom: credentials expired")
        self.assertEqual(payload["previous_state"], "running")
        # The task process exits right after the hook, without closing the
        # listener; if the event is still queued at that point it is gone.
        self.assertEqual(recorder.flushes, 1)

    def test2(self) -> None:
        """
        Test that a task instance crosses without being read for fields.

        Reading a named attribute would need a version branch per Airflow
        rename; forwarding the object whole means a rename cannot break this.
        """

        class TaskInstance:
            """Stands in for whatever Airflow hands the hook."""

            def __init__(self) -> None:
                self.dag_id = "orders"
                self.state = "failed"
                self._private = "hidden"

        recorder = _Recorder()
        cls = cealist.build_listener_class(_FAILED_SPEC)
        listener = cls(emitter=recorder)
        hook = getattr(listener, "on_task_instance_failed")
        hook(None, TaskInstance(), None, None)
        payload = recorder.sent[0]["payload"]["task_instance"]
        self.assertEqual(payload["dag_id"], "orders")
        self.assertNotIn("_private", payload)


# #############################################################################
# Test_listener_session1
# #############################################################################


class Test_listener_session1(unittest.TestCase):
    """
    Test that the ORM session is declared to pluggy and not forwarded.
    """

    def test1(self) -> None:
        """
        Test that the hook still takes `session`.

        pluggy rejects a plugin whose hook is missing a parameter Airflow
        passes, which takes the scheduler down at startup.
        """
        cls = cealist.build_listener_class(_FAILED_SPEC)
        hook = getattr(cls, "on_task_instance_failed")
        self.assertIn("session", cealist.inspect.signature(hook).parameters)

    def test2(self) -> None:
        """
        Test that the session does not reach the wire.

        It is a database connection, not anything about the run, and dumping
        it cost every task event a few hundred values of SQLAlchemy.
        """

        class Session:
            """Stands in for the scheduler's SQLAlchemy session."""

            def __init__(self) -> None:
                self.bind = "Engine(postgresql://localhost/airflow)"
                self.identity_map = {"rows": "many"}

        recorder = _Recorder()
        cls = cealist.build_listener_class(_FAILED_SPEC)
        listener = cls(emitter=recorder)
        hook = getattr(listener, "on_task_instance_failed")
        hook(None, "TI", None, Session())
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("session", payload)
        self.assertIn("task_instance", payload)


# #############################################################################
# Test_listener_dag_run1
# #############################################################################


class Test_listener_dag_run1(unittest.TestCase):
    """
    Test that a task event names the run it belongs to on Airflow 3.
    """

    def test1(self) -> None:
        """
        Test that the run is taken from the server context and spliced in.

        Airflow 3 hands the hook a `RuntimeTaskInstance` with no dag run of
        its own, so the run's type, data interval and logical date were
        nowhere in the payload and the receiver could build nothing.
        """

        class DagRun:
            """Stands in for the run the API server described."""

            def __init__(self) -> None:
                self.run_id = "manual__2026-09-11"
                self.run_type = "manual"
                self.logical_date = "2026-09-11T00:00:00+00:00"

        class ServerContext:
            """Stands in for a TIRunContext."""

            def __init__(self) -> None:
                self.dag_run = DagRun()

        class RuntimeTaskInstance:
            """Stands in for the Airflow 3 task instance."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.run_id = "manual__2026-09-11"
                # Airflow 3 keeps this in `__pydantic_private__`; a plain
                # private attribute is the same lookup.
                self._ti_context_from_server = ServerContext()

        recorder = _Recorder()
        cls = cealist.build_listener_class(_FAILED_SPEC)
        listener = cls(emitter=recorder)
        hook = getattr(listener, "on_task_instance_failed")
        hook(None, RuntimeTaskInstance(), None, None)
        dag_run = recorder.sent[0]["payload"]["task_instance"]["dag_run"]
        self.assertEqual(dag_run["run_type"], "manual")
        self.assertEqual(dag_run["run_id"], "manual__2026-09-11")

    def test2(self) -> None:
        """
        Test that a pydantic-style private attribute is found too.
        """

        class Held:
            """Stands in for a pydantic model's private storage."""

            def __init__(self, dag_run: Any) -> None:
                self.__pydantic_private__ = {"_context": dag_run}

        class Context:
            """Stands in for the server context."""

            dag_run = {"run_id": "scheduled__1"}

        self.assertEqual(
            cealist.find_dag_run(Held(Context())), {"run_id": "scheduled__1"}
        )

    def test3(self) -> None:
        """
        Test that Airflow 2's own dag run is left exactly as it was.
        """

        class TaskInstance:
            """Stands in for the Airflow 2 task instance."""

            def __init__(self) -> None:
                self.dag_run = {
                    "run_id": "scheduled__2",
                    "run_type": "scheduled",
                }

        payload, _ = cealist.shape({"task_instance": TaskInstance()})
        self.assertEqual(
            payload["task_instance"]["dag_run"],
            {"run_id": "scheduled__2", "run_type": "scheduled"},
        )

    def test4(self) -> None:
        """
        Test that a task instance exposing no run at all is still sent.
        """
        payload, _ = cealist.shape(
            {"task_instance": "TI", "previous_state": None}
        )
        self.assertEqual(payload["task_instance"], "TI")

    def test5(self) -> None:
        """
        Test that the Airflow 3 case -- the run found separately and dumped
        in its own `dump()` call -- still collapses a DAG shared with the
        task, exactly the shape emit issue #34 was about.

        The run and the task instance are dumped in two different calls
        inside `shape()`; only a budget shared across both catches this.
        """

        class Dag:
            """Stands in for an Airflow DAG."""

            def __init__(self) -> None:
                self.dag_id = "demo_pipeline"

        dag = Dag()

        class Task:
            """Stands in for the operator the hook is about."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.dag = dag

        class DagRun:
            """Stands in for the run the API server described."""

            def __init__(self) -> None:
                self.run_id = "manual__2026-09-11"
                self.dag = dag

        class ServerContext:
            """Stands in for a TIRunContext."""

            def __init__(self) -> None:
                self.dag_run = DagRun()

        class RuntimeTaskInstance:
            """Stands in for the Airflow 3 task instance."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.task = Task()
                self._ti_context_from_server = ServerContext()

        payload, _ = cealist.shape({"task_instance": RuntimeTaskInstance()})
        task = payload["task_instance"]["task"]
        dag_run = payload["task_instance"]["dag_run"]
        self.assertEqual(task["dag"]["dag_id"], "demo_pipeline")
        self.assertEqual(dag_run["dag"], {"$ref": task["dag"]["$id"]})


# #############################################################################
# Test_listener_task_group1
# #############################################################################


class Test_listener_task_group1(unittest.TestCase):
    """
    Test that a task group is named rather than walked.
    """

    def test1(self) -> None:
        """
        Test that the group's copy of the DAG does not travel again.

        A task group holds the DAG it belongs to, which the payload already
        carries on the task and on the dag run, so it was 40% of a task
        event and every byte of it a repeat.
        """

        class Group:
            """Stands in for an Airflow task group."""

            def __init__(self, dag: Any) -> None:
                self.dag = dag
                self.used_group_ids = ["extract", "load"]

            def __str__(self) -> str:
                return "<TaskGroup: demo_pipeline>"

        class Task:
            """Stands in for the operator the hook is about."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.dag = {"dag_id": "demo_pipeline", "description": "demo"}
                self.task_group = Group(self.dag)

        class TaskInstance:
            """Stands in for the task instance Airflow passes."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.task = Task()
                self.dag_run = {"run_id": "manual__1", "dag": self.task.dag}

        recorder = _Recorder()
        cls = cealist.build_listener_class(_FAILED_SPEC)
        listener = cls(emitter=recorder)
        hook = getattr(listener, "on_task_instance_failed")
        hook(None, TaskInstance(), None, None)
        task = recorder.sent[0]["payload"]["task_instance"]["task"]
        self.assertEqual(task["task_group"], "<TaskGroup: demo_pipeline>")
        # What the group held is still on the task, dumped in full.
        self.assertEqual(task["dag"]["dag_id"], "demo_pipeline")
        # The dag run carries the same DAG object, not a second copy: it
        # collapses to a $ref pointing at the task's copy.
        dag_run = recorder.sent[0]["payload"]["task_instance"]["dag_run"]
        self.assertEqual(dag_run["dag"], {"$ref": task["dag"]["$id"]})


# #############################################################################
# Test_connection_coordinates1
# #############################################################################


class Test_connection_coordinates1(unittest.TestCase):
    """
    Test that a task's connection ids resolve to coordinates, never secrets.
    """

    def test1(self) -> None:
        """
        Test that every `*_conn_id` attribute is resolved, and only the
        four allowed fields cross -- never `password`, never `extra`.
        """

        class Connection:
            """Stands in for Airflow's own Connection model."""

            def __init__(self) -> None:
                self.conn_type = "sqlite"
                self.host = "/tmp/convalesce.db"
                self.port = None
                self.schema = None
                self.password = "s3cret"
                self.extra = '{"private_key": "s3cret"}'

        class Task:
            """Stands in for a SQL operator."""

            def __init__(self) -> None:
                self.sqlite_conn_id = "convalesce_sqlite"
                self.task_id = "load"

        with unittest.mock.patch.object(
            cealist, "_get_connection", return_value=Connection()
        ) as get:
            coords = cealist.connection_coordinates(Task())
        get.assert_called_once_with("convalesce_sqlite")
        self.assertEqual(
            coords,
            {
                "convalesce_sqlite": {
                    "conn_id": "convalesce_sqlite",
                    "conn_type": "sqlite",
                    "host": "/tmp/convalesce.db",
                    "port": None,
                    "schema": None,
                }
            },
        )
        self.assertNotIn("password", str(coords))
        self.assertNotIn("s3cret", str(coords))

    def test2(self) -> None:
        """
        Test that a connection id that does not resolve is dropped, not
        raised -- a deleted or unseeded connection is Airflow's business.
        """

        class Task:
            """Stands in for an operator naming a connection that is gone."""

            def __init__(self) -> None:
                self.conn_id = "missing"

        with unittest.mock.patch.object(
            cealist, "_get_connection", side_effect=Exception("no such conn")
        ):
            coords = cealist.connection_coordinates(Task())
        self.assertEqual(coords, {})

    def test3(self) -> None:
        """
        Test that connection coordinates ride along on the dumped task.
        """

        class Connection:
            """Stands in for Airflow's own Connection model."""

            def __init__(self) -> None:
                self.conn_type = "postgres"
                self.host = "warehouse.internal"
                self.port = 5432
                self.schema = "public"

        class Task:
            """Stands in for a SQL operator."""

            def __init__(self) -> None:
                self.postgres_conn_id = "warehouse"
                self.task_id = "load"

        class TaskInstance:
            """Stands in for the task instance Airflow passes."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.task = Task()

        with unittest.mock.patch.object(
            cealist, "_get_connection", return_value=Connection()
        ):
            payload, _ = cealist.shape({"task_instance": TaskInstance()})
        connections = payload["task_instance"]["task"]["connections"]
        self.assertEqual(connections["warehouse"]["conn_type"], "postgres")


# #############################################################################
# Test_asset_aliases1
# #############################################################################


class Test_asset_aliases1(unittest.TestCase):
    """
    Test that runtime-resolved asset aliases are forwarded, and only when
    something actually resolved.
    """

    def test1(self) -> None:
        """
        Test that an alias with a resolved event is read off the template
        context's `OutletEventAccessors`.
        """

        class Accessor:
            """Stands in for one `OutletEventAccessor`."""

            def __init__(self, events: List[Any]) -> None:
                self.asset_alias_events = events

        class Accessors:
            """Stands in for `OutletEventAccessors`."""

            def __init__(self, mapping: Dict[str, Accessor]) -> None:
                self._mapping = mapping

            def items(self) -> Any:
                """Return the accessors, the same shape as the real class."""
                return self._mapping.items()

        class Context(dict):
            """Stands in for the template context."""

        resolved = {"source_alias_name": "daily", "dest_asset_key": "orders"}
        context = Context(
            {"outlet_events": Accessors({"daily": Accessor([resolved])})}
        )

        class TaskInstance:
            """Stands in for a task instance exposing its own context."""

            def get_template_context(self) -> Any:
                """Return the context the task ran with."""
                return context

        aliases = cealist.asset_aliases(TaskInstance())
        self.assertEqual(aliases, [resolved])

    def test2(self) -> None:
        """
        Test that an alias with nothing resolved is left out -- a direct
        asset outlet is already sent and carries no alias.
        """

        class Accessor:
            """Stands in for an accessor nothing wrote through."""

            asset_alias_events: List[Any] = []

        class Accessors:
            """Stands in for `OutletEventAccessors`."""

            def items(self) -> Any:
                """Return one accessor, resolved to nothing."""
                return [("unused", Accessor())]

        class TaskInstance:
            """Stands in for a task instance exposing its own context."""

            def get_template_context(self) -> Any:
                """Return the context the task ran with."""
                return {"outlet_events": Accessors()}

            dag_id = None

        self.assertEqual(cealist.asset_aliases(TaskInstance()), [])

    def test3(self) -> None:
        """
        Test that the `AssetEvent` fallback is used when the context has
        nothing, and rows with empty `source_aliases` are skipped -- those
        are direct assets, already sent.
        """

        class Row:
            """Stands in for one `AssetEvent` row."""

            def __init__(self, source_aliases: List[str]) -> None:
                self.source_aliases = source_aliases

        direct = Row([])
        aliased = Row(["daily"])

        class Query:
            """Stands in for the SQLAlchemy query chain."""

            def filter(self, *_args: Any) -> "Query":
                """Ignore the filter clauses; return the same two rows."""
                return self

            def all(self) -> List[Row]:
                """Return both rows, as an unfiltered stand-in would."""
                return [direct, aliased]

        class Session:
            """Stands in for Airflow's ORM session."""

            def query(self, *_args: Any) -> Query:
                """Return the stand-in query, ignoring what was asked for."""
                return Query()

            def close(self) -> None:
                """Do nothing; there is no real connection to release."""

        class TaskInstance:
            """Stands in for a task instance with no live context."""

            dag_id = "orders"
            run_id = "manual__1"
            task_id = "load"

        class FakeAssetEvent:
            """Stands in for the `AssetEvent` model class, columns only."""

            source_dag_id = None
            source_run_id = None
            source_task_id = None
            source_map_index = None

        def _session() -> Session:
            """Build the stand-in session; called where `Session()` is."""
            return Session()

        module = types.SimpleNamespace(Session=_session)
        asset_module = types.SimpleNamespace(AssetEvent=FakeAssetEvent)
        with unittest.mock.patch.dict(
            sys.modules,
            {
                "airflow.settings": module,
                "airflow.models.asset": asset_module,
            },
        ):
            aliases = cealist.asset_aliases(TaskInstance())
        self.assertEqual(aliases, [aliased])

    def test4(self) -> None:
        """
        Test that `Session()` itself raising -- what Airflow 3's sandboxed
        task process actually does, found by running a real DAG rather than
        by reading -- loses only the aliases, not the whole task event.

        The first working version of this fallback called `Session()`
        outside its own `try`, so this exact failure escaped
        `asset_aliases()` entirely and took down `on_task_instance_running`
        and `on_task_instance_success` for every task, not just the alias
        read: "Direct database access via the ORM is not allowed in
        Airflow 3.0", raised at construction, before a query was ever
        built.
        """

        class TaskInstance:
            """Stands in for a task instance with no live context."""

            dag_id = "orders"
            run_id = "manual__1"
            task_id = "load"

        class FakeAssetEvent:
            """Stands in for the `AssetEvent` model class, columns only."""

        def _forbidden_session() -> Any:
            """Raise, the way Airflow 3's task sandbox actually does."""
            raise RuntimeError(
                "Direct database access via the ORM is not allowed in "
                "Airflow 3.0"
            )

        module = types.SimpleNamespace(Session=_forbidden_session)
        asset_module = types.SimpleNamespace(AssetEvent=FakeAssetEvent)
        with unittest.mock.patch.dict(
            sys.modules,
            {
                "airflow.settings": module,
                "airflow.models.asset": asset_module,
            },
        ):
            self.assertEqual(cealist.asset_aliases(TaskInstance()), [])


def _failed_load() -> BaseException:
    """
    An exception raised from another, as a task's would be.

    :return: the outer exception, as caught
    """
    try:
        try:
            raise ConnectionError("db refused")
        except ConnectionError as exc:
            raise RuntimeError("load failed") from exc
    except RuntimeError as caught:
        return caught


# #############################################################################
# Test_listener_error_detail1
# #############################################################################


class Test_listener_error_detail1(unittest.TestCase):
    """
    Test that a failure carries the exception's class and traceback.
    """

    def test1(self) -> None:
        """
        Test that an exception passed as `error` is described alongside
        its message.
        """
        error = _failed_load()
        recorder = _Recorder()
        listener = cealist.build_listener_class(_FAILED_SPEC)(emitter=recorder)
        getattr(listener, _FAILED)("running", "TI", error, None)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["error"], "load failed")
        detail = payload["error_detail"]
        self.assertEqual(detail["type"], "builtins.RuntimeError")
        self.assertEqual(detail["message"], "load failed")
        self.assertIn("RuntimeError: load failed", detail["traceback"])
        self.assertEqual(detail["cause"]["type"], "builtins.ConnectionError")

    def test2(self) -> None:
        """
        Test that a message string, which is all Airflow's scheduler passes
        for a task it found dead, is sent without a made-up detail.
        """
        recorder = _Recorder()
        listener = cealist.build_listener_class(_FAILED_SPEC)(emitter=recorder)
        getattr(listener, _FAILED)("running", "TI", "zombie", None)
        getattr(listener, _FAILED)("running", "TI", None, None)
        for sent in recorder.sent:
            self.assertNotIn("error_detail", sent["payload"])

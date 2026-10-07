"""
Tests for what each step ran: its source, its SQL and its configuration.
"""

import os
import sys
import types
import unittest
import unittest.mock
from typing import Any, Dict, Iterator, List, Optional

import convalesce_emit.sqlcapture as cesqlcap
import convalesce_emit_dagster.sensor as cedsens
import convalesce_emit_dagster.steps as cedsteps

_SEAM = "dagster._core.execution.plan.execute_plan"


def _load_orders(context: Any) -> None:
    """Stands in for an op's function."""
    del context


def _tidy() -> None:
    """Stands in for an op's function inside a graph."""


class _Op:
    """The shape of an `OpDefinition`: the author's function, wrapped."""

    def __init__(self, function: Any) -> None:
        self.compute_fn = types.SimpleNamespace(decorated_fn=function)


class _Graph:
    """The shape of a `GraphDefinition`: nodes, and no function."""

    def __init__(self, **nodes: Any) -> None:
        self.nodes = [
            types.SimpleNamespace(name=name, definition=definition)
            for name, definition in nodes.items()
        ]


class _StepContext:
    """The shape of a step's execution context."""

    def __init__(self, config: Any = None, function: Any = _load_orders) -> None:
        self.op_config = config
        self.op_def = _Op(function)


def _engine_event(step_key: Optional[str], metadata: Any) -> Any:
    """One event-log record holding an engine event."""
    event = types.SimpleNamespace(
        step_key=step_key,
        event_specific_data=types.SimpleNamespace(metadata=metadata),
    )
    return types.SimpleNamespace(
        event_log_entry=types.SimpleNamespace(dagster_event=event)
    )


def _note(note: Dict[str, Any]) -> Dict[str, Any]:
    """A note as an engine event's metadata carries it."""
    return {cedsteps.NOTE_KEY: types.SimpleNamespace(data=note)}


def _sql(*statements: str) -> Dict[str, Any]:
    """What `sqlcapture.drain` hands over for these statements."""
    return {
        "statements": [
            {
                "statement": text,
                "dialect": "postgres",
                "database": "warehouse",
                "schema": None,
                "via": "psycopg2",
                "count": 1,
            }
            for text in statements
        ],
        "dropped": 0,
        "failures": 0,
    }


class _Instance:
    """An instance whose event log holds the given records."""

    def __init__(self, records: List[Any]) -> None:
        self.records = records
        self.asked: List[Any] = []

    def get_records_for_run(self, run_id: str, of_type: Any = None) -> Any:
        """The run's records of one type."""
        self.asked.append((run_id, of_type))
        return types.SimpleNamespace(records=self.records)


class _Repository:
    """A repository holding one job and, perhaps, assets."""

    def __init__(self, job: Any = None, assets: Any = None) -> None:
        self._job = job
        self.assets_defs_by_key = assets or {}

    def get_job(self, name: str) -> Any:
        """The job by name, or the error Dagster raises for an unknown one."""
        if self._job is None:
            raise KeyError(name)
        return self._job


def _fake_dagster() -> Dict[str, Any]:
    """A stand-in `dagster` module carrying the one name the sensor reads."""
    module = types.SimpleNamespace(
        DagsterEventType=types.SimpleNamespace(ENGINE_EVENT="ENGINE_EVENT")
    )
    return {"dagster": module}


# #############################################################################
# Test_bracketed1
# #############################################################################


class Test_bracketed1(unittest.TestCase):
    """
    Test that a bracketed step runs exactly as it did, and is noted.
    """

    def setUp(self) -> None:
        """
        Leave no statements behind from another test.
        """
        cesqlcap.drain()
        self.addCleanup(cesqlcap.drain)

    def test1(self) -> None:
        """
        Test that the step's events pass through and its SQL is written.
        """

        def original(step_context: Any, flag: bool = False) -> Iterator[str]:
            del step_context
            yield "STEP_START"
            cesqlcap.record(
                "insert into totals select * from orders", via="test"
            )
            yield f"STEP_SUCCESS {flag}"

        written: List[Any] = []
        context = _StepContext({"limit": 5})
        with unittest.mock.patch.object(
            cedsteps, "write_note", lambda ctx, note: written.append((ctx, note))
        ):
            events = list(cedsteps.bracketed(original)(context, flag=True))
        self.assertEqual(events, ["STEP_START", "STEP_SUCCESS True"])
        self.assertEqual(len(written), 1)
        self.assertIs(written[0][0], context)
        note = written[0][1]
        self.assertEqual(
            [entry["statement"] for entry in note["sql"]["statements"]],
            ["insert into totals select * from orders"],
        )
        self.assertEqual(note["config"], {"limit": 5})

    def test2(self) -> None:
        """
        Test that a failing step is still noted and its error untouched.
        """

        def original(step_context: Any) -> Iterator[str]:
            del step_context
            yield "STEP_START"
            cesqlcap.record("insert into totals select 1 from missing", via="t")
            raise ValueError("no such table")

        written: List[Any] = []
        with unittest.mock.patch.object(
            cedsteps, "write_note", lambda ctx, note: written.append(note)
        ):
            with self.assertRaises(ValueError):
                list(cedsteps.bracketed(original)(_StepContext({"table": "x"})))
        self.assertEqual(written[0]["config"], {"table": "x"})
        self.assertEqual(len(written[0]["sql"]["statements"]), 1)

    def test3(self) -> None:
        """
        Test that a note that cannot be written costs the step nothing.
        """

        def original(step_context: Any) -> Iterator[str]:
            del step_context
            yield "STEP_SUCCESS"

        def broken(ctx: Any, note: Any) -> None:
            raise RuntimeError("event log is down")

        with unittest.mock.patch.object(cedsteps, "write_note", broken):
            events = list(
                cedsteps.bracketed(original)(_StepContext({"limit": 5}))
            )
        self.assertEqual(events, ["STEP_SUCCESS"])

    def test4(self) -> None:
        """
        Test that a step with no SQL and no configuration writes nothing.
        """

        def original(step_context: Any) -> Iterator[str]:
            del step_context
            yield "STEP_SUCCESS"

        written: List[Any] = []
        with unittest.mock.patch.object(
            cedsteps, "write_note", lambda ctx, note: written.append(note)
        ):
            list(cedsteps.bracketed(original)(_StepContext()))
        self.assertEqual(written, [])

    def test5(self) -> None:
        """
        Test that nothing is noted once the step is over.
        """

        def original(step_context: Any) -> Iterator[str]:
            del step_context
            yield "STEP_SUCCESS"

        with unittest.mock.patch.object(cedsteps, "write_note", lambda *a: None):
            list(cedsteps.bracketed(original)(_StepContext()))
        cesqlcap.record("insert into totals select * from orders", via="test")
        self.assertIsNone(cesqlcap.drain())


# #############################################################################
# Test_note_of1
# #############################################################################


class Test_note_of1(unittest.TestCase):
    """
    Test what a step's note holds.
    """

    def test1(self) -> None:
        """
        Test that secrets in the configuration are masked before writing.

        The note goes into the customer's event log, so it is masked where
        it is made, not only where it is sent.
        """
        note = cedsteps.note_of(
            _StepContext({"table": "orders", "db_password": "hunter2"}), None
        )
        self.assertEqual(note["config"]["table"], "orders")
        self.assertNotIn("hunter2", repr(note))

    def test2(self) -> None:
        """
        Test that the note carries the hashes of the function that ran.
        """
        note = cedsteps.note_of(_StepContext({"limit": 1}), None)
        self.assertEqual(len(note["ran"]["sha256"]), 64)
        self.assertEqual(len(note["ran"]["file_sha256"]), 64)
        self.assertNotIn("text", note["ran"])

    def test3(self) -> None:
        """
        Test that each switch leaves its own fact out.
        """
        context = _StepContext({"limit": 1})
        noted = _sql("insert into totals select * from orders")
        with unittest.mock.patch.dict(
            os.environ, {"CONVALESCE_SEND_ARGUMENTS": "false"}
        ):
            self.assertNotIn("config", cedsteps.note_of(context, noted))
        with unittest.mock.patch.dict(
            os.environ, {"CONVALESCE_SQL_CAPTURE": "off"}
        ):
            self.assertNotIn("sql", cedsteps.note_of(context, noted))
        with unittest.mock.patch.dict(
            os.environ, {"CONVALESCE_SEND_SOURCE": "0"}
        ):
            self.assertNotIn("ran", cedsteps.note_of(context, noted))

    def test4(self) -> None:
        """
        Test that statements past the note's size are counted, not kept.
        """
        big = "insert into totals select " + "x" * cedsteps.MAX_NOTE_CHARS
        note = cedsteps.note_of(
            _StepContext(), _sql("insert into totals select 1", big)
        )
        self.assertEqual(len(note["sql"]["statements"]), 1)
        self.assertEqual(note["sql"]["dropped"], 1)

    def test5(self) -> None:
        """
        Test that a configuration holding Python objects crosses as text.
        """

        class Mode:
            """A value a config type mapped to."""

        note = cedsteps.note_of(_StepContext({"mode": Mode()}), None)
        self.assertIsInstance(note["config"]["mode"], (str, dict))


# #############################################################################
# Test_own_traffic1
# #############################################################################


def _function_in(module_name: str, name: str, body: str, **names: Any) -> Any:
    """
    Make a function whose code belongs to the named module.

    :param module_name: what the function's frame says its module is
    :param name: the function's name
    :param body: the one expression it returns
    :param names: what that expression may refer to
    :return: the function
    """
    scope: Dict[str, Any] = {"__name__": module_name, **names}
    source = f"def {name}():\n    return {body}\n"
    exec(source, scope)  # pylint: disable=exec-used
    return scope[name]


def _called_from(module_name: str) -> bool:
    """Ask `own_traffic` from code that belongs to the named module."""
    call = _function_in(module_name, "call", "ask({})", ask=cedsteps.own_traffic)
    return bool(call())


class Test_own_traffic1(unittest.TestCase):
    """
    Test that Dagster's own database traffic is told from a step's.
    """

    def test1(self) -> None:
        """
        Test that a statement sent by Dagster's storage is left out.
        """
        for name in (
            "dagster._core.storage.event_log.sql_event_log",
            "dagster._core.storage.runs.sql_run_storage",
            "dagster._core.storage.sqlite_storage",
            "dagster_postgres.event_log.event_log",
        ):
            self.assertTrue(_called_from(name), name)

    def test2(self) -> None:
        """
        Test that a statement sent by a step, or an IO manager, is kept.
        """
        for name in (
            "my_project.ops",
            "dagster._core.storage.db_io_manager",
            "dagster._core.execution.plan.compute_generator",
        ):
            self.assertFalse(_called_from(name), name)

    def test3(self) -> None:
        """
        Test that storage reached from a step's own code is still Dagster's.

        A step that asks the instance for an asset's last materialisation
        has its own frames on the stack, below the storage's.
        """
        read = _function_in(
            "dagster._core.storage.event_log.sql_event_log",
            "read",
            "ask({})",
            ask=cedsteps.own_traffic,
        )
        step = _function_in("my_project.ops", "step", "read()", read=read)
        self.assertTrue(step())


# #############################################################################
# Test_install1
# #############################################################################


class Test_install1(unittest.TestCase):
    """
    Test that the bracket goes in once, and only where it can.
    """

    def setUp(self) -> None:
        """
        Start each test as a process that has not installed anything.
        """
        patcher = unittest.mock.patch.object(cedsteps, "_installed", False)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in ("ignore", "install"):
            quiet = unittest.mock.patch.object(cesqlcap, name, lambda *a: None)
            quiet.start()
            self.addCleanup(quiet.stop)

    def test1(self) -> None:
        """
        Test that Dagster's step function is replaced by a bracketed one.
        """

        def original(step_context: Any) -> Iterator[str]:
            del step_context
            yield "STEP_SUCCESS"

        seam = types.SimpleNamespace(dagster_event_sequence_for_step=original)
        with unittest.mock.patch.dict(sys.modules, {_SEAM: seam}):
            self.assertTrue(cedsteps.install())
            wrapped = seam.dagster_event_sequence_for_step
            self.assertIsNot(wrapped, original)
            # A second import of the package wraps nothing again.
            self.assertFalse(cedsteps.install())
            self.assertIs(seam.dagster_event_sequence_for_step, wrapped)
        self.assertEqual(list(wrapped(_StepContext())), ["STEP_SUCCESS"])

    def test5(self) -> None:
        """
        Test that a module that already took the function by name follows.

        A step launcher's remote entry point imports it before the
        definitions, and so before this package, are loaded.
        """

        def original(step_context: Any) -> Iterator[str]:
            del step_context
            yield "STEP_SUCCESS"

        seam = types.SimpleNamespace(dagster_event_sequence_for_step=original)
        holder = types.SimpleNamespace(dagster_event_sequence_for_step=original)
        modules = {
            _SEAM: seam,
            "dagster._core.execution.plan.external_step": holder,
        }
        with unittest.mock.patch.dict(sys.modules, modules):
            self.assertTrue(cedsteps.install())
        self.assertIs(
            holder.dagster_event_sequence_for_step,
            seam.dagster_event_sequence_for_step,
        )

    def test2(self) -> None:
        """
        Test that a Dagster without that function is left alone.
        """
        seam = types.SimpleNamespace()
        with unittest.mock.patch.dict(sys.modules, {_SEAM: seam}):
            self.assertFalse(cedsteps.install())
        self.assertFalse(hasattr(seam, "dagster_event_sequence_for_step"))

    def test3(self) -> None:
        """
        Test that with Dagster absent nothing is raised.
        """
        with unittest.mock.patch.dict(sys.modules, {_SEAM: None}):
            self.assertFalse(cedsteps.install())

    def test4(self) -> None:
        """
        Test that with every switch off Dagster is not touched at all.
        """

        def original(step_context: Any) -> Iterator[str]:
            del step_context
            yield "STEP_SUCCESS"

        seam = types.SimpleNamespace(dagster_event_sequence_for_step=original)
        off = {
            "CONVALESCE_SQL_CAPTURE": "false",
            "CONVALESCE_SEND_ARGUMENTS": "false",
            "CONVALESCE_SEND_SOURCE": "false",
        }
        with unittest.mock.patch.dict(sys.modules, {_SEAM: seam}):
            with unittest.mock.patch.dict(os.environ, off):
                self.assertFalse(cedsteps.install())
        self.assertIs(seam.dagster_event_sequence_for_step, original)


# #############################################################################
# Test_read_notes1
# #############################################################################


class Test_read_notes1(unittest.TestCase):
    """
    Test that the notes steps wrote are read back by step key.
    """

    def test1(self) -> None:
        """
        Test that only our engine events are read, each under its step.
        """
        instance = _Instance(
            [
                _engine_event("load", {"pid": types.SimpleNamespace(data=7)}),
                _engine_event(None, _note({"config": {"a": 1}})),
                _engine_event("load", _note({"config": {"limit": 5}})),
                _engine_event("load", None),
            ]
        )
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            notes = cedsteps.read_notes(instance, "abc")
        self.assertEqual(notes, {"load": {"config": {"limit": 5}}})
        self.assertEqual(instance.asked, [("abc", {"ENGINE_EVENT"})])

    def test2(self) -> None:
        """
        Test that a step tried twice keeps both tries' statements.
        """
        first = {
            "sql": _sql("insert into a select 1 from b"),
            "config": {"n": 1},
        }
        second = {
            "sql": _sql(
                "insert into a select 1 from b", "insert into c select 1 from a"
            ),
            "config": {"n": 2},
        }
        instance = _Instance(
            [
                _engine_event("load", _note(first)),
                _engine_event("load", _note(second)),
            ]
        )
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            note = cedsteps.read_notes(instance, "abc")["load"]
        self.assertEqual(
            [entry["statement"] for entry in note["sql"]["statements"]],
            ["insert into a select 1 from b", "insert into c select 1 from a"],
        )
        self.assertEqual(note["config"], {"n": 2})

    def test3(self) -> None:
        """
        Test that an event log that cannot be read gives no notes.
        """

        class Broken:
            """An instance whose storage is unreachable."""

            def get_records_for_run(
                self, run_id: str, of_type: Any = None
            ) -> Any:
                """Fail, the way a storage outage does."""
                raise RuntimeError("no storage")

        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            self.assertEqual(cedsteps.read_notes(Broken(), "abc"), {})
        self.assertEqual(cedsteps.read_notes(None, "abc"), {})


# #############################################################################
# Test_describe1
# #############################################################################


class _Run:
    """Stands in for a DagsterRun."""

    def __init__(self) -> None:
        self.job_name = "nightly"
        self.run_id = "abc"
        self.run_config = {"ops": {"load": {"config": {"limit": 5}}}}


def _stats(*keys: str) -> List[Any]:
    """Step stats for the given step keys."""
    return [types.SimpleNamespace(step_key=key) for key in keys]


class Test_describe1(unittest.TestCase):
    """
    Test the one entry per executed step the sensor sends.
    """

    def _context(
        self, records: Any = (), job: Any = None, assets: Any = None
    ) -> Any:
        """A run-status context over the given event log and repository."""
        return types.SimpleNamespace(
            instance=_Instance(list(records)),
            repository_def=_Repository(job, assets),
        )

    def test1(self) -> None:
        """
        Test that a step gets its source, through a graph, and its note.
        """
        job = _Graph(load=_Op(_load_orders), refresh=_Graph(tidy=_Op(_tidy)))
        records = [
            _engine_event(
                "load",
                _note(
                    {
                        "config": {"limit": 5},
                        "sql": _sql("insert into a select 1"),
                    }
                ),
            )
        ]
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            steps = cedsteps.describe(
                self._context(records, job),
                _Run(),
                _stats("load", "refresh.tidy"),
            )
        by_key = {step["step_key"]: step for step in steps}
        self.assertIn("def _load_orders", by_key["load"]["source"]["text"])
        self.assertEqual(by_key["load"]["config"], {"limit": 5})
        self.assertEqual(
            by_key["load"]["sql"]["statements"][0]["statement"],
            "insert into a select 1",
        )
        self.assertIn("def _tidy", by_key["refresh.tidy"]["source"]["text"])
        self.assertNotIn("sql", by_key["refresh.tidy"])

    def test2(self) -> None:
        """
        Test that a mapped step's source text crosses once.
        """
        job = _Graph(load=_Op(_load_orders))
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            steps = cedsteps.describe(
                self._context((), job), _Run(), _stats("load[eu]", "load[us]")
            )
        self.assertIn("text", steps[0]["source"])
        self.assertNotIn("text", steps[1]["source"])
        self.assertEqual(
            steps[0]["source"]["sha256"], steps[1]["source"]["sha256"]
        )

    def test3(self) -> None:
        """
        Test that an asset's step finds its function without its job.

        A sensor watching another code location cannot read that job, but
        an asset it does define is still found by its node's name.
        """
        assets = {
            "k": types.SimpleNamespace(node_def=_named(_Op(_tidy), "totals"))
        }
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            steps = cedsteps.describe(
                self._context((), None, assets), _Run(), _stats("totals")
            )
        self.assertIn("def _tidy", steps[0]["source"]["text"])

    def test4(self) -> None:
        """
        Test that the switches leave source and configuration out.
        """
        job = _Graph(load=_Op(_load_orders))
        records = [_engine_event("load", _note({"config": {"limit": 5}}))]
        off = {"CONVALESCE_SEND_ARGUMENTS": "no", "CONVALESCE_SEND_SOURCE": "no"}
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            with unittest.mock.patch.dict(os.environ, off):
                steps = cedsteps.describe(
                    self._context(records, job), _Run(), _stats("load")
                )
        self.assertEqual(steps, [])

    def test5(self) -> None:
        """
        Test that a note for a step the stats do not list still crosses.
        """
        records = [_engine_event("late", _note({"config": {"n": 1}}))]
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            steps = cedsteps.describe(self._context(records), _Run(), None)
        self.assertEqual(steps, [{"step_key": "late", "config": {"n": 1}}])

    def test6(self) -> None:
        """
        Test that a mapping key is not taken for part of the node's name.
        """
        self.assertEqual(cedsteps.node_of("load[eu.west]"), "load")
        self.assertEqual(cedsteps.node_of("refresh.tidy[a]"), "refresh.tidy")
        self.assertEqual(cedsteps.node_of("load"), "load")


def _named(definition: Any, name: str) -> Any:
    """Give a definition the name its node goes by."""
    definition.name = name
    return definition


# #############################################################################
# Test_sensor_steps1
# #############################################################################


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


class _SensorInstance(_Instance):
    """An instance that also answers for the run's step stats."""

    def get_run_step_stats(self, run_id: str) -> List[Any]:
        """One step, which is what ran."""
        del run_id
        return _stats("load")


class _SensorContext:
    """A run-status context with an instance and a repository behind it."""

    def __init__(self) -> None:
        note = {
            "config": {"limit": 5, "api_token": {"redacted": True}},
            "sql": _sql("insert into a select 1 from b"),
        }
        self.instance = _SensorInstance([_engine_event("load", _note(note))])
        self.repository_def = _Repository(_Graph(load=_Op(_load_orders)))
        self.sensor_name = "convalesce_on_success"
        self.dagster_event = {"event_type_value": "PIPELINE_SUCCESS"}
        self.dagster_run = _Run()


class Test_sensor_steps1(unittest.TestCase):
    """
    Test that the sensor sends what each step ran with the run.
    """

    def test1(self) -> None:
        """
        Test that `steps` crosses beside the run, source and SQL in it.
        """
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            cedsens.convalesce_sensor(_SensorContext(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        step = payload["steps"][0]
        self.assertEqual(step["step_key"], "load")
        self.assertIn("def _load_orders", step["source"]["text"])
        self.assertEqual(
            step["sql"]["statements"][0]["statement"],
            "insert into a select 1 from b",
        )
        self.assertEqual(step["config"]["limit"], 5)
        self.assertEqual(
            payload["dagster_run"]["run_config"],
            {"ops": {"load": {"config": {"limit": 5}}}},
        )

    def test2(self) -> None:
        """
        Test that with arguments off neither configuration crosses.

        The run's own configuration is the same values a step's is
        resolved from, so the switch that withholds one withholds both,
        and says so.
        """
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, _fake_dagster()):
            with unittest.mock.patch.dict(
                os.environ, {"CONVALESCE_SEND_ARGUMENTS": "false"}
            ):
                cedsens.convalesce_sensor(_SensorContext(), emitter=recorder)
        sent = recorder.sent[0]
        self.assertNotIn("run_config", sent["payload"]["dagster_run"])
        self.assertNotIn("config", sent["payload"]["steps"][0])
        self.assertIn(
            {"path": "dagster_run.run_config", "reason": "arguments not sent"},
            sent["excluded"],
        )

    def test3(self) -> None:
        """
        Test that an asset's code references are allowed through.
        """
        kept = cedsens._allowed_metadata(  # pylint: disable=protected-access
            {
                "dagster/code_references": {
                    "code_references": [{"file_path": "a.py", "line_number": 3}]
                },
                "preview": {"md_str": "rows"},
            }
        )
        self.assertEqual(
            kept["dagster/code_references"]["code_references"][0]["line_number"],
            3,
        )
        self.assertEqual(kept["count"], 1)

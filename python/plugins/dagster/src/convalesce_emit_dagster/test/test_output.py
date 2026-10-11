"""
Tests for sending the last lines a failed step logged.

Run with `make test`.
"""

import json
import logging
import os
import sys
import types
import unittest
import unittest.mock
from typing import Any, Callable, Dict, Iterator, List, Optional

import convalesce_emit.output as ceoutput
import convalesce_emit.sqlcapture as cesqlcap
import convalesce_emit_dagster.sensor as cedsens
import convalesce_emit_dagster.steps as cedsteps

# Each plugin proves the same thing of its own event: the tail is sent
# with a failure and no other event, within its limits and masked. The
# plugins are independently installable with no dependency on each other,
# so sharing a test module between them would add one for no real benefit;
# the similarity stays and the check is turned off here rather than
# everywhere.
# pylint: disable=duplicate-code

_PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n"
    "-----END PRIVATE KEY-----"
)
# Only the output is under test: a step with nothing else to say.
_ENV = {"CONVALESCE_SEND_ARGUMENTS": "false", "CONVALESCE_SEND_SOURCE": "false"}


class _StepContext:
    """The shape of a step's execution context, with its own logger."""

    def __init__(self) -> None:
        self.op_config = None
        self.op_def = None
        # In no hierarchy, as Dagster's log manager is.
        self.log = logging.Logger("dagster")
        self.log.setLevel(logging.DEBUG)

    def say(self, text: str) -> None:
        """Log one line the way Dagster's log manager hands it on."""
        meta = {"orig_message": text, "dagster_event": None}
        self.log.info(
            "orders - 1a2b - load - %s", text, extra={"dagster_meta": meta}
        )

    def report(self, text: str) -> None:
        """Log one of Dagster's own events."""
        meta = {"orig_message": text, "dagster_event": object()}
        self.log.info(
            "orders - 1a2b - load - %s", text, extra={"dagster_meta": meta}
        )


def _error(message: str, *stack: str, cause: Any = None) -> Any:
    """The shape of the error a step's failure event carries."""
    return types.SimpleNamespace(
        message=message + "\n", stack=list(stack), cause=cause, context=None
    )


def _failure(error: Any) -> Any:
    """The shape of a `STEP_FAILURE` event."""
    return types.SimpleNamespace(
        event_type_value="STEP_FAILURE",
        event_specific_data=types.SimpleNamespace(error=error),
    )


def _step(
    context: _StepContext,
    body: Callable[[], None],
    ending: Any = "STEP_SUCCESS",
    raises: Optional[BaseException] = None,
    environ: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Run one bracketed step and return the notes it wrote.

    :param context: the step's context
    :param body: what the step's code does
    :param ending: the event the step ends with
    :param raises: what leaves Dagster's own code instead, if anything
    :param environ: the environment the step's process has
    :return: each note written, one at most
    """

    def original(step_context: Any) -> Iterator[Any]:
        del step_context
        yield "STEP_START"
        body()
        if raises is not None:
            raise raises
        yield ending

    written: List[Dict[str, Any]] = []
    with (
        unittest.mock.patch.dict(os.environ, environ or _ENV, clear=True),
        unittest.mock.patch.object(
            cedsteps, "write_note", lambda ctx, note: written.append(note)
        ),
    ):
        try:
            list(cedsteps.bracketed(original)(context))
        except BaseException as exc:  # pylint: disable=broad-exception-caught
            if exc is not raises:
                raise
    return written


def _noted(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """The one note a step run as `_step` runs it wrote."""
    return _step(*args, **kwargs)[0]


# #############################################################################
# Test_step_output1
# #############################################################################


class Test_step_output1(unittest.TestCase):
    """
    Test what a step's process notes of its output.
    """

    def setUp(self) -> None:
        cesqlcap.drain()
        self.addCleanup(cesqlcap.drain)
        root = logging.getLogger()
        self.addCleanup(root.setLevel, root.level)
        root.setLevel(logging.INFO)
        self.handlers = list(root.handlers)
        self.factory = logging.getLogRecordFactory()
        self.addCleanup(logging.setLogRecordFactory, self.factory)
        self.context = _StepContext()

    def _assert_detached(self) -> None:
        """Check `logging` is left as the step's process had it."""
        self.assertEqual(logging.getLogger().handlers, self.handlers)
        self.assertEqual(self.context.log.handlers, [])
        self.assertIs(logging.getLogRecordFactory(), self.factory)

    def test1(self) -> None:
        """
        Test that a failed step notes its own lines as they were written,
        a library's among them, and then its error, the cause first.
        """
        cause = _error("KeyError: 'amount'", "  File a.py, line 3\n    row[k]\n")
        error = _error(
            "ValueError: bad row",
            "  File dagster.py, line 1\n",
            "  File b.py, line 9\n    raise ValueError\n",
            cause=cause,
        )

        def body() -> None:
            self.context.report("STEP_START with a long stack")
            self.context.say("Running with dbt=1.8.7")
            logging.getLogger("snowflake.connector").warning("retrying")
            self.context.say("invalid identifier 'SALES_AMOUNT'")

        note = _noted(self.context, body, _failure(error))
        self.assertEqual(
            note["output_tail"].split("\n"),
            [
                "Running with dbt=1.8.7",
                "retrying",
                "invalid identifier 'SALES_AMOUNT'",
                "  File a.py, line 3",
                "    row[k]",
                "KeyError: 'amount'",
                "  File dagster.py, line 1",
                "  File b.py, line 9",
                "    raise ValueError",
                "ValueError: bad row",
            ],
        )
        self.assertNotIn("output_excluded", note)
        self._assert_detached()

    def test10(self) -> None:
        """
        Test that a line written to one of Dagster's named loggers is held
        in its place, and the copy the log manager makes of each line for
        a job's own loggers is not held again.
        """
        configured = logging.Logger("dagster")

        def body() -> None:
            self.context.say("first")
            meta = {"orig_message": "first", "dagster_event": None}
            configured.info(
                "orders - 1a2b - first", extra={"dagster_meta": meta}
            )
            logging.getLogger("dagster.builtin").warning("to a named logger")
            self.context.say("third")

        note = _noted(self.context, body, _failure(_error("ValueError: x")))
        self.assertEqual(
            note["output_tail"].split("\n"),
            ["first", "to a named logger", "third", "ValueError: x"],
        )

    def test2(self) -> None:
        """
        Test that a step that succeeds notes none of what it logged, and
        the ring comes off.
        """
        written = _step(self.context, lambda: self.context.say("all is well"))
        self.assertEqual(written, [])
        self._assert_detached()

    def test3(self) -> None:
        """
        Test that an exception that leaves Dagster's own code is a failure
        too, noted by its class and message, and is raised untouched.
        """
        written = _step(
            self.context,
            lambda: self.context.say("about to be interrupted"),
            raises=KeyboardInterrupt("stop"),
        )
        self.assertEqual(
            written[0]["output_tail"],
            "about to be interrupted\nKeyboardInterrupt: stop",
        )
        self._assert_detached()

    def test4(self) -> None:
        """
        Test that the end is kept, within both limits, and the cut is
        declared in the note.
        """

        def body() -> None:
            for i in range(500):
                self.context.say(f"line {i:04d} " + "x" * 150)

        note = _noted(
            self.context, body, _failure(_error("ValueError: the last word"))
        )
        tail = note["output_tail"]
        lines = tail.split("\n")
        self.assertLessEqual(len(lines), ceoutput.MAX_LINES)
        self.assertLessEqual(len(tail.encode("utf-8")), ceoutput.MAX_BYTES)
        self.assertEqual(lines[-1], "ValueError: the last word")
        self.assertTrue(lines[-2].startswith("line 0499 "))
        self.assertEqual(
            note["output_excluded"],
            [
                {"path": "output_tail", "reason": "limited to 60 lines"},
                {"path": "output_tail", "reason": "limited to 4096 bytes"},
            ],
        )

    def test5(self) -> None:
        """
        Test that no seeded credential is anywhere in the note, the error's
        own message included.
        """

        def body() -> None:
            self.context.say("export DB_PASSWORD=hunter2")
            self.context.say(_PEM)

        error = _error(
            "OperationalError: could not connect to "
            "postgresql://svc:hunter2@db.internal/app"
        )
        note = _noted(self.context, body, _failure(error))
        whole = json.dumps(note)
        self.assertNotIn("hunter2", whole)
        self.assertNotIn("MIIEvQIBADANBgkqhkiG9w0BAQEFAASC", whole)
        self.assertIn("svc:***@db.internal", note["output_tail"])
        self.assertEqual(
            note["output_excluded"],
            [{"path": "output_tail", "reason": "credential masked"}],
        )

    def test6(self) -> None:
        """
        Test that nothing is held or noted with the switch off, under
        either of its names.
        """
        for name in (
            "CONVALESCE_SEND_OUTPUT",
            "CUSTOMER_CONVALESCE_SEND_OUTPUT",
        ):
            seen: List[Any] = []

            def body() -> None:
                seen.append(list(self.context.log.handlers))
                self.context.say("it failed")

            written = _step(
                self.context,
                body,
                _failure(_error("ValueError: x")),
                environ={**_ENV, name: "false"},
            )
            self.assertEqual(seen, [[]])
            self.assertEqual(written, [])

    def test7(self) -> None:
        """
        Test that a ring that breaks as a line arrives never raises into
        the step, whose events pass through and whose error still ends
        its note.
        """

        def body() -> None:
            self.context.say("lost")

        with unittest.mock.patch.object(
            ceoutput, "clip", side_effect=RuntimeError("password=hunter2")
        ):
            note = _noted(
                self.context, body, _failure(_error("ValueError: kept"))
            )
        self.assertEqual(note["output_tail"], "ValueError: kept")
        self._assert_detached()

    def test8(self) -> None:
        """
        Test that output that cannot be read costs the step its note and
        nothing else, and is logged by the error's class alone.
        """
        with (
            unittest.mock.patch.object(
                cedsteps,
                "error_lines",
                side_effect=RuntimeError("password=hunter2"),
            ),
            self.assertLogs("convalesce_emit_dagster.steps", "DEBUG") as logs,
        ):
            written = _step(
                self.context,
                lambda: self.context.say("x"),
                _failure(_error("ValueError: y")),
            )
        self.assertEqual(written, [])
        self.assertIn("RuntimeError", logs.output[0])
        self.assertNotIn("hunter2", "".join(logs.output))
        self._assert_detached()

    def test9(self) -> None:
        """
        Test that the bracket is put in for output alone.
        """
        module = types.ModuleType("execute_plan")

        def original(step_context: Any) -> Iterator[Any]:
            del step_context
            yield "STEP_SUCCESS"

        module.dagster_event_sequence_for_step = original  # type: ignore[attr-defined]
        off = {
            **_ENV,
            "CONVALESCE_SQL_CAPTURE": "false",
            "CONVALESCE_SEND_OUTPUT": "false",
        }
        on = {**off, "CONVALESCE_SEND_OUTPUT": "true"}
        for environ, expected in ((off, False), (on, True)):
            with (
                unittest.mock.patch.dict(os.environ, environ, clear=True),
                unittest.mock.patch.object(cedsteps, "_installed", False),
                unittest.mock.patch.object(
                    cedsteps.importlib, "import_module", return_value=module
                ),
                unittest.mock.patch.object(cesqlcap, "install"),
                unittest.mock.patch.object(cesqlcap, "ignore"),
            ):
                self.assertEqual(cedsteps.install(), expected)


# #############################################################################
# Test_error_lines1
# #############################################################################


class Test_error_lines1(unittest.TestCase):
    """
    Test how a step's error is written as lines.
    """

    def test1(self) -> None:
        """
        Test that only the innermost frames of each error are kept.
        """
        frames = [f"  File f{i}.py\n" for i in range(10)]
        lines = cedsteps.error_lines(_error("ValueError: x", *frames))
        self.assertEqual(
            lines,
            ["  File f7.py", "  File f8.py", "  File f9.py", "ValueError: x"],
        )

    def test2(self) -> None:
        """
        Test that an error that says nothing, and no error, is no lines.
        """
        self.assertEqual(cedsteps.error_lines(None), [])
        self.assertEqual(cedsteps.error_lines(object()), [])

    def test3(self) -> None:
        """
        Test that a chain of causes that never ends is followed only so
        far.
        """
        error = _error("ValueError: x")
        error.cause = error
        self.assertEqual(len(cedsteps.error_lines(error)), 5)


# #############################################################################
# Test_sensor_output1
# #############################################################################


def _note_event(step_key: str, note: Dict[str, Any]) -> Any:
    """One event-log record holding a step's note."""
    event = types.SimpleNamespace(
        step_key=step_key,
        event_specific_data=types.SimpleNamespace(
            metadata={cedsteps.NOTE_KEY: types.SimpleNamespace(data=note)}
        ),
    )
    return types.SimpleNamespace(
        event_log_entry=types.SimpleNamespace(dagster_event=event)
    )


class _Instance:
    """An instance whose event log holds the given notes."""

    def __init__(self, records: List[Any], statuses: Dict[str, str]) -> None:
        self.records = records
        self.statuses = statuses

    def get_records_for_run(self, run_id: str, of_type: Any = None) -> Any:
        """The run's records of one type."""
        del run_id, of_type
        return types.SimpleNamespace(records=self.records)

    def get_run_step_stats(self, run_id: str) -> List[Any]:
        """Each step and how it ended."""
        del run_id
        return [
            types.SimpleNamespace(
                step_key=key, status=types.SimpleNamespace(value=status)
            )
            for key, status in self.statuses.items()
        ]


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


def _sensed(records: List[Any], statuses: Dict[str, str]) -> Dict[str, Any]:
    """
    What the sensor sends for a run whose steps wrote these notes.

    :param records: the run's engine events
    :param statuses: each step's key to how its stats say it ended
    :return: the one observation, as the emitter was given it
    """
    context = types.SimpleNamespace(
        instance=_Instance(records, statuses),
        sensor_name="convalesce_on_failure",
        dagster_event={"event_type_value": "PIPELINE_FAILURE"},
        dagster_run=types.SimpleNamespace(
            run_id="1a2b", job_name="orders", run_config={}, tags={}
        ),
    )
    recorder = _Recorder()
    fake = types.SimpleNamespace(
        DagsterEventType=types.SimpleNamespace(ENGINE_EVENT="ENGINE_EVENT")
    )
    with (
        unittest.mock.patch.dict(os.environ, _ENV, clear=True),
        unittest.mock.patch.dict(sys.modules, {"dagster": fake}),
    ):
        cedsens.convalesce_sensor(context, emitter=recorder)
    return recorder.sent[0]


class Test_sensor_output1(unittest.TestCase):
    """
    Test that the sensor sends a failed step's output with the run.
    """

    def test1(self) -> None:
        """
        Test that the tail crosses on its step, and what was masked in it
        is declared by its path from the payload's root.
        """
        note = {
            "output_tail": "password=***\nValueError: bad row",
            "output_excluded": [
                {"path": "output_tail", "reason": "credential masked"}
            ],
        }
        sent = _sensed([_note_event("load", note)], {"load": "FAILURE"})
        self.assertEqual(
            sent["payload"]["steps"],
            [
                {
                    "step_key": "load",
                    "output_tail": "password=***\nValueError: bad row",
                }
            ],
        )
        self.assertIn(
            {"path": "steps[0].output_tail", "reason": "credential masked"},
            sent["excluded"],
        )

    def test2(self) -> None:
        """
        Test that a step that failed and then succeeded on another try
        sends no tail, whether or not the try that succeeded wrote a note.
        """
        failed = {"output_tail": "ValueError: first try"}
        for later in ([], [_note_event("load", {"config": {"a": 1}})]):
            sent = _sensed(
                [_note_event("load", failed)] + later, {"load": "SUCCESS"}
            )
            for step in sent["payload"].get("steps", []):
                self.assertNotIn("output_tail", step)
            self.assertNotIn("first try", json.dumps(sent, default=str))

    def test3(self) -> None:
        """
        Test that a step that failed on every try sends its last try's.
        """
        sent = _sensed(
            [
                _note_event("load", {"output_tail": "ValueError: first"}),
                _note_event("load", {"output_tail": "ValueError: second"}),
            ],
            {"load": "FAILURE"},
        )
        self.assertEqual(
            sent["payload"]["steps"][0]["output_tail"], "ValueError: second"
        )

"""
Tests for sending the last lines a failed task logged.

Run with `make test`.
"""

import json
import logging
import os
import sys
import types
import unittest
import unittest.mock
from typing import Any, Callable, Dict, List, Optional

# The tests look at the module's own note of the ring on a running task.
# pylint: disable=protected-access

import convalesce_emit.output as ceoutput
import convalesce_emit_airflow.listener as cealist
import convalesce_emit_airflow.runsettings as cealruns

_LOG = logging.getLogger(__name__)

_RUNNING = cealist._RUNNING
_FAILED = cealist._FAILED
_SUCCESS = "on_task_instance_success"
_SPEC = {
    _RUNNING: ("previous_state", "task_instance", "session"),
    _SUCCESS: ("previous_state", "task_instance", "session"),
    _FAILED: ("previous_state", "task_instance", "error", "session"),
}
# Where Airflow 2 hands a task's and a command's lines to `logging`.
_TASK_LOGGER = "airflow.task.operators"
_HOOK_LOGGER = "airflow.hooks.subprocess.SubprocessHook"
_PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n"
    "-----END PRIVATE KEY-----"
)


# #############################################################################
# _Recorder / _TaskInstance
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


class _TaskInstance:
    """Stands in for the task instance Airflow passes."""

    def __init__(self) -> None:
        self.task_id = "build"
        self.dag_id = "orders"


def _command() -> None:
    """What a failing `dbt build` under a `BashOperator` logs."""
    log = logging.getLogger(_HOOK_LOGGER)
    log.info("Running with dbt=1.8.7")
    log.info("Database Error in model stg_orders")
    log.info("  invalid identifier 'SALES_AMOUNT'")
    logging.getLogger(_TASK_LOGGER).error(
        "Command exited with return code %d", 1
    )


def _run(
    event: str,
    task: Callable[[], None] = _command,
    environ: Optional[Dict[str, str]] = None,
    start: bool = True,
) -> Dict[str, Any]:
    """
    Run one task to its end and return what was sent when it ended.

    :param event: the hook the task ends with
    :param task: what the task logs while it runs
    :param environ: the environment the task's process has
    :param start: whether the task's start is seen by this process
    :return: the observation sent for its end
    """
    recorder = _Recorder()
    listener = cealist.build_listener_class(_SPEC)(emitter=recorder)
    task_instance = _TaskInstance()
    with (
        unittest.mock.patch.dict(os.environ, environ or {}, clear=True),
        unittest.mock.patch.object(cealist, "watch_sql"),
        unittest.mock.patch.object(cealruns, "watch_variables"),
    ):
        if start:
            getattr(listener, _RUNNING)(None, task_instance, None)
        task()
        if event == _FAILED:
            error = RuntimeError("Bash command failed.")
            getattr(listener, event)("running", task_instance, error, None)
        else:
            getattr(listener, event)("running", task_instance, None)
    return recorder.sent[-1]


def _output_paths(sent: Dict[str, Any]) -> List[Dict[str, str]]:
    """What an observation declares about its output."""
    return [
        item
        for item in sent["excluded"]
        if item["path"].startswith(ceoutput.FIELD)
    ]


# #############################################################################
# Test_output_tail1
# #############################################################################


class Test_output_tail1(unittest.TestCase):
    """
    Test that a failed task's last lines are sent, and only a failed one's.
    """

    def setUp(self) -> None:
        for name in ("", "airflow.task", _TASK_LOGGER, _HOOK_LOGGER):
            logger = logging.getLogger(name)
            level = logger.level
            self.addCleanup(logger.setLevel, level)
        logging.getLogger().setLevel(logging.INFO)
        self.handlers = list(logging.getLogger().handlers)
        self.factory = logging.getLogRecordFactory()
        self.addCleanup(logging.setLogRecordFactory, self.factory)
        self.addCleanup(cealist._OUTPUT.clear)

    def _assert_detached(self) -> None:
        """Check `logging` is left as the task's process had it."""
        self.assertEqual(cealist._OUTPUT, {})
        self.assertEqual(logging.getLogger().handlers, self.handlers)
        self.assertIs(logging.getLogRecordFactory(), self.factory)

    def test1(self) -> None:
        """
        Test that a failure carries what the task logged, in order.
        """
        sent = _run(_FAILED)
        self.assertEqual(
            sent["payload"]["output_tail"],
            "Running with dbt=1.8.7\n"
            "Database Error in model stg_orders\n"
            "  invalid identifier 'SALES_AMOUNT'\n"
            "Command exited with return code 1",
        )
        self.assertEqual(_output_paths(sent), [])
        self._assert_detached()

    def test2(self) -> None:
        """
        Test that a success carries none of it, and nothing is held on.
        """
        sent = _run(_SUCCESS)
        self.assertNotIn("output_tail", sent["payload"])
        self.assertEqual(_output_paths(sent), [])
        self._assert_detached()

    def test3(self) -> None:
        """
        Test that the end is kept, within both limits, and the cut is
        declared.
        """

        def task() -> None:
            log = logging.getLogger(_HOOK_LOGGER)
            for i in range(500):
                log.info("line %04d %s", i, "x" * 150)

        sent = _run(_FAILED, task)
        tail = sent["payload"]["output_tail"]
        lines = tail.split("\n")
        self.assertLessEqual(len(lines), ceoutput.MAX_LINES)
        self.assertLessEqual(len(tail.encode("utf-8")), ceoutput.MAX_BYTES)
        self.assertTrue(lines[-1].startswith("line 0499 "))
        self.assertTrue(lines[0].startswith("line 04"))
        self.assertEqual(
            [item["reason"] for item in _output_paths(sent)],
            ["limited to 60 lines", "limited to 4096 bytes"],
        )

    def test4(self) -> None:
        """
        Test that no seeded credential is anywhere in what is sent.
        """

        def task() -> None:
            log = logging.getLogger(_HOOK_LOGGER)
            log.info("export DB_PASSWORD=hunter2")
            log.info("connecting to postgresql://svc:hunter2@db.internal/app")
            for line in _PEM.split("\n"):
                log.info("%s", line)
            log.info("Database Error: invalid identifier 'SALES_AMOUNT'")

        sent = _run(_FAILED, task)
        whole = json.dumps(sent, default=str)
        self.assertNotIn("hunter2", whole)
        self.assertNotIn("MIIEvQIBADANBgkqhkiG9w0BAQEFAASC", whole)
        tail = sent["payload"]["output_tail"]
        self.assertIn("svc:***@db.internal", tail)
        self.assertIn("invalid identifier 'SALES_AMOUNT'", tail)
        self.assertEqual(
            _output_paths(sent),
            [{"path": "output_tail", "reason": "credential masked"}],
        )

    def test5(self) -> None:
        """
        Test that nothing is held or sent with the switch off, under
        either of its names.
        """
        for name in (
            "CONVALESCE_SEND_OUTPUT",
            "CUSTOMER_CONVALESCE_SEND_OUTPUT",
        ):
            seen: List[Any] = []

            def task() -> None:
                seen.append(cealist._OUTPUT.get("ring"))
                _command()

            sent = _run(_FAILED, task, {name: "false"})
            self.assertEqual(seen, [None])
            self.assertNotIn("output_tail", sent["payload"])
            self.assertEqual(_output_paths(sent), [])
            self._assert_detached()

    def test6(self) -> None:
        """
        Test that a failure this process never saw start, as a scheduler's
        of a task it found dead, is sent without a tail.
        """
        sent = _run(_FAILED, start=False)
        self.assertEqual(sent["event"], _FAILED)
        self.assertNotIn("output_tail", sent["payload"])

    def test7(self) -> None:
        """
        Test that a second task in one process starts with nothing held.
        """
        _run(_SUCCESS, lambda: logging.getLogger(_HOOK_LOGGER).info("first"))
        sent = _run(
            _FAILED, lambda: logging.getLogger(_HOOK_LOGGER).info("second")
        )
        self.assertEqual(sent["payload"]["output_tail"], "second")

    def test8(self) -> None:
        """
        Test that the plugin's own log lines are not part of the output.
        """

        def task() -> None:
            logging.getLogger("convalesce_emit.client").warning("ours")
            logging.getLogger(_HOOK_LOGGER).info("theirs")

        sent = _run(_FAILED, task)
        self.assertEqual(sent["payload"]["output_tail"], "theirs")


# #############################################################################
# Test_output_errors1
# #############################################################################


class Test_output_errors1(unittest.TestCase):
    """
    Test that nothing about the output can cost the task or its event.
    """

    def setUp(self) -> None:
        logger = logging.getLogger()
        self.addCleanup(logger.setLevel, logger.level)
        logger.setLevel(logging.INFO)
        self.addCleanup(cealist._OUTPUT.clear)

    def test1(self) -> None:
        """
        Test that a ring that breaks as a record arrives never raises into
        the task, and the failure is still sent.
        """
        with unittest.mock.patch.object(
            ceoutput, "clip", side_effect=RuntimeError("password=hunter2")
        ):
            sent = _run(_FAILED)
        self.assertEqual(sent["event"], _FAILED)
        self.assertNotIn("output_tail", sent["payload"])

    def test2(self) -> None:
        """
        Test that a ring that cannot be read is logged by the error's
        class alone, and the failure is still sent.
        """
        with (
            unittest.mock.patch.object(
                ceoutput.Ring,
                "lines",
                side_effect=RuntimeError("password=hunter2"),
            ),
            self.assertLogs(cealist._LOG, "WARNING") as logs,
        ):
            sent = _run(_FAILED)
        self.assertEqual(sent["event"], _FAILED)
        self.assertNotIn("output_tail", sent["payload"])
        self.assertIn("RuntimeError", logs.output[0])
        self.assertNotIn("hunter2", "".join(logs.output))

    def test3(self) -> None:
        """
        Test that a ring that cannot be put in place is logged by the
        error's class alone, and the task's events are still sent.
        """
        with (
            unittest.mock.patch.object(
                ceoutput, "watch", side_effect=RuntimeError("password=hunter2")
            ),
            self.assertLogs(cealist._LOG, "WARNING") as logs,
        ):
            sent = _run(_FAILED)
        self.assertEqual(sent["event"], _FAILED)
        self.assertNotIn("output_tail", sent["payload"])
        self.assertNotIn("hunter2", "".join(logs.output))


# #############################################################################
# Test_output_known_secrets1
# #############################################################################


class Test_output_known_secrets1(unittest.TestCase):
    """
    Test the credentials only Airflow knows, which no pattern can find.
    """

    def setUp(self) -> None:
        logger = logging.getLogger()
        self.addCleanup(logger.setLevel, logger.level)
        logger.setLevel(logging.INFO)
        self.addCleanup(cealist._OUTPUT.clear)
        self.addCleanup(cealist._airflow_redact.cache_clear)
        cealist._airflow_redact.cache_clear()

    def _masker(self, redact: Any) -> Any:
        """Stand in for the module Airflow keeps its masker in."""
        module = types.ModuleType(cealist._MASKER_MODULES[-1])
        module.redact = redact  # type: ignore[attr-defined]
        return unittest.mock.patch.dict(
            sys.modules, {cealist._MASKER_MODULES[-1]: module}
        )

    def test1(self) -> None:
        """
        Test that a connection's password logged bare is masked as
        Airflow's own task log masks it, and that this is declared.
        """

        def task() -> None:
            logging.getLogger(_HOOK_LOGGER).info("psql -W wh-pass-91 -c x")

        with self._masker(lambda line: line.replace("wh-pass-91", "***")):
            sent = _run(_FAILED, task)
        self.assertEqual(sent["payload"]["output_tail"], "psql -W *** -c x")
        self.assertNotIn("wh-pass-91", json.dumps(sent, default=str))
        self.assertEqual(
            _output_paths(sent),
            [{"path": "output_tail", "reason": "credential masked"}],
        )

    def test2(self) -> None:
        """
        Test that a line Airflow's masker and the core's rules both
        change is declared once.
        """

        def task() -> None:
            logging.getLogger(_HOOK_LOGGER).info("wh-pass-91 token=abc")

        with self._masker(lambda line: line.replace("wh-pass-91", "***")):
            sent = _run(_FAILED, task)
        self.assertEqual(sent["payload"]["output_tail"], "*** token=***")
        self.assertEqual(len(_output_paths(sent)), 1)

    def test3(self) -> None:
        """
        Test that where Airflow's masker breaks, the failure is sent
        without the lines it could not vouch for.
        """

        def broken(line: str) -> str:
            raise RuntimeError(line)

        with (
            self._masker(broken),
            self.assertLogs(cealist._LOG, "WARNING") as logs,
        ):
            sent = _run(_FAILED)
        self.assertEqual(sent["event"], _FAILED)
        self.assertNotIn("output_tail", sent["payload"])
        self.assertIn("RuntimeError", logs.output[0])
        self.assertNotIn("dbt", "".join(logs.output))

    def test4(self) -> None:
        """
        Test that without Airflow's masker the core's rules still apply.
        """
        self.assertIsNone(cealist._airflow_redact())
        sent = _run(
            _FAILED, lambda: logging.getLogger(_HOOK_LOGGER).info("pwd=abc")
        )
        self.assertEqual(sent["payload"]["output_tail"], "pwd=***")


# #############################################################################
# Test_output_events1
# #############################################################################


class Test_output_events1(unittest.TestCase):
    """
    Test the lines a task says through `structlog`, as on Airflow 3.1.
    """

    def setUp(self) -> None:
        self.processors: List[Any] = [lambda _l, _m, e: e, self._render]
        module = types.ModuleType("structlog")
        module.is_configured = lambda: True  # type: ignore[attr-defined]
        module.get_config = lambda: {  # type: ignore[attr-defined]
            "processors": self.processors
        }
        patch = unittest.mock.patch.dict(sys.modules, {"structlog": module})
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(cealist._OUTPUT.clear)
        self.rendered: List[str] = []

    def _render(self, _logger: Any, _method: str, event: Any) -> str:
        """Stand in for the processor that renders, always the last."""
        self.rendered.append(event["event"])
        return str(event["event"])

    def _log(self, text: str) -> None:
        """Run an event through the processors, as a bound logger does."""
        event: Any = {"event": text, "level": "info"}
        for processor in list(self.processors):
            event = processor(None, "info", event)

    def test1(self) -> None:
        """
        Test that an event is held, rendered as before, and that the
        processor is gone once the task has ended.
        """
        before = list(self.processors)
        sent = _run(
            _FAILED, lambda: self._log("invalid identifier 'SALES_AMOUNT'")
        )
        self.assertEqual(
            sent["payload"]["output_tail"], "invalid identifier 'SALES_AMOUNT'"
        )
        self.assertEqual(self.rendered, ["invalid identifier 'SALES_AMOUNT'"])
        self.assertEqual(self.processors, before)

    def test2(self) -> None:
        """
        Test that the processor sits just before the one that renders.
        """
        seen: List[List[Any]] = []
        _run(_SUCCESS, lambda: seen.append(list(self.processors)))
        self.assertEqual(len(seen[0]), 3)
        self.assertIs(seen[0][1], cealist._hold_event)
        self.assertEqual(seen[0][2], self._render)

    def test3(self) -> None:
        """
        Test that an event that is not text passes through untouched.
        """
        event = {"event": 7}
        self.assertIs(cealist._hold_event(None, "info", event), event)
        self.assertIs(cealist._hold_event(None, "info", None), None)

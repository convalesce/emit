"""
Tests for sending the last lines a failed run logged.

Run with `make test`.
"""

import json
import logging
import logging.config
import os
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest import mock

import convalesce_emit.output as ceoutput
import convalesce_emit_prefect._output as ceout
import convalesce_emit_prefect.hooks as cephooks

# Each plugin proves the same thing of its own event: the tail is sent
# with a failure and no other event, within its limits and masked. The
# plugins are independently installable with no dependency on each other,
# so sharing a test module between them would add one for no real benefit;
# the similarity stays and the check is turned off here rather than
# everywhere.
# pylint: disable=duplicate-code

_ENV = {"CONVALESCE_PREFECT_API_READS": "false"}
_FLOW_RUN = "0c1d2e3f-flow"
_TASK_RUN = "4a5b6c7d-task"
_OTHER_TASK_RUN = "8e9f0a1b-task"
_PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7\n"
    "-----END PRIVATE KEY-----"
)


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.config = types.SimpleNamespace(ingest_key=None)
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


class _State:
    """Stands in for a Prefect state, which names its type by an enum."""

    def __init__(self, kind: str) -> None:
        self.type = types.SimpleNamespace(value=kind)
        self.name = kind.title()


class _Run:
    """Stands in for a flow run or a task run."""

    def __init__(self, run_id: str) -> None:
        self.id = run_id
        self.name = "helpful-marmot"


def _task_says(text: str, task_run: str = _TASK_RUN) -> None:
    """Log one line as Prefect's logger for a task run does."""
    logging.getLogger("prefect.task_runs").info(
        text, extra={"flow_run_id": _FLOW_RUN, "task_run_id": task_run}
    )


def _flow_says(text: str) -> None:
    """Log one line as Prefect's logger for a flow run does."""
    logging.getLogger("prefect.flow_runs").info(
        text, extra={"flow_run_id": _FLOW_RUN}
    )


def _output_paths(sent: Dict[str, Any]) -> List[Dict[str, str]]:
    """What an observation declares about its output."""
    return [
        item
        for item in sent["excluded"]
        if item["path"].startswith(ceoutput.FIELD)
    ]


class _Case(unittest.TestCase):
    """
    A test in a process whose runs are being listened to.
    """

    env: Dict[str, str] = _ENV

    def setUp(self) -> None:
        """
        Set the environment a flow would run in, and start with nothing
        held.
        """
        patcher = mock.patch.dict(os.environ, self.env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in ceout.LOGGERS:
            logger = logging.getLogger(name)
            self.addCleanup(logger.setLevel, logger.level)
            logger.setLevel(logging.INFO)
        ceout.unwatch()
        ceout.watch()
        self.addCleanup(ceout.unwatch)

    def task_event(
        self, kind: str = "FAILED", run_id: Optional[str] = _TASK_RUN
    ) -> Dict[str, Any]:
        """
        What the task hook sends for a run that moved to a state.

        :param kind: the state's type
        :param run_id: the task run's id
        :return: the one observation, as the emitter was given it
        """
        recorder = _Recorder()
        with mock.patch.object(cephooks, "running_flow", return_value={}):
            cephooks.emit_task_run(
                task="T",
                task_run=_Run(run_id) if run_id else None,
                state=_State(kind),
                emitter=recorder,
            )
        return recorder.sent[0]

    def flow_event(self, kind: str = "FAILED") -> Dict[str, Any]:
        """
        What the flow hook sends for a run that moved to a state.

        :param kind: the state's type
        :return: the one observation, as the emitter was given it
        """
        recorder = _Recorder()
        cephooks.emit_flow_run(
            flow="F",
            flow_run=_Run(_FLOW_RUN),
            state=_State(kind),
            emitter=recorder,
        )
        return recorder.sent[0]


# #############################################################################
# Test_output_tail1
# #############################################################################


class Test_output_tail1(_Case):
    """
    Test that a failed run's last lines are sent, and only a failed run's.
    """

    def test1(self) -> None:
        """
        Test that a failed task carries its own lines, in order, and none
        of its neighbour's or its flow's.
        """
        _flow_says("flow began")
        _task_says("Running with dbt=1.8.7")
        _task_says("a neighbour's line", _OTHER_TASK_RUN)
        _task_says("invalid identifier 'SALES_AMOUNT'")
        sent = self.task_event()
        self.assertEqual(
            sent["payload"]["output_tail"],
            "Running with dbt=1.8.7\ninvalid identifier 'SALES_AMOUNT'",
        )
        self.assertEqual(_output_paths(sent), [])

    def test2(self) -> None:
        """
        Test that a failed flow carries what the flow itself logged.
        """
        _flow_says("flow began")
        _task_says("a task's line")
        _flow_says("Encountered exception during execution")
        sent = self.flow_event()
        self.assertEqual(
            sent["payload"]["output_tail"],
            "flow began\nEncountered exception during execution",
        )

    def test3(self) -> None:
        """
        Test that a crashed run carries its lines too.
        """
        _task_says("killed by the platform")
        sent = self.task_event("CRASHED")
        self.assertEqual(
            sent["payload"]["output_tail"], "killed by the platform"
        )

    def test4(self) -> None:
        """
        Test that no other state carries any, whatever was logged.
        """
        _task_says("all is well")
        _flow_says("all is well")
        for kind in ("COMPLETED", "RUNNING", "CANCELLED", "PENDING"):
            for sent in (self.task_event(kind), self.flow_event(kind)):
                self.assertNotIn("output_tail", sent["payload"], kind)
                self.assertEqual(_output_paths(sent), [], kind)

    def test5(self) -> None:
        """
        Test that the end is kept, within both limits, and the cut is
        declared.
        """
        for i in range(500):
            _task_says(f"line {i:04d} " + "x" * 150)
        sent = self.task_event()
        tail = sent["payload"]["output_tail"]
        lines = tail.split("\n")
        self.assertLessEqual(len(lines), ceoutput.MAX_LINES)
        self.assertLessEqual(len(tail.encode("utf-8")), ceoutput.MAX_BYTES)
        self.assertTrue(lines[-1].startswith("line 0499 "))
        self.assertEqual(
            [item["reason"] for item in _output_paths(sent)],
            ["limited to 60 lines", "limited to 4096 bytes"],
        )

    def test6(self) -> None:
        """
        Test that no seeded credential is anywhere in what is sent.
        """
        _task_says("export DB_PASSWORD=hunter2")
        _task_says("connecting to postgresql://svc:hunter2@db.internal/app")
        _task_says(_PEM)
        _task_says("invalid identifier 'SALES_AMOUNT'")
        sent = self.task_event()
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

    def test7(self) -> None:
        """
        Test that a failed run that logged nothing, or that has no id,
        sends no tail.
        """
        self.assertNotIn("output_tail", self.task_event()["payload"])
        _task_says("somebody's line")
        sent = self.task_event(run_id=None)
        self.assertNotIn("output_tail", sent["payload"])


# #############################################################################
# Test_output_switch1
# #############################################################################


class Test_output_switch1(_Case):
    """
    Test that the switch turns all of it off.
    """

    env = {**_ENV, "CONVALESCE_SEND_OUTPUT": "false"}

    def test1(self) -> None:
        """
        Test that nothing listens and nothing is sent.
        """
        for name in ceout.LOGGERS:
            self.assertEqual(logging.getLogger(name).filters, [])
        _task_says("it failed")
        sent = self.task_event()
        self.assertNotIn("output_tail", sent["payload"])
        self.assertEqual(_output_paths(sent), [])

    def test2(self) -> None:
        """
        Test that lines held before the switch was turned send nothing.
        """
        with mock.patch.dict(os.environ, _ENV, clear=True):
            ceout.watch()
            _task_says("it failed")
        sent = self.task_event()
        self.assertNotIn("output_tail", sent["payload"])


class Test_output_switch2(_Case):
    """
    Test the switch under the name a platform gives it.
    """

    env = {**_ENV, "CUSTOMER_CONVALESCE_SEND_OUTPUT": "false"}

    def test1(self) -> None:
        """
        Test that the prefixed name turns it off too.
        """
        _task_says("it failed")
        self.assertNotIn("output_tail", self.task_event()["payload"])


# #############################################################################
# Test_output_listening1
# #############################################################################


class Test_output_listening1(_Case):
    """
    Test how the runs' loggers are listened to.
    """

    def test1(self) -> None:
        """
        Test that listening twice listens once, and stopping leaves the
        loggers as they were.
        """
        ceout.watch()
        for name in ceout.LOGGERS:
            self.assertEqual(len(logging.getLogger(name).filters), 1)
        ceout.unwatch()
        for name in ceout.LOGGERS:
            self.assertEqual(logging.getLogger(name).filters, [])

    def test2(self) -> None:
        """
        Test that a logging configuration applied afterwards, as Prefect
        applies its own when it is imported, does not stop the listening.
        """
        handlers = {
            name: list(logging.getLogger(name).handlers)
            for name in ceout.LOGGERS
        }
        for name, held in handlers.items():
            self.addCleanup(setattr, logging.getLogger(name), "handlers", held)
        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "handlers": {"null": {"class": "logging.NullHandler"}},
                "loggers": {
                    name: {"level": "INFO", "handlers": ["null"]}
                    for name in ceout.LOGGERS
                },
            }
        )
        _task_says("said after the configuration")
        self.assertEqual(
            self.task_event()["payload"]["output_tail"],
            "said after the configuration",
        )

    def test3(self) -> None:
        """
        Test that a record still reaches Prefect's own handlers, and its
        logger never raises, when holding it fails.
        """
        seen: List[str] = []

        class _Handler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                seen.append(record.getMessage())

        handler = _Handler()
        logger = logging.getLogger("prefect.task_runs")
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)
        with mock.patch.object(
            ceoutput, "clip", side_effect=RuntimeError("password=hunter2")
        ):
            _task_says("still logged")
        self.assertEqual(seen, ["still logged"])
        self.assertNotIn("output_tail", self.task_event()["payload"])

    def test4(self) -> None:
        """
        Test that lines that cannot be read are logged by the error's
        class alone, and the failure is still sent.
        """
        _task_says("it failed")
        with (
            mock.patch.object(
                ceout, "lines_of", side_effect=RuntimeError("password=hunter2")
            ),
            self.assertLogs(
                "convalesce_emit_prefect._output", "WARNING"
            ) as logs,
        ):
            sent = self.task_event()
        self.assertEqual(sent["event"], "task_run")
        self.assertNotIn("output_tail", sent["payload"])
        self.assertIn("RuntimeError", logs.output[0])
        self.assertNotIn("hunter2", "".join(logs.output))

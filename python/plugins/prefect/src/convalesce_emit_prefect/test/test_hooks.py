"""
Tests for the flow and task state hooks.

Run with `make test`.
"""

import logging
import sys
import unittest
from typing import Any, Dict, List
from unittest import mock

import convalesce_emit_prefect.hooks as cephooks

_LOG = logging.getLogger(__name__)


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


class _Flow:
    """Stands in for a Prefect flow."""

    def __init__(self) -> None:
        self.name = "nightly"
        self.description = "The nightly flow."


class _FlowRun:
    """Stands in for a Prefect flow run."""

    def __init__(self) -> None:
        self.id = "a64690f5"
        self.name = "helpful-marmot"


class _TaskRun:
    """Stands in for a Prefect task run, which names its flow run by id."""

    def __init__(self) -> None:
        self.name = "extract-12f"
        self.flow_run_id = "a64690f5"
        self.state_name = "Completed"


class _ContextModule:
    """A stand-in `prefect.context` whose `get` answers what it was given."""

    def __init__(self, context: Any) -> None:
        class _FlowRunContext:
            """Stands in for Prefect's own run context."""

            @staticmethod
            def get() -> Any:
                """The running flow's context, or None outside one."""
                return context

        # Named as Prefect names it, which is what the hook looks up.
        setattr(self, "FlowRunContext", _FlowRunContext)


class _Context:
    """Stands in for a live FlowRunContext."""

    def __init__(self) -> None:
        self.flow = _Flow()
        self.flow_run = _FlowRun()


# #############################################################################
# Test_emit_task_run1
# #############################################################################


class Test_emit_task_run1(unittest.TestCase):
    """
    Test that a task event names the flow it belongs to.
    """

    def test1(self) -> None:
        """
        Test that the flow and flow run come off the run context.

        A task run names its flow run by id and nothing else, and the
        flow-run hook that carries the name fires last, after every task
        hook, so a task run on its own said nothing about its pipeline.
        """
        recorder = _Recorder()
        module = _ContextModule(_Context())
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            cephooks.emit_task_run(
                task="T", task_run=_TaskRun(), state="S", emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["flow"]["name"], "nightly")
        self.assertEqual(payload["flow_run"]["name"], "helpful-marmot")
        self.assertEqual(payload["task_run"]["name"], "extract-12f")

    def test2(self) -> None:
        """
        Test that a hook outside a flow run still sends the task run.
        """
        recorder = _Recorder()
        module = _ContextModule(None)
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            cephooks.emit_task_run(
                task="T", task_run=_TaskRun(), state="S", emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["task_run"]["name"], "extract-12f")
        self.assertNotIn("flow", payload)

    def test3(self) -> None:
        """
        Test that what Prefect passed wins over what the context holds.
        """
        recorder = _Recorder()
        module = _ContextModule(_Context())
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            cephooks.emit_task_run(
                task="T",
                task_run=_TaskRun(),
                state="S",
                emitter=recorder,
                flow={"name": "passed-in"},
            )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["flow"]["name"], "passed-in")

    def test4(self) -> None:
        """
        Test that no Prefect at all is not a failure.

        The package does not depend on Prefect, and this module has to
        import and run where it is absent.
        """
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            cephooks.emit_task_run(
                task="T", task_run=_TaskRun(), state="S", emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["task_run"]["name"], "extract-12f")


# #############################################################################
# Test_emit_flow_run1
# #############################################################################


class Test_emit_flow_run1(unittest.TestCase):
    """
    Test that a flow event forwards what Prefect handed it.
    """

    def test1(self) -> None:
        """
        Test that the flow, its run and the state all cross.
        """
        recorder = _Recorder()
        cephooks.emit_flow_run(
            flow=_Flow(), flow_run=_FlowRun(), state="S", emitter=recorder
        )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["flow"]["name"], "nightly")
        self.assertEqual(payload["flow_run"]["id"], "a64690f5")
        self.assertEqual(payload["state"], "S")

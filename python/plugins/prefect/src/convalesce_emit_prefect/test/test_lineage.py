"""
Tests for the lineage a task declares, and the hook that sends it.

Run with `make test`.
"""

import logging
import sys
import types
import unittest
from typing import Any, Dict, List
from unittest import mock

import convalesce_emit_prefect as ceprefec
import convalesce_emit_prefect._lineage as celin
import convalesce_emit_prefect.hooks as cephooks

_LOG = logging.getLogger(__name__)

_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t,PROD)"


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


def _context_module(task_run_id: Any) -> types.SimpleNamespace:
    """
    A stand-in `prefect.context` running the task run `task_run_id`.

    :param task_run_id: the running task run's id, or None outside one
    :return: the module
    """
    context = (
        None
        if task_run_id is None
        else types.SimpleNamespace(
            task_run=types.SimpleNamespace(id=task_run_id)
        )
    )
    return types.SimpleNamespace(
        TaskRunContext=types.SimpleNamespace(get=lambda: context),
        FlowRunContext=types.SimpleNamespace(get=lambda: None),
    )


# #############################################################################
# Test_lineage1
# #############################################################################


class Test_lineage1(unittest.TestCase):
    """
    Test that a task's declared lineage reaches its task-run hook.
    """

    def setUp(self) -> None:
        celin._PENDING.clear()  # pylint: disable=protected-access

    def test1(self) -> None:
        """
        Test that declared datasets are sent as the payload's `lineage`,
        urns as given and mappings with `env` defaulted.
        """
        recorder = _Recorder()
        module = _context_module("tr-1")
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            ceprefec.lineage(
                inputs=[{"platform": "postgres", "name": "shop.orders"}],
                outputs=[_URN],
            )
            cephooks.emit_task_run(
                task="T",
                task_run=types.SimpleNamespace(id="tr-1"),
                state="S",
                emitter=recorder,
            )
        self.assertEqual(
            recorder.sent[0]["payload"]["lineage"],
            {
                "inputs": [
                    {
                        "platform": "postgres",
                        "name": "shop.orders",
                        "env": "PROD",
                    }
                ],
                "outputs": [_URN],
            },
        )

    def test2(self) -> None:
        """
        Test that repeated declarations add up, each dataset once, and are
        forgotten once sent.
        """
        module = _context_module("tr-2")
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            ceprefec.lineage(inputs=[_URN])
            ceprefec.lineage(
                inputs=[_URN],
                outputs=[{"platform": "s3", "name": "b/k", "env": "DEV"}],
            )
            task_run = types.SimpleNamespace(id="tr-2")
            first = celin.take(task_run)
            second = celin.take(task_run)
        self.assertEqual(
            first,
            {
                "inputs": [_URN],
                "outputs": [{"platform": "s3", "name": "b/k", "env": "DEV"}],
            },
        )
        self.assertIsNone(second)

    def test3(self) -> None:
        """
        Test that a malformed dataset is dropped rather than raised.
        """
        module = _context_module("tr-3")
        malformed: List[Any] = [{"platform": "pg"}, None, "", 3, {"name": "x"}]
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            ceprefec.lineage(inputs=malformed, outputs=[_URN])
            declared = celin.take(types.SimpleNamespace(id="tr-3"))
        self.assertEqual(declared, {"inputs": [], "outputs": [_URN]})

    def test4(self) -> None:
        """
        Test that a declaration outside a task run, or with no Prefect at
        all, does nothing and a task run that declared nothing sends no
        `lineage`.
        """
        recorder = _Recorder()
        with mock.patch.dict(
            sys.modules, {"prefect.context": _context_module(None)}
        ):
            ceprefec.lineage(inputs=[_URN])
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            ceprefec.lineage(inputs=[_URN])
            cephooks.emit_task_run(
                task="T",
                task_run=types.SimpleNamespace(id="tr-4"),
                state="S",
                emitter=recorder,
            )
        self.assertEqual(celin._PENDING, {})  # pylint: disable=protected-access
        self.assertNotIn("lineage", recorder.sent[0]["payload"])

    def test5(self) -> None:
        """
        Test that declarations whose hook never fires are not held forever.
        """
        limit = celin._MAX_PENDING  # pylint: disable=protected-access
        for i in range(limit + 5):
            module = _context_module(f"tr-{i}")
            with mock.patch.dict(sys.modules, {"prefect.context": module}):
                ceprefec.lineage(inputs=[_URN])
        pending = celin._PENDING  # pylint: disable=protected-access
        self.assertEqual(len(pending), limit)
        self.assertNotIn("tr-0", pending)

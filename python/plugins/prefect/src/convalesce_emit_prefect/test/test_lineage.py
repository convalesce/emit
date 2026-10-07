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

    def test6(self) -> None:
        """
        Test that a dataset's platform instance is sent with it, and that
        one that is not a name is left out.
        """
        module = _context_module("tr-6")
        orders = {
            "platform": "postgres",
            "name": "shop.orders",
            "platform_instance": "replica",
        }
        totals = {"platform": "postgres", "name": "shop.totals"}
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            ceprefec.lineage(
                inputs=[orders],
                outputs=[{**totals, "platform_instance": 3}],
            )
            declared = celin.take(types.SimpleNamespace(id="tr-6"))
        self.assertEqual(
            declared,
            {
                "inputs": [{**orders, "env": "PROD"}],
                "outputs": [{**totals, "env": "PROD"}],
            },
        )


# #############################################################################
# Test_launched1
# #############################################################################


_GLUE_RUN_ID = "jr_" + "0123456789abcdef" * 4


class Test_launched1(unittest.TestCase):
    """
    Test that a run a task started elsewhere reaches its task-run hook.
    """

    def setUp(self) -> None:
        celin._LAUNCHED.clear()  # pylint: disable=protected-access

    def test1(self) -> None:
        """
        Test that declared runs are sent as the payload's `launched`, in
        order, each once.
        """
        recorder = _Recorder()
        module = _context_module("tr-l1")
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            ceprefec.launched("glue", _GLUE_RUN_ID, job="lake_daily_agg")
            ceprefec.launched("glue", _GLUE_RUN_ID, job="lake_daily_agg")
            ceprefec.launched("databricks", "1234")
            cephooks.emit_task_run(
                task="T",
                task_run=types.SimpleNamespace(id="tr-l1"),
                state="S",
                emitter=recorder,
            )
        self.assertEqual(
            recorder.sent[0]["payload"]["launched"],
            [
                {
                    "platform": "glue",
                    "run_id": _GLUE_RUN_ID,
                    "job": "lake_daily_agg",
                },
                {"platform": "databricks", "run_id": "1234", "job": ""},
            ],
        )
        self.assertIsNone(celin.take_launched(types.SimpleNamespace(id="tr-l1")))

    def test2(self) -> None:
        """
        Test that a task run keeps at most 20 runs.
        """
        module = _context_module("tr-l2")
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            for i in range(25):
                ceprefec.launched("glue", f"jr_{i}")
            declared = celin.take_launched(types.SimpleNamespace(id="tr-l2"))
        assert declared is not None
        self.assertEqual(len(declared), 20)
        self.assertEqual(declared[-1]["run_id"], "jr_19")

    def test3(self) -> None:
        """
        Test that a declaration outside a task run, or with no Prefect at
        all, does nothing and a task run that declared none sends no
        `launched`.
        """
        recorder = _Recorder()
        with mock.patch.dict(
            sys.modules, {"prefect.context": _context_module(None)}
        ):
            ceprefec.launched("glue", _GLUE_RUN_ID)
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            ceprefec.launched("glue", _GLUE_RUN_ID)
            cephooks.emit_task_run(
                task="T",
                task_run=types.SimpleNamespace(id="tr-l3"),
                state="S",
                emitter=recorder,
            )
        self.assertEqual(celin._LAUNCHED, {})  # pylint: disable=protected-access
        self.assertNotIn("launched", recorder.sent[0]["payload"])

    def test4(self) -> None:
        """
        Test that an invalid platform, run id or job is ignored rather than
        raised.
        """
        invalid: List[Any] = [
            ("", _GLUE_RUN_ID, ""),
            ("glue", "", ""),
            (None, _GLUE_RUN_ID, ""),
            ("glue", 1234, ""),
            ("g" * 201, _GLUE_RUN_ID, ""),
            ("glue", "j" * 201, ""),
            ("glue", _GLUE_RUN_ID, None),
            ("glue", _GLUE_RUN_ID, "j" * 201),
        ]
        module = _context_module("tr-l4")
        with mock.patch.dict(sys.modules, {"prefect.context": module}):
            for platform, run_id, job in invalid:
                ceprefec.launched(platform, run_id, job=job)
            ceprefec.launched("g" * 200, "j" * 200)
            declared = celin.take_launched(types.SimpleNamespace(id="tr-l4"))
        self.assertEqual(
            declared, [{"platform": "g" * 200, "run_id": "j" * 200, "job": ""}]
        )

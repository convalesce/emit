"""
Tests for attaching to Airflow's listener API safely.

Run with `make test`.
"""

import logging
import unittest
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

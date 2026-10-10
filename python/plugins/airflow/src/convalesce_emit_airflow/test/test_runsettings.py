"""
Tests for sending the settings a task ran with.

Run with `make test`.
"""

import json
import logging
import os
import sys
import types
import unittest
import unittest.mock
from typing import Any, Dict, List

# The tests reset the module's own note of a started task between cases.
# pylint: disable=protected-access

import convalesce_emit.settings as cesettin
import convalesce_emit_airflow.listener as cealist
import convalesce_emit_airflow.runsettings as cealruns

_LOG = logging.getLogger(__name__)

_RUNNING = cealist._RUNNING
_SUCCESS = "on_task_instance_success"
_SPEC = {
    _RUNNING: ("previous_state", "task_instance", "session"),
    _SUCCESS: ("previous_state", "task_instance", "session"),
}
_INGEST = "ingest-key-0123456789"
_ON = {
    "CONVALESCE_SEND_SETTINGS": "true",
    "STAGE": "prod",
    "DB_PASSWORD": "hunter2-not-sent",
}


# #############################################################################
# _Config / _Recorder / _Task / _TaskInstance
# #############################################################################


class _Config:
    """Stands in for an emitter's configuration."""

    ingest_key = _INGEST


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.config = _Config()
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


class _Task:
    """Stands in for a Bash operator whose command was rendered."""

    def __init__(self) -> None:
        self.task_id = "load"
        self.template_fields = ("bash_command", "env")
        self.bash_command = "load --region rendered-from-a-variable"
        self.env = {"REGION": "rendered-too"}
        self.retries = 2


class _TaskInstance:
    """Stands in for the task instance Airflow passes."""

    def __init__(self) -> None:
        self.task_id = "load"
        self.dag_id = "orders"
        self.task = _Task()
        self.rendered_map_index = "rendered-index"


def _run(environ: Dict[str, str]) -> Dict[str, Any]:
    """
    Run one task to success and return what was sent when it ended.

    :param environ: the environment the task's process has
    :return: the observation sent for its success
    """
    recorder = _Recorder()
    listener = cealist.build_listener_class(_SPEC)(emitter=recorder)
    task_instance = _TaskInstance()
    with (
        unittest.mock.patch.dict(os.environ, environ, clear=True),
        unittest.mock.patch.object(cealist, "watch_sql"),
        unittest.mock.patch.object(cealruns, "watch_variables"),
        unittest.mock.patch.object(
            cealist, "connection_coordinates", return_value={}
        ),
    ):
        getattr(listener, _RUNNING)(None, task_instance, None)
        cesettin.note(cesettin.VARIABLE, "region", "eu-west-1")
        cesettin.note(cesettin.VARIABLE, "api_token", "tok-not-sent")
        getattr(listener, _SUCCESS)("running", task_instance, None)
    return recorder.sent[-1]


# #############################################################################
# Test_listener_settings1
# #############################################################################


class Test_listener_settings1(unittest.TestCase):
    """
    Test that a task's end carries its settings, and no secret among them.
    """

    def setUp(self) -> None:
        self.addCleanup(cesettin.noted)
        self.addCleanup(cealruns._STARTED.clear)

    def test1(self) -> None:
        """
        Test that nothing is sent unless the setting is on.
        """
        sent = _run({"STAGE": "prod"})
        self.assertNotIn("settings", sent["payload"])

    def test2(self) -> None:
        """
        Test that ordinary settings carry values and secrets carry hashes.
        """
        sent = _run(_ON)
        items = {
            (item["kind"], item["name"]): item
            for item in sent["payload"]["settings"]["items"]
        }
        self.assertEqual(items[("environment", "STAGE")]["value"], "prod")
        self.assertEqual(items[("variable", "region")]["value"], "eu-west-1")
        for name in (("environment", "DB_PASSWORD"), ("variable", "api_token")):
            self.assertNotIn("value", items[name])
            self.assertRegex(items[name]["fingerprint"], "^[0-9a-f]{16}$")
        self.assertRegex(
            sent["payload"]["settings"]["keyed_by"], "^[0-9a-f]{16}$"
        )

    def test3(self) -> None:
        """
        Test that no secret, and no key, is anywhere in what was sent.
        """
        whole = json.dumps(_run(_ON), default=str)
        for secret in ("hunter2-not-sent", "tok-not-sent", _INGEST):
            self.assertNotIn(secret, whole)

    def test4(self) -> None:
        """
        Test that a process that started no task sends no settings.

        The scheduler fails a task it found dead from its own process, and
        the environment there is not the task's.
        """
        recorder = _Recorder()
        listener = cealist.build_listener_class(_SPEC)(emitter=recorder)
        with (
            unittest.mock.patch.dict(os.environ, _ON, clear=True),
            unittest.mock.patch.object(
                cealist, "connection_coordinates", return_value={}
            ),
        ):
            getattr(listener, _SUCCESS)("running", _TaskInstance(), None)
        self.assertNotIn("settings", recorder.sent[-1]["payload"])

    def test5(self) -> None:
        """
        Test that what one task read is not sent for the next.
        """
        _run(_ON)
        self.assertEqual(cesettin.noted(), {})


# #############################################################################
# Test_send_arguments1
# #############################################################################


class Test_send_arguments1(unittest.TestCase):
    """
    Test that switching arguments off withholds what templates rendered.
    """

    def setUp(self) -> None:
        self.addCleanup(cesettin.noted)
        self.addCleanup(cealruns._STARTED.clear)

    def test1(self) -> None:
        """
        Test that an operator's templated fields are sent by default.
        """
        task = _run({})["payload"]["task_instance"]["task"]
        self.assertEqual(
            task["bash_command"], "load --region rendered-from-a-variable"
        )

    def test2(self) -> None:
        """
        Test that they are withheld, and said to be, with arguments off.
        """
        sent = _run({"CONVALESCE_SEND_ARGUMENTS": "false"})
        dumped = sent["payload"]["task_instance"]
        self.assertNotIn("bash_command", dumped["task"])
        self.assertNotIn("env", dumped["task"])
        self.assertNotIn("rendered_map_index", dumped)
        self.assertEqual(dumped["task"]["retries"], 2)
        self.assertNotIn("rendered", json.dumps(sent["payload"], default=str))
        reasons = {e["path"]: e["reason"] for e in sent["excluded"]}
        self.assertEqual(
            reasons["task_instance.task.bash_command"], "arguments not sent"
        )

    def test3(self) -> None:
        """
        Test that every operator of the DAG is covered, not the task's alone.
        """
        other = {
            "template_fields": ["sql"],
            "sql": "select 'rendered'",
            "task_id": "other",
        }
        excluded: List[Dict[str, str]] = []
        out = cealist._without_templated(
            {"dag_run": {"dag": {"task_dict": {"other": other}}}}, "", excluded
        )
        self.assertEqual(
            out["dag_run"]["dag"]["task_dict"]["other"],
            {"template_fields": ["sql"], "task_id": "other"},
        )
        self.assertEqual(
            excluded,
            [
                {
                    "path": "dag_run.dag.task_dict.other.sql",
                    "reason": "arguments not sent",
                }
            ],
        )


# #############################################################################
# Test_watch_variables1
# #############################################################################


class Test_watch_variables1(unittest.TestCase):
    """
    Test that a Variable is noted as it is read.
    """

    def setUp(self) -> None:
        self.addCleanup(cesettin.noted)

    def _model(self) -> Any:
        """
        Put a stand-in for Airflow 2's Variable model where it is imported.

        :return: the stand-in class
        """

        class Variable:
            """Stands in for `airflow.models.variable.Variable`."""

            store = {"region": "eu-west-1", "parsed": {"b": 1, "a": 2}}

            @classmethod
            def get(cls, key: str, default_var: Any = None) -> Any:
                """Read one Variable."""
                return cls.store.get(key, default_var)

        module = types.ModuleType("airflow.models.variable")
        setattr(module, "Variable", Variable)
        patched = unittest.mock.patch.dict(
            sys.modules,
            {
                "airflow.models.variable": module,
                "airflow.sdk.execution_time.context": None,
                "airflow.sdk": None,
            },
        )
        patched.start()
        self.addCleanup(patched.stop)
        return Variable

    def test1(self) -> None:
        """
        Test that a read through the model is noted and still returns.
        """
        variable = self._model()
        with unittest.mock.patch.dict(os.environ, _ON):
            cealruns.watch_variables()
            self.assertEqual(variable.get("region"), "eu-west-1")
            self.assertIsNone(variable.get("missing"))
            self.assertEqual(variable.get("parsed"), {"b": 1, "a": 2})
        self.assertEqual(
            cesettin.noted(),
            {"variable": {"region": "eu-west-1", "parsed": '{"a": 2, "b": 1}'}},
        )

    def test2(self) -> None:
        """
        Test that wrapping twice notes a read once and wraps once.
        """
        variable = self._model()
        cealruns.watch_variables()
        first = variable.__dict__["get"]
        cealruns.watch_variables()
        self.assertIs(variable.__dict__["get"], first)

    def test3(self) -> None:
        """
        Test that Airflow 3's reader is wrapped where it is the one there.
        """
        context = types.ModuleType("airflow.sdk.execution_time.context")
        setattr(
            context, "_get_variable", lambda key, deserialize_json: "v-" + key
        )
        with (
            unittest.mock.patch.dict(
                sys.modules, {"airflow.sdk.execution_time.context": context}
            ),
            unittest.mock.patch.dict(os.environ, _ON),
        ):
            cealruns.watch_variables()
            reader = getattr(context, "_get_variable")
            self.assertEqual(reader("region", False), "v-region")
        self.assertEqual(cesettin.noted(), {"variable": {"region": "v-region"}})

    def test4(self) -> None:
        """
        Test that a listed Variable is read when the task ends.
        """
        self._model()
        environ = dict(_ON, CONVALESCE_SETTINGS_VARIABLES="region, missing")
        with unittest.mock.patch.dict(os.environ, environ, clear=True):
            cealruns.start()
            self.addCleanup(cealruns._STARTED.clear)
            found, excluded = cealruns.collect(_INGEST)
        self.assertEqual(excluded, [])
        self.assertIn(
            {"kind": "variable", "name": "region", "value": "eu-west-1"},
            found["items"],
        )

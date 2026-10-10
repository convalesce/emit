"""
Tests for the settings a run's last event carries, and the Variables noted.

Run with `make test`.
"""

import asyncio
import inspect
import json
import os
import re
import sys
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest import mock

import convalesce_emit.settings as cesettin
import convalesce_emit_prefect._settings as ceset
import convalesce_emit_prefect.hooks as cephooks

# Each plugin proves the same thing of its own event: a plain setting
# crosses, a secret crosses as a hash, and no key crosses at all. The
# plugins are independently installable with no dependency on each other,
# so sharing a test module between them would add one for no real benefit;
# the similarity stays and the check is turned off here rather than
# everywhere.
# pylint: disable=duplicate-code

_INGEST_KEY = "cvl_ingest_3f9a1c77d2e04b58"
_FINGERPRINT_KEY = "fingerprint-key-kept-at-home"
_SECRET = "s3cr3t-warehouse-pass"
_HEX16 = re.compile(r"^[0-9a-f]{16}$")
# The process a flow runs in, as a customer would have it with settings on.
_ENV = {
    "CONVALESCE_SEND_SETTINGS": "true",
    "CONVALESCE_FINGERPRINT_KEY": _FINGERPRINT_KEY,
    "CONVALESCE_PREFECT_API_READS": "false",
    "WAREHOUSE": "analytics",
    "API_KEY": _SECRET,
}


class _Recorder:
    """Stands in for an emitter, key and all, remembering what it was given."""

    def __init__(self, ingest_key: Optional[str] = _INGEST_KEY) -> None:
        self.config = types.SimpleNamespace(ingest_key=ingest_key)
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

    def __init__(self) -> None:
        self.id = "a64690f5"
        self.name = "helpful-marmot"


def _by_name(settings: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    A `settings` field's items, by the name of each.

    :param settings: what was sent under `settings`
    :return: name to item
    """
    return {item["name"]: item for item in settings[cesettin.ITEMS]}


class _Case(unittest.TestCase):
    """
    A test in a process with settings on and nothing noted yet.
    """

    env: Dict[str, str] = _ENV

    def setUp(self) -> None:
        """
        Set the environment a flow would run in, and forget what was noted.
        """
        patcher = mock.patch.dict(os.environ, self.env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        cesettin.noted()
        self.addCleanup(cesettin.noted)

    def flow_event(self, kind: str = "COMPLETED") -> Dict[str, Any]:
        """
        What the flow hook sends for a run that moved to a state.

        :param kind: the state's type
        :return: the one observation, as the emitter was given it
        """
        recorder = _Recorder()
        cephooks.emit_flow_run(
            flow="F", flow_run=_Run(), state=_State(kind), emitter=recorder
        )
        return recorder.sent[0]

    def task_event(self, kind: str = "COMPLETED") -> Dict[str, Any]:
        """
        What the task hook sends for a run that moved to a state.

        :param kind: the state's type
        :return: the one observation, as the emitter was given it
        """
        recorder = _Recorder()
        cephooks.emit_task_run(
            task="T", task_run=_Run(), state=_State(kind), emitter=recorder
        )
        return recorder.sent[0]


# #############################################################################
# Test_emit_settings1
# #############################################################################


class Test_emit_settings1(_Case):
    """
    Test that the event a run ends with carries its settings, secrets as
    hashes.
    """

    def test1(self) -> None:
        """
        Test that a plain setting arrives intact and a secret as a hash.
        """
        for sent in (self.flow_event(), self.task_event()):
            settings = sent["payload"]["settings"]
            items = _by_name(settings)
            self.assertEqual(
                items["WAREHOUSE"],
                {
                    "kind": "environment",
                    "name": "WAREHOUSE",
                    "value": "analytics",
                },
            )
            self.assertEqual(
                set(items["API_KEY"]), {"kind", "name", "fingerprint"}
            )
            self.assertRegex(items["API_KEY"]["fingerprint"], _HEX16)
            self.assertRegex(settings["keyed_by"], _HEX16)

    def test2(self) -> None:
        """
        Test that no secret and neither key is anywhere in what is sent.
        """
        with mock.patch.dict(os.environ, {"CONVALESCE_INGEST_KEY": _INGEST_KEY}):
            cesettin.note(cesettin.VARIABLE, "db_password", _SECRET)
            events = (self.task_event("FAILED"), self.flow_event("FAILED"))
        for sent in events:
            text = json.dumps(sent, default=str)
            self.assertIn("db_password", text)
            self.assertNotIn(_SECRET, text)
            self.assertNotIn(_INGEST_KEY, text)
            self.assertNotIn(_FINGERPRINT_KEY, text)

    def test3(self) -> None:
        """
        Test that with the setting off there is no `settings` field at all.
        """
        with mock.patch.dict(os.environ, {"CONVALESCE_SEND_SETTINGS": "false"}):
            events = (self.flow_event(), self.task_event())
        for sent in events:
            self.assertNotIn("settings", sent["payload"])
            self.assertNotIn("settings", json.dumps(sent, default=str))

    def test4(self) -> None:
        """
        Test that only a state that ends a run carries settings.
        """
        for kind in ("COMPLETED", "FAILED", "CRASHED", "CANCELLED"):
            self.assertIn("settings", self.flow_event(kind)["payload"], kind)
        for kind in ("RUNNING", "PENDING", "SCHEDULED", "CANCELLING"):
            self.assertNotIn("settings", self.flow_event(kind)["payload"], kind)
            self.assertNotIn("settings", self.task_event(kind)["payload"], kind)

    def test5(self) -> None:
        """
        Test that a noted Variable rides on each task's last event and is
        forgotten with the flow run's.
        """
        cesettin.note(cesettin.VARIABLE, "region", "eu")
        expected = {"kind": "variable", "name": "region", "value": "eu"}
        # A hook for a run still going neither sends it nor forgets it.
        self.task_event("RUNNING")
        self.flow_event("RUNNING")
        for sent in (self.task_event(), self.task_event(), self.flow_event()):
            self.assertIn(expected, sent["payload"]["settings"]["items"])
        self.assertNotIn(
            expected, self.flow_event()["payload"]["settings"]["items"]
        )

    def test6(self) -> None:
        """
        Test that without a key a secret is left out and declared so.
        """
        del os.environ["CONVALESCE_FINGERPRINT_KEY"]
        recorder = _Recorder(ingest_key=None)
        cephooks.emit_flow_run(
            flow="F", flow_run=_Run(), state=_State("FAILED"), emitter=recorder
        )
        sent = recorder.sent[0]
        settings = sent["payload"]["settings"]
        self.assertNotIn("API_KEY", _by_name(settings))
        self.assertNotIn("keyed_by", settings)
        self.assertIn(
            {
                "path": "settings",
                "reason": "no key to fingerprint 1 settings with",
            },
            sent["excluded"],
        )
        self.assertNotIn(_SECRET, json.dumps(sent, default=str))

    def test7(self) -> None:
        """
        Test that the hash is keyed from the emitter's ingest key where
        there is no fingerprint key.
        """
        own = self.flow_event()["payload"]["settings"]
        del os.environ["CONVALESCE_FINGERPRINT_KEY"]
        derived = self.flow_event()["payload"]["settings"]
        self.assertRegex(derived["keyed_by"], _HEX16)
        self.assertNotEqual(derived["keyed_by"], own["keyed_by"])

    def test8(self) -> None:
        """
        Test that a name the operator listed is never sent.
        """
        os.environ["CONVALESCE_SETTINGS_SKIP"] = "WAREHOUSE, API_KEY"
        items = _by_name(self.flow_event()["payload"]["settings"])
        self.assertNotIn("WAREHOUSE", items)
        self.assertNotIn("API_KEY", items)
        self.assertIn("CONVALESCE_SEND_SETTINGS", items)

    def test9(self) -> None:
        """
        Test that settings that cannot be read cost the field, not the
        event, and that the log names the error's class and no more.
        """
        boom = RuntimeError(f"could not read {_SECRET}")
        with mock.patch.object(cesettin, "collect", side_effect=boom):
            with self.assertLogs(ceset.__name__, level="WARNING") as logs:
                sent = self.flow_event()
        self.assertNotIn("settings", sent["payload"])
        self.assertEqual(sent["payload"]["flow"], "F")
        self.assertIn("RuntimeError", logs.output[0])
        self.assertNotIn(_SECRET, "".join(logs.output))


# #############################################################################
# Test_ingest_key_of1
# #############################################################################


class Test_ingest_key_of1(_Case):
    """
    Test which key a setting's hash is keyed from.
    """

    def test1(self) -> None:
        """
        Test that a hook given an emitter uses that emitter's key.
        """
        os.environ["CONVALESCE_INGEST_KEY"] = "another"
        self.assertEqual(ceset.ingest_key_of(_Recorder()), _INGEST_KEY)
        self.assertIsNone(ceset.ingest_key_of(_Recorder(ingest_key=None)))
        self.assertIsNone(ceset.ingest_key_of(object()))

    def test2(self) -> None:
        """
        Test that a hook given none uses the key the environment holds,
        trimmed as an emitter trims it and under either of its names.
        """
        self.assertIsNone(ceset.ingest_key_of(None))
        os.environ["CUSTOMER_CONVALESCE_INGEST_KEY"] = f" {_INGEST_KEY}\n"
        self.assertEqual(ceset.ingest_key_of(None), _INGEST_KEY)


# #############################################################################
# Test_watch_variables1
# #############################################################################

_VALUES: Dict[str, Any] = {
    "region": "eu",
    "limits": {"rows": 10, "batch": 2},
    "db_password": _SECRET,
}


async def _lookup(name: str, default: Any = None) -> Any:
    """Stands in for the API read behind every reader."""
    if name == "broken":
        raise LookupError(f"no access with {_SECRET}")
    return _VALUES.get(name, default)


def _dispatched(name: str, default: Any = None) -> Any:
    """
    Read as Prefect's hybrid readers do: a value to sync code, and a
    coroutine to code already inside an event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_lookup(name, default))
    return _lookup(name, default)


def _prefect3() -> Any:
    """A stand-in `prefect.variables` shaped as Prefect 3.1 and later."""

    class Variable:
        """Stands in for Prefect's `Variable`."""

        @classmethod
        async def aget(cls, name: str, default: Any = None) -> Any:
            """The async reader."""
            return await _lookup(name, default)

        @classmethod
        def get(cls, name: str, default: Any = None) -> Any:
            """The reader that serves both worlds."""
            return _dispatched(name, default)

    # An attribute callers reach through the function, as `.aio` is.
    setattr(Variable.__dict__["get"].__func__, "aio", _lookup)
    return types.SimpleNamespace(Variable=Variable)


def _prefect2() -> Any:
    """A stand-in `prefect.variables` shaped as Prefect 2.20."""

    class Variable:
        """Stands in for Prefect's `Variable`, which a read can return."""

        def __init__(self, name: str, value: Any) -> None:
            self.name = name
            self.value = value

        @classmethod
        def get(
            cls, name: str, default: Any = None, as_object: bool = False
        ) -> Any:
            """The reader, handing back the value or the object."""
            value = _dispatched(name, default)
            if as_object and value is not None:
                return cls(name, value)
            return value

    def get(name: str, default: Any = None) -> Any:
        """The module's own, older reader."""
        return _dispatched(name, default)

    return types.SimpleNamespace(Variable=Variable, get=get)


class Test_watch_variables1(_Case):
    """
    Test that a Variable is noted as it is read, and the read unchanged.
    """

    def watched(self, module: Any) -> Any:
        """
        Put the wrapping in, with `module` standing in for Prefect's.

        :param module: a stand-in `prefect.variables`
        :return: the same module
        """
        with mock.patch.dict(sys.modules, {"prefect.variables": module}):
            ceset.watch_variables()
        return module

    def test1(self) -> None:
        """
        Test that a sync read returns what it did and is noted.
        """
        variable = self.watched(_prefect3()).Variable
        self.assertEqual(variable.get("region"), "eu")
        self.assertEqual(variable.get(name="limits"), {"rows": 10, "batch": 2})
        self.assertEqual(
            cesettin.noted(),
            {"variable": {"region": "eu", "limits": '{"batch": 2, "rows": 10}'}},
        )

    def test2(self) -> None:
        """
        Test that a read from async code still hands back an awaitable, and
        is noted once that resolves and not before.
        """
        variable = self.watched(_prefect3()).Variable

        async def flow() -> List[Any]:
            pending = variable.get("region")
            before = cesettin.noted(clear=False)
            return [before, await pending, await variable.aget("limits")]

        before, region, limits = asyncio.run(flow())
        self.assertEqual(before, {})
        self.assertEqual(region, "eu")
        self.assertEqual(limits, {"rows": 10, "batch": 2})
        self.assertEqual(
            sorted(cesettin.noted()["variable"]), ["limits", "region"]
        )

    def test3(self) -> None:
        """
        Test that each reader keeps its calling convention and what hangs
        off it.
        """
        variable = self.watched(_prefect3()).Variable
        self.assertTrue(inspect.iscoroutinefunction(variable.aget))
        self.assertFalse(inspect.iscoroutinefunction(variable.get))
        self.assertIs(variable.get.aio, _lookup)
        self.assertEqual(variable.get.__name__, "get")

    def test4(self) -> None:
        """
        Test that wrapping twice wraps once.
        """
        module = self.watched(_prefect3())
        first = module.Variable.__dict__["get"].__func__
        self.watched(module)
        self.assertIs(module.Variable.__dict__["get"].__func__, first)
        module.Variable.get("region")
        self.assertEqual(cesettin.noted(), {"variable": {"region": "eu"}})

    def test5(self) -> None:
        """
        Test that Prefect 2's readers are noted: the class's, which can
        return the Variable itself, and the module's own.
        """
        module = self.watched(_prefect2())
        held = module.Variable.get("region", as_object=True)
        self.assertEqual(held.value, "eu")
        self.assertEqual(cesettin.noted(), {"variable": {"region": "eu"}})
        self.assertEqual(module.get("limits"), {"rows": 10, "batch": 2})
        self.assertIn("limits", cesettin.noted()["variable"])

    def test6(self) -> None:
        """
        Test that a read that finds nothing is not noted, and one that
        falls back to a default is noted as that default.
        """
        variable = self.watched(_prefect3()).Variable
        self.assertIsNone(variable.get("absent"))
        self.assertEqual(variable.get("absent_too", "fallback"), "fallback")
        self.assertEqual(
            cesettin.noted(), {"variable": {"absent_too": "fallback"}}
        )

    def test7(self) -> None:
        """
        Test that a read that raises still raises, exactly as it did.
        """
        variable = self.watched(_prefect3()).Variable
        with self.assertRaises(LookupError):
            variable.get("broken")
        with self.assertRaises(LookupError):
            asyncio.run(variable.aget("broken"))
        self.assertEqual(cesettin.noted(), {})

    def test8(self) -> None:
        """
        Test that a `Variable` shaped otherwise is left exactly as it is.
        """

        class Variable:
            """A `Variable` whose reader is not a classmethod."""

            @staticmethod
            def get(name: str) -> str:
                """A reader of another shape."""
                return name

        marker = object()
        module = types.SimpleNamespace(Variable=Variable, get=marker)
        before = Variable.__dict__["get"]
        self.watched(module)
        self.assertIs(Variable.__dict__["get"], before)
        self.assertIs(module.get, marker)
        self.watched(types.SimpleNamespace())
        self.assertEqual(cesettin.noted(), {})

    def test9(self) -> None:
        """
        Test that without Prefect nothing is raised.
        """
        with mock.patch.dict(sys.modules, {"prefect.variables": None}):
            ceset.watch_variables()

    def test10(self) -> None:
        """
        Test that with settings off Prefect is not touched at all.
        """
        os.environ["CONVALESCE_SEND_SETTINGS"] = "false"
        module = _prefect3()
        before = module.Variable.__dict__["get"]
        self.watched(module)
        self.assertIs(module.Variable.__dict__["get"], before)

    def test11(self) -> None:
        """
        Test that a Variable read through the wrapper reaches the event,
        a secret one only as its hash.
        """
        variable = self.watched(_prefect3()).Variable
        variable.get("region")
        variable.get("db_password")
        sent = self.flow_event()
        items = [
            item
            for item in sent["payload"]["settings"]["items"]
            if item["kind"] == "variable"
        ]
        self.assertEqual(
            items[1], {"kind": "variable", "name": "region", "value": "eu"}
        )
        self.assertEqual(items[0]["name"], "db_password")
        self.assertRegex(items[0]["fingerprint"], _HEX16)
        self.assertNotIn(_SECRET, json.dumps(sent, default=str))

    def test13(self) -> None:
        """
        Test that a reader serving both worlds is never awaited for its
        caller, even where it is marked as a coroutine function.
        """
        module = _prefect3()
        hybrid = module.Variable.__dict__["get"].__func__
        # What `inspect.markcoroutinefunction` sets, on Python 3.12.
        marker = getattr(inspect, "_is_coroutine_mark", None)
        setattr(hybrid, "_is_coroutine_marker", marker)
        variable = self.watched(module).Variable
        self.assertEqual(variable.get("region"), "eu")

    def test12(self) -> None:
        """
        Test that a note that cannot be taken never reaches the reader's
        caller.
        """
        variable = self.watched(_prefect3()).Variable
        with mock.patch.object(cesettin, "note", side_effect=RuntimeError("x")):
            self.assertEqual(variable.get("region"), "eu")
            self.assertEqual(asyncio.run(variable.aget("region")), "eu")

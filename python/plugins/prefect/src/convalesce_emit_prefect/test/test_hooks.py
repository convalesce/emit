"""
Tests for the flow and task state hooks.

Run with `make test`.
"""

import json
import logging
import os
import sys
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest import mock

import convalesce_emit.sqlcapture as cesqlcap
import convalesce_emit_prefect._capture as cecap
import convalesce_emit_prefect.hooks as cephooks

_LOG = logging.getLogger(__name__)

# Restated rather than read off the module under test, so this stays a test
# of the documented kill switch's name, not of the private constant.
_API_READS_ENV = "CONVALESCE_PREFECT_API_READS"


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
        self.flow_id = "f-nightly"


class _TaskRun:
    """Stands in for a Prefect task run, which names its flow run by id."""

    def __init__(self) -> None:
        self.name = "extract-12f"
        self.flow_run_id = "a64690f5"
        self.state_name = "Completed"
        self.state: Any = None


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


# #############################################################################
# Fakes for the API reads
# #############################################################################


class _APIFlow:
    """Stands in for the `Flow` the API's own `read_flow` returns."""

    def __init__(self, name: str) -> None:
        self.name = name


class _APIFlowRun:
    """Stands in for the `FlowRun` the API's own `read_flow_run` returns."""

    def __init__(self, name: str) -> None:
        self.name = name


class _APIResponse:
    """Stands in for the `httpx.Response` a raw `GET` returns."""

    def __init__(self, data: Any) -> None:
        self._data = data

    def json(self) -> Any:
        """The parsed body, the way a real response's `.json()` would."""
        return self._data


class _HttpClient:
    """Stands in for the raw HTTP client underneath the typed one."""

    def __init__(self, calls: List[Any]) -> None:
        self._calls = calls

    def get(self, path: str) -> _APIResponse:
        """Answer a raw `GET`, the fallback path uses."""
        self._calls.append(("http_get", path))
        return _APIResponse({"flow": "raw"})


class _SyncClient:
    """Stands in for `SyncPrefectClient`, configurably missing a method or
    two, the way Prefect 2's actually is."""

    def __init__(
        self,
        task_pages: Optional[List[List[Any]]] = None,
        has_read_flow: bool = True,
        has_request: bool = True,
    ) -> None:
        self.calls: List[Any] = []
        self._task_pages = task_pages or []
        self._http = _HttpClient(self.calls)
        if has_read_flow:
            self.read_flow = self._read_flow
        if has_request:
            self.request = self._request

    def __enter__(self) -> "_SyncClient":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    @property
    def _client(self) -> _HttpClient:
        return self._http

    def _read_flow(self, flow_id: str) -> _APIFlow:
        self.calls.append(("read_flow", flow_id))
        return _APIFlow("nightly")

    def read_flow_run(self, flow_run_id: str) -> _APIFlowRun:
        """The flow run record, by id."""
        self.calls.append(("read_flow_run", flow_run_id))
        return _APIFlowRun("helpful-marmot")

    def read_task_runs(
        self, *, flow_run_filter: Any, limit: int, offset: int
    ) -> List[Any]:
        """One page of task runs, the way the real client's does."""
        self.calls.append(("read_task_runs", flow_run_filter, limit, offset))
        page_index = offset // limit
        if page_index >= len(self._task_pages):
            return []
        return self._task_pages[page_index]

    def _request(self, method: str, path: str) -> _APIResponse:
        self.calls.append(("request", method, path))
        return _APIResponse({"nodes": ["extract", "load"]})


class _FlowRunFilterId:
    """Stands in for `FlowRunFilterId`."""

    def __init__(self, any_: List[str]) -> None:
        self.any_ = any_


class _FlowRunFilter:
    """Stands in for `FlowRunFilter`, whose real keyword is `id`."""

    # pylint: disable-next=redefined-builtin
    def __init__(self, id: Any) -> None:
        self.id = id


def _prefect_modules(client: Any) -> Dict[str, Any]:
    """The two `prefect.*` modules `api_state` imports, faked."""
    orchestration = types.SimpleNamespace(
        get_client=lambda sync_client=True: client
    )
    filters = types.SimpleNamespace(
        FlowRunFilter=_FlowRunFilter, FlowRunFilterId=_FlowRunFilterId
    )
    return {
        "prefect.client.orchestration": orchestration,
        "prefect.client.schemas.filters": filters,
    }


# #############################################################################
# Test_api_state1
# #############################################################################


class Test_api_state1(unittest.TestCase):
    """
    Test the four API reads Phase 2 adds, and their kill switch.
    """

    def test1(self) -> None:
        """
        Test that all four reads land under their own `api_`-prefixed keys,
        never colliding with the hook's own `flow`/`flow_run`, and that
        task runs are paged rather than capped at one page.
        """
        client = _SyncClient(task_pages=[["t1", "t2"], ["t3"]])
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, _prefect_modules(client)):
            with mock.patch.object(cephooks, "_TASK_RUN_PAGE_SIZE", 2):
                cephooks.emit_flow_run(
                    flow=_Flow(),
                    flow_run=_FlowRun(),
                    state="S",
                    emitter=recorder,
                )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["flow"]["name"], "nightly")
        self.assertEqual(payload["api_flow"]["name"], "nightly")
        self.assertEqual(payload["api_flow_run"]["name"], "helpful-marmot")
        self.assertEqual(
            payload["api_flow_run_graph"]["nodes"], ["extract", "load"]
        )
        self.assertEqual(payload["api_task_runs"], ["t1", "t2", "t3"])
        offsets = [
            call[3] for call in client.calls if call[0] == "read_task_runs"
        ]
        # A page short of the page size is the API's own "no more" signal,
        # so the second page ends the read without a needless third call.
        self.assertEqual(offsets, [0, 2])

    def test2(self) -> None:
        """
        Test that the kill switch turns every read off, leaving only what
        the hook's own arguments already carried.
        """
        client = _SyncClient()
        recorder = _Recorder()
        env = {_API_READS_ENV: "false"}
        with mock.patch.dict(os.environ, env, clear=False):
            with mock.patch.dict(sys.modules, _prefect_modules(client)):
                cephooks.emit_flow_run(
                    flow=_Flow(),
                    flow_run=_FlowRun(),
                    state="S",
                    emitter=recorder,
                )
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("api_flow", payload)
        self.assertEqual(client.calls, [])

    def test3(self) -> None:
        """
        Test that a Prefect 2 sync client -- missing both `read_flow` and
        the generic `request` 3.x's clients carry -- falls back to the raw
        HTTP client every typed method already calls through, rather than
        losing the flow record entirely.
        """
        client = _SyncClient(has_read_flow=False, has_request=False)
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, _prefect_modules(client)):
            cephooks.emit_flow_run(
                flow=_Flow(), flow_run=_FlowRun(), state="S", emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["api_flow"], {"flow": "raw"})

    def test4(self) -> None:
        """
        Test that Prefect being entirely absent loses only the API reads,
        not the event.
        """
        recorder = _Recorder()
        with mock.patch.dict(
            sys.modules, {"prefect.client.orchestration": None}
        ):
            cephooks.emit_flow_run(
                flow=_Flow(), flow_run=_FlowRun(), state="S", emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["flow_run"]["id"], "a64690f5")
        self.assertNotIn("api_flow", payload)

    def test5(self) -> None:
        """
        Test that an unreachable API loses only the reads it was asking
        for, not the whole event.
        """

        class _Broken(_SyncClient):
            """A client whose every typed method raises."""

            def read_flow_run(self, flow_run_id: str) -> Any:
                """Fail the way a downed API does."""
                raise RuntimeError("connection refused")

        client = _Broken()
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, _prefect_modules(client)):
            cephooks.emit_flow_run(
                flow=_Flow(), flow_run=_FlowRun(), state="S", emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["flow_run"]["id"], "a64690f5")
        self.assertNotIn("api_flow_run", payload)


# #############################################################################
# Test_cloud_workspace1
# #############################################################################


class Test_cloud_workspace1(unittest.TestCase):
    """
    Test that the Cloud account and workspace are read from the API URL
    itself, with no run involved at all.
    """

    def test1(self) -> None:
        """
        Test that a Prefect Cloud URL names both.
        """
        url = "https://api.prefect.cloud/api/accounts/acct-1/workspaces/ws-1"
        with mock.patch.dict(os.environ, {"PREFECT_API_URL": url}, clear=False):
            workspace = cephooks.cloud_workspace()
        self.assertEqual(
            workspace, {"account_id": "acct-1", "workspace_id": "ws-1"}
        )

    def test2(self) -> None:
        """
        Test that a self-hosted server's URL names neither.
        """
        url = "https://prefect.internal.example.com/api"
        with mock.patch.dict(os.environ, {"PREFECT_API_URL": url}, clear=False):
            self.assertEqual(cephooks.cloud_workspace(), {})


# #############################################################################
# Test_api_reads_enabled1
# #############################################################################


class Test_api_reads_enabled1(unittest.TestCase):
    """
    Test that the reads default on and the kill switch is case-insensitive.
    """

    def test1(self) -> None:
        """
        Test that leaving the variable unset means the reads run.
        """
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(cephooks.api_reads_enabled())

    def test2(self) -> None:
        """
        Test that every documented falsy spelling turns the reads off.
        """
        for value in ("false", "FALSE", "0", "no", "off"):
            env = {_API_READS_ENV: value}
            with mock.patch.dict(os.environ, env, clear=False):
                self.assertFalse(cephooks.api_reads_enabled(), value)


# #############################################################################
# Test_error_detail1
# #############################################################################


def _raised() -> ValueError:
    """
    A ValueError that was raised, so it carries a traceback.

    :return: the exception
    """
    try:
        try:
            raise KeyError("id")
        except KeyError as cause:
            raise ValueError("bad row") from cause
    except ValueError as exc:
        return exc


class _State:
    """Stands in for a Prefect state."""

    def __init__(self, failed: bool, data: Any) -> None:
        self._failed = failed
        self.data = data

    def is_failed(self) -> bool:
        """Whether the run failed."""
        return self._failed

    def is_crashed(self) -> bool:
        """Whether the run crashed."""
        return False

    def is_completed(self) -> bool:
        """Whether the run completed."""
        return not self._failed


class _ResultRecord:
    """Stands in for Prefect 3's `ResultRecord`, holding its result."""

    def __init__(self, result: Any) -> None:
        self.result = result


class _PersistedResult:
    """A result that would read from storage if asked for its value."""

    def __init__(self) -> None:
        self.storage_key = "s3://bucket/key"

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"read {name} from storage")


class Test_error_detail1(unittest.TestCase):
    """
    Test that a failed task's exception crosses with its traceback.
    """

    def test1(self) -> None:
        """
        Test that the exception a Prefect 3 result record holds is sent as
        `error_detail`, its traceback included.
        """
        recorder = _Recorder()
        state = _State(True, _ResultRecord(_raised()))
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            cephooks.emit_task_run(
                task="T", task_run=_TaskRun(), state=state, emitter=recorder
            )
        detail = recorder.sent[0]["payload"]["error_detail"]
        self.assertEqual(detail["type"], "builtins.ValueError")
        self.assertEqual(detail["message"], "bad row")
        self.assertIn("_raised", detail["traceback"])
        self.assertEqual(detail["cause"]["type"], "builtins.KeyError")

    def test2(self) -> None:
        """
        Test that a bare exception, or one in a Prefect 2 result's cache,
        is found too.
        """

        class Cached:
            """Stands in for a Prefect 2 result with its value cached."""

            def __init__(self, value: Any) -> None:
                self._cache = value

        for data in (_raised(), Cached(_raised())):
            detail = cephooks.error_detail(_State(True, data))
            assert detail is not None
            self.assertEqual(detail["type"], "builtins.ValueError")

    def test3(self) -> None:
        """
        Test that nothing is added for a success, a failure whose result is
        not in memory, or a state that is not Prefect's.
        """
        self.assertIsNone(cephooks.error_detail(_State(False, _raised())))
        self.assertIsNone(
            cephooks.error_detail(_State(True, _PersistedResult()))
        )
        self.assertIsNone(cephooks.error_detail("S"))
        self.assertIsNone(cephooks.error_detail(None))

    def test4(self) -> None:
        """
        Test that a failed flow run sends its exception too, not only the
        failed task run.
        """
        recorder = _Recorder()
        state = _State(True, _ResultRecord(_raised()))
        cephooks.emit_flow_run(
            flow=_Flow(), flow_run=_FlowRun(), state=state, emitter=recorder
        )
        detail = recorder.sent[0]["payload"]["error_detail"]
        self.assertEqual(detail["type"], "builtins.ValueError")
        self.assertIn("_raised", detail["traceback"])

    def test5(self) -> None:
        """
        Test that what a task returned never crosses as data: a state's data
        is the customer's own, on the state and on the run's copy of it
        alike. Only a short scalar crosses, as `result_text`.
        """
        recorder = _Recorder()
        result = {"email": "customer@example.com"}
        task_run = _TaskRun()
        task_run.state = _State(False, _ResultRecord(result))
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            cephooks.emit_task_run(
                task="T",
                task_run=task_run,
                state=_State(False, _ResultRecord(result)),
                emitter=recorder,
            )
        sent = recorder.sent[0]
        self.assertNotIn("customer@example.com", json.dumps(sent["payload"]))
        self.assertIn(
            {"path": "state.data", "reason": "excluded by name"},
            sent["excluded"],
        )


# #############################################################################
# Test_result_text1
# #############################################################################


# Restated for the same reason as `_API_READS_ENV`.
_SEND_RESULT_ENV = "CONVALESCE_PREFECT_SEND_RESULT"

_GLUE_RUN_ID = "jr_" + "0123456789abcdef" * 4


def _completed_task_payload(data: Any) -> Dict[str, Any]:
    """
    Fire the task hook for a task that completed holding `data`.

    :param data: the state's data
    :return: the payload sent
    """
    recorder = _Recorder()
    with mock.patch.dict(sys.modules, {"prefect.context": None}):
        cephooks.emit_task_run(
            task="T",
            task_run=_TaskRun(),
            state=_State(False, data),
            emitter=recorder,
        )
    payload: Dict[str, Any] = recorder.sent[0]["payload"]
    return payload


class Test_result_text1(unittest.TestCase):
    """
    Test that a task's short scalar result crosses as `result_text`.
    """

    def setUp(self) -> None:
        patcher = mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop(_SEND_RESULT_ENV, None)

    def test1(self) -> None:
        """
        Test that a returned run id crosses as is, from a Prefect 3 result
        record, a Prefect 2 cached result, or bare data.
        """

        class Cached:
            """Stands in for a Prefect 2 result, a pydantic 1 model: its
            cached value is a declared private attribute, kept in a slot and
            not in the instance's `__dict__`."""

            __slots__ = ("_cache",)
            __private_attributes__ = {"_cache": None}

            def __init__(self, value: Any) -> None:
                object.__setattr__(self, "_cache", value)

        for data in (
            _ResultRecord(_GLUE_RUN_ID),
            Cached(_GLUE_RUN_ID),
            _GLUE_RUN_ID,
        ):
            payload = _completed_task_payload(data)
            self.assertEqual(payload["result_text"], _GLUE_RUN_ID)

    def test1b(self) -> None:
        """
        Test that a `_cache` the class does not declare private, such as a
        property that would load the result, is never read.
        """

        class Loads:
            """A result whose `_cache` is a property that reads storage."""

            @property
            def _cache(self) -> str:
                raise AssertionError("the result was loaded")

        payload = _completed_task_payload(Loads())
        self.assertNotIn("result_text", payload)

    def test2(self) -> None:
        """
        Test that other scalars and short lists are rendered as text, and
        anything is cut to 200 characters.
        """
        cases = [
            (42, "42"),
            (True, "True"),
            ([_GLUE_RUN_ID, 3], json.dumps([_GLUE_RUN_ID, 3])),
            ("x" * 500, "x" * 200),
            (["y" * 150, "z" * 150], json.dumps(["y" * 150, "z" * 150])[:200]),
        ]
        for value, expected in cases:
            text = cephooks.result_text(_State(False, _ResultRecord(value)))
            self.assertEqual(text, expected)

    def test3(self) -> None:
        """
        Test that nothing else crosses: a mapping, an object, a long or
        mixed list, a failed state, or a result not held in memory.
        """

        class Opaque:
            """A customer object, whose repr is its own business."""

            def __repr__(self) -> str:
                return _GLUE_RUN_ID

        class Label(str):
            """A str subclass, which can render itself any way it likes."""

        for data in (
            _ResultRecord({"run_id": _GLUE_RUN_ID}),
            _ResultRecord(Opaque()),
            _ResultRecord(Label(_GLUE_RUN_ID)),
            _ResultRecord(list(range(11))),
            _ResultRecord([_GLUE_RUN_ID, {"a": 1}]),
            _ResultRecord(None),
            _PersistedResult(),
        ):
            payload = _completed_task_payload(data)
            self.assertNotIn("result_text", payload)
            self.assertNotIn(_GLUE_RUN_ID, json.dumps(payload))
        self.assertIsNone(
            cephooks.result_text(_State(True, _ResultRecord(_GLUE_RUN_ID)))
        )
        self.assertIsNone(cephooks.result_text("S"))

    def test4(self) -> None:
        """
        Test that `CONVALESCE_PREFECT_SEND_RESULT` set falsy keeps it back.
        """
        for raw in ("0", "false", "FALSE"):
            os.environ[_SEND_RESULT_ENV] = raw
            payload = _completed_task_payload(_ResultRecord(_GLUE_RUN_ID))
            self.assertNotIn("result_text", payload)
        os.environ[_SEND_RESULT_ENV] = "1"
        payload = _completed_task_payload(_ResultRecord(_GLUE_RUN_ID))
        self.assertEqual(payload["result_text"], _GLUE_RUN_ID)

    def test5(self) -> None:
        """
        Test that a flow run sends no `result_text`: only a task's result
        names a run it started.
        """
        recorder = _Recorder()
        cephooks.emit_flow_run(
            flow=_Flow(),
            flow_run=_FlowRun(),
            state=_State(False, _ResultRecord(_GLUE_RUN_ID)),
            emitter=recorder,
        )
        self.assertNotIn("result_text", recorder.sent[0]["payload"])


# #############################################################################
# Test_withhold_parameters1
# #############################################################################


# Restated for the same reason as `_API_READS_ENV`.
_SEND_PARAMETERS_ENV = "CONVALESCE_PREFECT_SEND_PARAMETERS"
_SEND_ARGUMENTS_ENV = "CONVALESCE_SEND_ARGUMENTS"
_SEND_SOURCE_ENV = "CONVALESCE_SEND_SOURCE"


class _ParameterisedFlow(_Flow):
    """Stands in for a flow whose schema carries a default."""

    def __init__(self) -> None:
        super().__init__()
        self.parameters = {
            "type": "object",
            "properties": {
                "customer": {"type": "string", "default": "acme-corp"},
                "limit": {"type": "integer"},
            },
        }


class _ParameterisedRun(_FlowRun):
    """Stands in for a flow run launched with customer values."""

    def __init__(self) -> None:
        super().__init__()
        self.parameters = {"customer": "acme-corp", "limit": 10}
        self.job_variables = {"env": {"DB_PASSWORD": "hunter2"}}


class Test_withhold_parameters1(unittest.TestCase):
    """
    Test that a flow's parameter values cross unless switched off.
    """

    def _send(self, env: Dict[str, str]) -> Dict[str, Any]:
        """
        Fire the flow hook for a run launched with customer values.

        :param env: the environment to fire it under
        :return: the observation sent
        """
        recorder = _Recorder()
        with mock.patch.dict(os.environ, env, clear=True):
            cephooks.emit_flow_run(
                flow=_ParameterisedFlow(),
                flow_run=_ParameterisedRun(),
                state="S",
                emitter=recorder,
            )
        return recorder.sent[0]

    def test1(self) -> None:
        """
        Test that with arguments switched off the names and types cross,
        never the values.
        """
        sent = self._send({_SEND_ARGUMENTS_ENV: "false"})
        payload = sent["payload"]
        self.assertEqual(
            payload["flow_run"]["parameters"],
            {"customer": "<str>", "limit": "<int>"},
        )
        schema = payload["flow"]["parameters"]["properties"]
        self.assertEqual(schema["customer"]["default"], "<str>")
        self.assertNotIn("default", schema["limit"])
        self.assertNotIn("acme-corp", json.dumps(payload))
        reason = "parameter values withheld"
        self.assertIn(
            {"path": "flow_run.parameters", "reason": reason}, sent["excluded"]
        )
        self.assertIn(
            {
                "path": "flow.parameters.properties.customer.default",
                "reason": reason,
            },
            sent["excluded"],
        )

    def test2(self) -> None:
        """
        Test that by default the values cross as they are, and that the
        older setting, set to a yes, sends them even with arguments off.
        """
        for env in (
            {},
            {_SEND_PARAMETERS_ENV: "true"},
            {_SEND_PARAMETERS_ENV: "true", _SEND_ARGUMENTS_ENV: "false"},
        ):
            sent = self._send(env)
            payload = sent["payload"]
            self.assertEqual(
                payload["flow_run"]["parameters"],
                {"customer": "acme-corp", "limit": 10},
            )
            schema = payload["flow"]["parameters"]["properties"]
            self.assertEqual(schema["customer"]["default"], "acme-corp")
            self.assertNotIn(
                "parameter values withheld",
                [entry["reason"] for entry in sent["excluded"]],
            )

    def test2b(self) -> None:
        """
        Test that the older setting, set to a no, still withholds the
        values, whatever the newer one says.
        """
        for env in (
            {_SEND_PARAMETERS_ENV: "false"},
            {_SEND_PARAMETERS_ENV: "0", _SEND_ARGUMENTS_ENV: "true"},
        ):
            sent = self._send(env)
            self.assertEqual(
                sent["payload"]["flow_run"]["parameters"],
                {"customer": "<str>", "limit": "<int>"},
            )

    def test2c(self) -> None:
        """
        Test that a parameter holding a document is cut like any argument,
        and the cut is declared.
        """
        recorder = _Recorder()
        run = _ParameterisedRun()
        run.parameters = {"ids": list(range(400)), "limit": 10}
        with mock.patch.dict(os.environ, {}, clear=True):
            cephooks.emit_flow_run(
                flow=_ParameterisedFlow(),
                flow_run=run,
                state="S",
                emitter=recorder,
            )
        sent = recorder.sent[0]
        parameters = sent["payload"]["flow_run"]["parameters"]
        self.assertEqual(parameters["ids"], list(range(50)))
        self.assertEqual(parameters["limit"], 10)
        self.assertIn(
            {
                "path": "flow_run.parameters.ids",
                "reason": "argument cut: first 50 of 400",
            },
            sent["excluded"],
        )

    def test3(self) -> None:
        """
        Test that a credential in a run's job variables never crosses, in
        either mode.
        """
        for env in ({}, {_SEND_PARAMETERS_ENV: "false"}):
            sent = self._send(env)
            self.assertNotIn("hunter2", json.dumps(sent["payload"]))
            self.assertIn(
                {
                    "path": "flow_run.job_variables.env.DB_PASSWORD",
                    "reason": "secret redacted",
                },
                sent["excluded"],
            )

    def test4(self) -> None:
        """
        Test that the API's copy of the run is withheld the same way, and a
        payload with no parameters anywhere is left alone.
        """
        body = {"api_flow_run": {"parameters": {"since": "2026-01-01"}}}
        out, excluded = cephooks.withhold_parameters(body)
        self.assertEqual(out["api_flow_run"]["parameters"], {"since": "<str>"})
        self.assertEqual(
            excluded,
            [
                {
                    "path": "api_flow_run.parameters",
                    "reason": "parameter values withheld",
                }
            ],
        )
        self.assertEqual(cephooks.withhold_parameters({"a": 1}), ({"a": 1}, []))


# #############################################################################
# Test_what_ran1
# #############################################################################


def _load(table: str, minimum: int) -> str:
    """A task body whose text is looked for below."""
    return f"{table}:{minimum}"


class _RunningTask:
    """Stands in for a Prefect task: the function, and on 3.8 its text."""

    def __init__(self) -> None:
        self.name = "load"
        self.fn = _load
        self.source_code = "def _load(table, minimum): ..."


class _RunContexts:
    """A stand-in `prefect.context` with a task run in progress."""

    def __init__(self, task_run: Any, parameters: Dict[str, Any]) -> None:
        context = types.SimpleNamespace(task_run=task_run, parameters=parameters)
        flow_context = types.SimpleNamespace(flow=_Flow(), flow_run=_FlowRun())
        # Named as Prefect names them, which is what the hook looks up.
        setattr(
            self, "TaskRunContext", types.SimpleNamespace(get=lambda: context)
        )
        setattr(
            self,
            "FlowRunContext",
            types.SimpleNamespace(get=lambda: flow_context),
        )


class Test_what_ran1(unittest.TestCase):
    """
    Test that a run says what it ran: source, arguments and SQL.
    """

    def setUp(self) -> None:
        self.task_run = _TaskRun()
        self.task_run.id = "t-1"  # type: ignore[attr-defined]
        arguments = {
            "table": "orders",
            "minimum": 1000000000,
            "options": {"api_key": "abc123", "dsn": "postgres://u:pw@h/db"},
        }
        modules = {"prefect.context": _RunContexts(self.task_run, arguments)}
        patch = mock.patch.dict(sys.modules, modules)
        patch.start()
        self.addCleanup(patch.stop)
        cesqlcap.scope_by(cecap.running_scope)
        self.addCleanup(cesqlcap.scope_by, None)

    def _send(self, env: Dict[str, str], state: Any = "Failed") -> Any:
        """
        Run one statement as the task, then fire its hook.

        :param env: the environment to do both under
        :param state: the state the hook is given
        :return: the observation sent
        """
        recorder = _Recorder()
        with mock.patch.dict(os.environ, env, clear=True):
            cesqlcap.record(
                "insert into shop.big select * from shop.orders where n > %s",
                dialect="postgres",
                database="shop",
                via="test",
            )
            cephooks.emit_task_run(
                task=_RunningTask(),
                task_run=self.task_run,
                state=state,
                emitter=recorder,
            )
        return recorder.sent[0]

    def test1(self) -> None:
        """
        Test that a task event carries the task's source, the values it
        was called with and the statements it sent, credentials masked.
        """
        sent = self._send({})
        payload = sent["payload"]
        self.assertIn("def _load(table: str", payload["task"]["source"]["text"])
        self.assertEqual(
            payload["arguments"],
            {
                "table": "orders",
                "minimum": 1000000000,
                "options": {
                    "api_key": {"redacted": True},
                    "dsn": "postgres://u:***@h/db",
                },
            },
        )
        (noted,) = payload["sql_capture"]["statements"]
        self.assertEqual(
            (noted["statement"], noted["dialect"], noted["database"]),
            (
                "insert into shop.big select * from shop.orders where n > %s",
                "postgres",
                "shop",
            ),
        )
        self.assertIn(
            {"path": "arguments.options.api_key", "reason": "secret redacted"},
            sent["excluded"],
        )

    def test2(self) -> None:
        """
        Test that each switch keeps its own part behind, including the
        source text Prefect 3.8 keeps on the task itself.
        """
        sent = self._send(
            {
                _SEND_SOURCE_ENV: "false",
                _SEND_ARGUMENTS_ENV: "false",
                "CONVALESCE_SQL_CAPTURE": "false",
            }
        )
        payload = sent["payload"]
        self.assertNotIn("source", payload["task"])
        self.assertNotIn("source_code", payload["task"])
        self.assertNotIn("arguments", payload)
        self.assertNotIn("sql_capture", payload)
        self.assertIn(
            {"path": "task.source_code", "reason": "excluded by name"},
            sent["excluded"],
        )

    def test3(self) -> None:
        """
        Test that a task still running sends its arguments and keeps its
        statements for the event that ends it.
        """

        class _Running:
            """Stands in for a state that is not over."""

            def is_running(self) -> bool:
                """Whether the run is still going."""
                return True

        payload = self._send({}, state=_Running())["payload"]
        self.assertEqual(payload["arguments"]["table"], "orders")
        self.assertNotIn("sql_capture", payload)
        ended = self._send({})["payload"]
        self.assertEqual(ended["sql_capture"]["statements"][0]["count"], 2)

    def test4(self) -> None:
        """
        Test that a flow event carries the flow's source, and what the
        flow itself sent outside any task.
        """
        flow = _Flow()
        flow.fn = _load  # type: ignore[attr-defined]
        flow_run = _FlowRun()
        modules = {
            "prefect.context": types.SimpleNamespace(
                TaskRunContext=types.SimpleNamespace(get=lambda: None),
                FlowRunContext=types.SimpleNamespace(
                    get=lambda: types.SimpleNamespace(flow_run=flow_run)
                ),
            )
        }
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, modules):
            cesqlcap.record("truncate shop.staging", via="test")
            cephooks.emit_flow_run(
                flow=flow, flow_run=flow_run, state="S", emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertIn("def _load(", payload["flow"]["source"]["text"])
        self.assertEqual(
            [row["statement"] for row in payload["sql_capture"]["statements"]],
            ["truncate shop.staging"],
        )
        self.assertNotIn("arguments", payload)


# #############################################################################
# Test_deployment1
# #############################################################################


class _Deployment:
    """Stands in for the `Deployment` the API's `read_deployment` returns."""

    def __init__(self) -> None:
        self.id = "d-1"
        self.name = "nightly"
        self.entrypoint = "flows/orders.py:nightly"
        self.path = "."
        self.job_variables = {"env": {"DB_PASSWORD": "hunter2"}}
        self.pull_steps = [{"git_clone": {"access_token": "tok"}}]


class _DeployedRun(_FlowRun):
    """Stands in for a flow run a deployment started."""

    def __init__(self) -> None:
        super().__init__()
        self.deployment_id = "d-1"


class Test_deployment1(unittest.TestCase):
    """
    Test reading where a deployed run's code is.
    """

    def _send(self, client: Any, flow_run: Any) -> Dict[str, Any]:
        """
        Fire the flow hook against a faked API.

        :param client: the client the API reads go through
        :param flow_run: the flow run the hook is given
        :return: the payload sent
        """
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, _prefect_modules(client)):
            cephooks.emit_flow_run(
                flow=_Flow(), flow_run=flow_run, state="S", emitter=recorder
            )
        payload: Dict[str, Any] = recorder.sent[0]["payload"]
        return payload

    def test1(self) -> None:
        """
        Test that the entrypoint, path and name cross, and nothing else of
        the deployment does.
        """
        client = _SyncClient()
        client.read_deployment = lambda _id: _Deployment()  # type: ignore[attr-defined]
        payload = self._send(client, _DeployedRun())
        self.assertEqual(
            payload["api_deployment"],
            {
                "id": "d-1",
                "name": "nightly",
                "entrypoint": "flows/orders.py:nightly",
                "path": ".",
            },
        )
        self.assertNotIn("hunter2", json.dumps(payload))

    def test2(self) -> None:
        """
        Test that a client with no typed method is asked by path, and that
        a run no deployment started asks nothing.
        """

        class _Raw(_SyncClient):
            """A client that answers the deployment only by its path."""

            def _request(self, method: str, path: str) -> _APIResponse:
                self.calls.append(("request", method, path))
                if path.startswith("/deployments/"):
                    return _APIResponse(
                        {"name": "nightly", "entrypoint": "a.py:f", "tags": []}
                    )
                return _APIResponse({})

        client = _Raw()
        payload = self._send(client, _DeployedRun())
        self.assertEqual(
            payload["api_deployment"],
            {"name": "nightly", "entrypoint": "a.py:f"},
        )
        self.assertIn(("request", "GET", "/deployments/d-1"), client.calls)
        plain = _Raw()
        self.assertNotIn("api_deployment", self._send(plain, _FlowRun()))
        self.assertNotIn(("request", "GET", "/deployments/d-1"), plain.calls)

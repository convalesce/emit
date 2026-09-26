"""
Tests for the flow and task state hooks.

Run with `make test`.
"""

import logging
import os
import sys
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest import mock

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
        self.assertEqual(detail["type"], "ValueError")
        self.assertEqual(detail["message"], "bad row")
        self.assertIn("KeyError", detail["traceback"])
        self.assertIn("_raised", detail["traceback"])

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
            self.assertEqual(detail["type"], "ValueError")

    def test3(self) -> None:
        """
        Test that a long traceback keeps its tail, where it failed.
        """
        exc = _raised()
        with mock.patch.object(
            cephooks.traceback,
            "format_exception",
            return_value=["head" + "x" * 20000, "tail"],
        ):
            detail = cephooks.error_detail(_State(True, exc))
        assert detail is not None
        self.assertEqual(len(detail["traceback"]), 16000)
        self.assertTrue(detail["traceback"].endswith("tail"))

    def test4(self) -> None:
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

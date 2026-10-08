"""
Tests for the flow and task state hooks.

Run with `make test`.
"""

import asyncio
import json
import logging
import os
import sys
import threading
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


def _graph_v2() -> Dict[str, Any]:
    """`/flow_runs/{id}/graph-v2` as Prefect Cloud answered it for a run of
    two task runs, the second fed by the first, with an artifact on each
    level."""
    artifact = {"id": "a1", "key": "report", "type": "table", "data": "rows"}
    first = {
        "kind": "task-run",
        "id": "t1",
        "label": "extract-12f",
        "state_type": "COMPLETED",
        "start_time": "2026-10-07T23:05:00Z",
        "end_time": "2026-10-07T23:05:01Z",
        "parents": [],
        "children": [{"id": "t2"}],
        "encapsulating": [],
        "artifacts": [artifact],
    }
    second = {
        **first,
        "id": "t2",
        "label": "load-9e6",
        "parents": [{"id": "t1"}],
        "children": [],
        "artifacts": [],
    }
    return {
        "start_time": "2026-10-07T23:05:00Z",
        "end_time": "2026-10-07T23:05:03Z",
        "root_node_ids": ["t1"],
        "nodes": [["t1", first], ["t2", second]],
        "artifacts": [artifact],
        "states": [{"id": "s1", "type": "RUNNING", "name": "Running"}],
    }


class _SyncClient:
    """Stands in for `SyncPrefectClient`, configurably missing a method or
    two, the way Prefect 2's actually is."""

    def __init__(
        self,
        task_pages: Optional[List[List[Any]]] = None,
        has_read_flow: bool = True,
        has_request: bool = True,
        graph_v2: Any = None,
    ) -> None:
        self.calls: List[Any] = []
        self._task_pages = task_pages or []
        # What each raw path answers with, by its last segment.
        self.bodies: Dict[str, Any] = {
            "graph": [{"id": "t2", "upstream_dependencies": [{"id": "t1"}]}],
            "graph-v2": _graph_v2() if graph_v2 is None else graph_v2,
        }
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
        return _APIResponse(self.bodies.get(path.rsplit("/", 1)[-1]))


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
        # The newer graph answered, so the older one was never asked for.
        graph = payload["api_flow_run_graph_v2"]
        self.assertEqual([node["id"] for node in graph["nodes"]], ["t1", "t2"])
        self.assertEqual(graph["nodes"][1]["parents"], [{"id": "t1"}])
        self.assertEqual(graph["root_node_ids"], ["t1"])
        self.assertNotIn("api_flow_run_graph", payload)
        self.assertNotIn(
            "/flow_runs/a64690f5/graph", [call[-1] for call in client.calls]
        )
        # An artifact is customer-authored: none crosses, on either level.
        self.assertNotIn("artifacts", graph)
        self.assertNotIn("artifacts", graph["nodes"][0])
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
# Test_cloud_workspace_name1
# #############################################################################


class _Workspace:
    """Stands in for one of Prefect Cloud's workspaces."""

    def __init__(self, workspace_id: str, workspace_name: Any) -> None:
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name


class _CloudClient:
    """Stands in for Prefect's Cloud client, asynchronous on both majors."""

    def __init__(self, workspaces: Any, calls: List[str]) -> None:
        self._workspaces = workspaces
        self._calls = calls

    async def __aenter__(self) -> "_CloudClient":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def read_workspaces(self) -> Any:
        """Every workspace the key can see, or the failure to list them."""
        self._calls.append("read_workspaces")
        if isinstance(self._workspaces, Exception):
            raise self._workspaces
        return self._workspaces


def _cloud_modules(workspaces: Any, calls: List[str]) -> Dict[str, Any]:
    """The `prefect.client.cloud` module the name is read through, faked."""
    cloud = types.SimpleNamespace(
        get_cloud_client=lambda: _CloudClient(workspaces, calls)
    )
    return {"prefect.client.cloud": cloud}


_CLOUD_URL = "https://api.prefect.cloud/api/accounts/acct-1/workspaces/ws-1"


class Test_cloud_workspace_name1(unittest.TestCase):
    """
    Test that the workspace's name is read from Prefect Cloud once, and
    that nothing about the read can cost an event or hold a flow.
    """

    def setUp(self) -> None:
        cephooks._WORKSPACE_NAMES.clear()  # pylint: disable=protected-access
        self.addCleanup(
            cephooks._WORKSPACE_NAMES.clear  # pylint: disable=protected-access
        )

    def test1(self) -> None:
        """
        Test that the name is sent beside the ids, and read only once
        however many events are sent.
        """
        calls: List[str] = []
        workspaces = [
            _Workspace("ws-0", "sandbox"),
            _Workspace("ws-1", "analytics"),
        ]
        with mock.patch.dict(os.environ, {"PREFECT_API_URL": _CLOUD_URL}):
            with mock.patch.dict(sys.modules, _cloud_modules(workspaces, calls)):
                first = cephooks.api_state({})
                second = cephooks.api_state({})
        expected = {
            "api_cloud_workspace": {
                "account_id": "acct-1",
                "workspace_id": "ws-1",
                "workspace_name": "analytics",
            }
        }
        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        self.assertEqual(calls, ["read_workspaces"])

    def test2(self) -> None:
        """
        Test that a Cloud that refuses the read leaves only the ids, and is
        not asked again.
        """
        calls: List[str] = []
        modules = _cloud_modules(RuntimeError("401"), calls)
        with mock.patch.dict(os.environ, {"PREFECT_API_URL": _CLOUD_URL}):
            with mock.patch.dict(sys.modules, modules):
                first = cephooks.api_state({})
                second = cephooks.api_state({})
        expected = {
            "api_cloud_workspace": {
                "account_id": "acct-1",
                "workspace_id": "ws-1",
            }
        }
        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        self.assertEqual(calls, ["read_workspaces"])

    def test3(self) -> None:
        """
        Test that a Prefect without the Cloud client names no workspace.
        """
        with mock.patch.dict(sys.modules, {"prefect.client.cloud": None}):
            self.assertIsNone(cephooks.cloud_workspace_name("ws-1"))

    def test4(self) -> None:
        """
        Test that a workspace the key cannot see, or one without a name,
        is not named.
        """
        calls: List[str] = []
        workspaces = [_Workspace("ws-0", "sandbox"), _Workspace("ws-1", None)]
        with mock.patch.dict(sys.modules, _cloud_modules(workspaces, calls)):
            self.assertIsNone(cephooks.cloud_workspace_name("ws-1"))
            self.assertIsNone(cephooks.cloud_workspace_name("ws-2"))
        self.assertEqual(calls, ["read_workspaces", "read_workspaces"])

    def test5(self) -> None:
        """
        Test that a slow read holds the first event only for the timeout,
        and that its answer rides on the events that follow.
        """
        release = threading.Event()
        done = threading.Event()

        def slow_read(_workspace_id: str, found: Dict[str, str]) -> None:
            release.wait(5)
            found["name"] = "analytics"
            done.set()

        with (
            mock.patch.object(cephooks, "_WORKSPACE_NAME_TIMEOUT_SECONDS", 0.01),
            mock.patch.object(cephooks, "_read_workspace_name", slow_read),
        ):
            self.assertIsNone(cephooks.cloud_workspace_name("ws-1"))
            release.set()
            self.assertTrue(done.wait(5))
            self.assertEqual(cephooks.cloud_workspace_name("ws-1"), "analytics")

    def test6(self) -> None:
        """
        Test that a read slower than the timeout is given up on.
        """

        async def never(_cloud: Any) -> List[Any]:
            await asyncio.sleep(5)
            return [_Workspace("ws-1", "analytics")]

        found: Dict[str, str] = {}
        with (
            mock.patch.object(cephooks, "_WORKSPACE_NAME_TIMEOUT_SECONDS", 0.01),
            mock.patch.object(cephooks, "_read_workspaces", never),
        ):
            with mock.patch.dict(sys.modules, _cloud_modules([], [])):
                # pylint: disable-next=protected-access
                cephooks._read_workspace_name("ws-1", found)
        self.assertEqual(found, {})

    def test7(self) -> None:
        """
        Test that a process that cannot start a thread still gets the ids,
        and that a self-hosted server is never asked.
        """
        with mock.patch.object(
            cephooks.threading,
            "Thread",
            side_effect=RuntimeError("no threads"),
        ) as thread:
            with mock.patch.dict(os.environ, {"PREFECT_API_URL": _CLOUD_URL}):
                state = cephooks.api_state({})
            self.assertEqual(thread.call_count, 1)
            url = "https://prefect.internal.example.com/api"
            with mock.patch.dict(os.environ, {"PREFECT_API_URL": url}):
                self.assertEqual(cephooks.api_state({}), {})
            self.assertEqual(thread.call_count, 1)
        self.assertEqual(
            state["api_cloud_workspace"],
            {"account_id": "acct-1", "workspace_id": "ws-1"},
        )


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


class _PydanticV1Result:
    """
    Stands in for Prefect 2.20's `UnpersistedResult`, a pydantic 1 model.

    Its fields are its `__dict__`; the private `_cache`, which holds what
    the task returned or raised, is a slot of the class.
    """

    __slots__ = ("__dict__", "_cache")

    def __init__(self, value: Any) -> None:
        object.__setattr__(
            self,
            "__dict__",
            {
                "type": "unpersisted",
                "artifact_type": None,
                "artifact_description": None,
            },
        )
        object.__setattr__(self, "_cache", value)


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

    def test6(self) -> None:
        """
        Test that the exception a Prefect 2.20 result holds is found: the
        result is a pydantic 1 model, whose `_cache` is a slot of the class,
        in neither its `__dict__` nor a `__pydantic_private__`.
        """
        data = _PydanticV1Result(_raised())
        self.assertNotIn("_cache", data.__dict__)
        with self.assertRaises(AttributeError):
            object.__getattribute__(data, "__pydantic_private__")
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, {"prefect.context": None}):
            cephooks.emit_task_run(
                task="T",
                task_run=_TaskRun(),
                state=_State(True, data),
                emitter=recorder,
            )
        detail = recorder.sent[0]["payload"]["error_detail"]
        self.assertEqual(detail["type"], "builtins.ValueError")
        self.assertIn("_raised", detail["traceback"])
        self.assertEqual(detail["cause"]["type"], "builtins.KeyError")

    def test7(self) -> None:
        """
        Test that only storage is read, and nothing raises: a slot never
        set holds nothing, a property of the same name is not called, and
        an object whose own storage cannot be read holds nothing.
        """

        class Unset(_PydanticV1Result):
            """A result whose cache was never filled."""

            def __init__(self) -> None:  # pylint: disable=super-init-not-called
                object.__setattr__(self, "__dict__", {"type": "unpersisted"})

        class Loads:
            """A result that reads storage when asked for its cache."""

            @property
            def _cache(self) -> Any:
                raise AssertionError("read the cache from storage")

        class Closed:
            """An object that refuses every read of itself."""

            __slots__ = ()

            def __getattribute__(self, name: str) -> Any:
                raise RuntimeError(name)

        for data in (Unset(), Loads(), Closed(), 7):
            self.assertIsNone(cephooks.error_detail(_State(True, data)))


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


# #############################################################################
# Test_configured_api_url1
# #############################################################################


def _settings_modules(value: Any) -> Dict[str, Any]:
    """A stand-in `prefect.settings` whose API URL setting answers `value`,
    or raises it."""

    def read() -> Any:
        if isinstance(value, Exception):
            raise value
        return value

    setting = types.SimpleNamespace(value=read)
    return {"prefect.settings": types.SimpleNamespace(PREFECT_API_URL=setting)}


class Test_configured_api_url1(unittest.TestCase):
    """
    Test that the API URL is read the way Prefect resolved it, so a
    workspace signed in to by profile is found without the environment.
    """

    def test1(self) -> None:
        """
        Test that a URL only a profile holds still names the workspace.
        """
        env = {
            key: value
            for key, value in os.environ.items()
            if key != "PREFECT_API_URL"
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch.dict(sys.modules, _settings_modules(_CLOUD_URL)):
                workspace = cephooks.cloud_workspace()
        self.assertEqual(
            workspace, {"account_id": "acct-1", "workspace_id": "ws-1"}
        )

    def test2(self) -> None:
        """
        Test that settings that cannot be read, or that hold no URL, leave
        the environment's.
        """
        for value in (RuntimeError("no profile"), None):
            with self.subTest(value=value):
                env = {"PREFECT_API_URL": _CLOUD_URL}
                with mock.patch.dict(os.environ, env, clear=False):
                    with mock.patch.dict(sys.modules, _settings_modules(value)):
                        url = cephooks.configured_api_url()
                self.assertEqual(url, _CLOUD_URL)


# #############################################################################
# Test_read_run1
# #############################################################################


class _Typed:
    """Stands in for a state's type, an enum on every supported Prefect."""

    def __init__(self, value: str) -> None:
        self.value = value


class _Ended:
    """Stands in for the state a flow hook is handed."""

    def __init__(self, kind: str) -> None:
        self.type = _Typed(kind)
        self.name = kind.title()


class _ListedRun:
    """Stands in for a `TaskRun` as the API lists it."""

    def __init__(self, run_id: str, kind: str = "COMPLETED") -> None:
        self.id = run_id
        self.state_type = _Typed(kind)


class _LateClient(_SyncClient):
    """A client whose list of task runs catches up read by read, the way
    Prefect Cloud's does."""

    def __init__(self, reads: List[List[Any]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._reads = reads
        self.listed = 0

    def read_task_runs(
        self, *, flow_run_filter: Any, limit: int, offset: int
    ) -> List[Any]:
        """The next read's task runs; the last read's from then on."""
        read = self._reads[min(self.listed, len(self._reads) - 1)]
        self.listed += 1
        return read


class _Clock:
    """Time that passes only while something waits on it."""

    def __init__(self) -> None:
        self.now = 0.0
        self.waits: List[float] = []

    def monotonic(self) -> float:
        """The time so far."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Wait, by moving the time on."""
        self.waits.append(seconds)
        self.now += seconds


class Test_read_run1(unittest.TestCase):
    """
    Test the read of a run's graph and task runs: which graph is sent, and
    the wait for the API's list at the hook that ends the flow run.
    """

    # The waits, the registry and the mask are private to the plugin.
    # pylint: disable=protected-access

    def setUp(self) -> None:
        self.clock = _Clock()
        patcher = mock.patch.object(cephooks, "time", self.clock)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(cephooks._HOOKED.clear)

    def _flow_event(self, client: Any, kind: str) -> Dict[str, Any]:
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, _prefect_modules(client)):
            cephooks.emit_flow_run(
                flow=_Flow(),
                flow_run=_FlowRun(),
                state=_Ended(kind),
                emitter=recorder,
            )
        payload: Dict[str, Any] = recorder.sent[0]["payload"]
        return payload

    def test1(self) -> None:
        """
        Test that a server without the newer graph is asked for the older
        one, which crosses under its own name.
        """
        client = _SyncClient(graph_v2={"detail": "Not Found"})
        payload = self._flow_event(client, "RUNNING")
        self.assertNotIn("api_flow_run_graph_v2", payload)
        self.assertEqual(payload["api_flow_run_graph"][0]["id"], "t2")

    def test2(self) -> None:
        """
        Test that the hook that ends the flow run waits for a list that is
        shorter than the graph, and stops as soon as it is whole.
        """
        late = [[], [_ListedRun("t1")], [_ListedRun("t1"), _ListedRun("t2")]]
        client = _LateClient(late)
        payload = self._flow_event(client, "COMPLETED")
        self.assertEqual(len(payload["api_task_runs"]), 2)
        self.assertEqual(self.clock.waits, [0.25, 0.5])

    def test3(self) -> None:
        """
        Test that an empty list is waited on when a task hook fired in this
        process, though the graph is empty too.
        """
        task_run = _TaskRun()
        task_run.id = "t9"  # type: ignore[attr-defined]
        cephooks.emit_task_run(
            task=object(), task_run=task_run, state="S", emitter=_Recorder()
        )
        client = _LateClient([[], [_ListedRun("t9")]], graph_v2={"nodes": []})
        payload = self._flow_event(client, "FAILED")
        self.assertEqual(len(payload["api_task_runs"]), 1)
        self.assertEqual(self.clock.waits, [0.25])
        # Read once, then forgotten.
        self.assertEqual(cephooks._HOOKED, {})

    def test4(self) -> None:
        """
        Test that a task run listed as still running is waited on, and that
        the wait never passes its budget.
        """
        stuck = [[_ListedRun("t1"), _ListedRun("t2", "RUNNING")]]
        client = _LateClient(stuck)
        payload = self._flow_event(client, "CRASHED")
        self.assertEqual(len(payload["api_task_runs"]), 2)
        self.assertEqual(self.clock.waits, [0.25, 0.5, 1.0, 1.5, 1.5])
        self.assertLessEqual(
            sum(self.clock.waits), cephooks._COMPLETE_READ_BUDGET_SECONDS
        )

    def test5(self) -> None:
        """
        Test that a wait that would pass the budget, counting a read as
        slow as the last, is not started.
        """
        client = _LateClient([[]])
        real = client.read_task_runs

        def slow(**kwargs: Any) -> List[Any]:
            self.clock.now += 1.0
            return real(**kwargs)

        client.read_task_runs = slow  # type: ignore[method-assign]
        with self.assertLogs(cephooks._LOG, level="DEBUG") as logs:
            payload = self._flow_event(client, "CANCELLED")
        self.assertNotIn("api_task_runs", payload)
        # 1.0 to read, then 0.25 and 0.5 each with a read after it end at
        # 3.75; a wait of 1.0 and one more read would end past the budget.
        self.assertEqual(self.clock.waits, [0.25, 0.5])
        self.assertTrue(any("incompletely" in line for line in logs.output))

    def test6(self) -> None:
        """
        Test that nothing waits where the flow is still running, at a task
        hook, or where the list is already whole.
        """
        client = _LateClient([[]])
        self._flow_event(client, "RUNNING")
        recorder = _Recorder()
        with mock.patch.dict(sys.modules, _prefect_modules(_LateClient([[]]))):
            cephooks.emit_task_run(
                task=object(),
                task_run=_TaskRun(),
                flow=_Flow(),
                flow_run=_FlowRun(),
                state=_Ended("COMPLETED"),
                emitter=recorder,
            )
        whole = _LateClient([[_ListedRun("t1"), _ListedRun("t2")]])
        self._flow_event(whole, "COMPLETED")
        self.assertEqual(self.clock.waits, [])
        self.assertIn("api_flow_run_graph_v2", recorder.sent[0]["payload"])

    def test7(self) -> None:
        """
        Test that a task run whose result Prefect is tracking is waited for,
        though no hook is attached to it and the graph is empty.
        """

        class _Tracked:
            """Stands in for the state of a task run that returned."""

            def __init__(self, run_id: Any) -> None:
                self.state_details = types.SimpleNamespace(task_run_id=run_id)

        context = _Context()
        # Prefect 3 keeps the state first in a tuple; Prefect 2 keeps it
        # bare. A subflow's state names no task run.
        setattr(
            context,
            "run_results",
            {1: (_Tracked("t1"), "task_run", None), 2: (_Tracked(None),)},
        )
        setattr(context, "task_run_results", {3: _Tracked("t2")})
        client = _LateClient(
            [[], [_ListedRun("t1")], [_ListedRun("t1"), _ListedRun("t2")]],
            graph_v2={"nodes": []},
        )
        modules = {"prefect.context": _ContextModule(context)}
        with mock.patch.dict(sys.modules, modules):
            payload = self._flow_event(client, "COMPLETED")
        self.assertEqual(len(payload["api_task_runs"]), 2)
        self.assertEqual(self.clock.waits, [0.25, 0.5])

    def test8(self) -> None:
        """
        Test that the context of another flow run, or one that cannot be
        read, names no task run.
        """
        other = _Context()
        other.flow_run.id = "another"
        setattr(other, "run_results", {1: (object(),)})
        modules = {"prefect.context": _ContextModule(other)}
        with mock.patch.dict(sys.modules, modules):
            self.assertEqual(cephooks._tracked_task_runs("a64690f5"), set())
        broken = types.SimpleNamespace()
        with mock.patch.dict(sys.modules, {"prefect.context": broken}):
            self.assertEqual(cephooks._tracked_task_runs("a64690f5"), set())

    def test9(self) -> None:
        """
        Test that the task runs remembered per flow run are bounded, oldest
        flow run first, and that a task run naming no flow run is not kept.
        """
        with mock.patch.object(cephooks, "_HOOKED_FLOW_RUNS", 2):
            for flow_run_id in ("f1", "f2", "f3"):
                run = types.SimpleNamespace(id="t1", flow_run_id=flow_run_id)
                cephooks._note_hooked(run)
            cephooks._note_hooked(types.SimpleNamespace(id="t1"))
        self.assertEqual(list(cephooks._HOOKED), ["f2", "f3"])

    def test10(self) -> None:
        """
        Test that a raw record of a task run reads as a typed one does.
        """
        graph = {"nodes": [{"kind": "flow-run", "id": "child"}]}
        listed = [{"id": "t1", "state_type": "COMPLETED"}]
        self.assertFalse(cephooks._incomplete(graph, listed, {"t1"}))
        self.assertTrue(cephooks._incomplete(None, listed, {"t2"}))


# #############################################################################
# Test_withhold_private1
# #############################################################################


class Test_withhold_private1(unittest.TestCase):
    """
    Test what is taken out of a payload because it names a person or a
    place on disk.
    """

    def test1(self) -> None:
        """
        Test that who started a run keeps only its type, on the hook's
        flow run and on the API's.
        """
        who = {"id": "u-1", "type": "USER", "display_value": "a-handle"}
        body = {
            "flow_run": {"created_by": dict(who)},
            "api_flow_run": {"created_by": dict(who)},
        }
        out, excluded = cephooks.withhold_private(body)
        self.assertEqual(out["flow_run"]["created_by"], {"type": "USER"})
        self.assertEqual(out["api_flow_run"]["created_by"], {"type": "USER"})
        self.assertEqual(
            [each["path"] for each in excluded],
            [
                "flow_run.created_by.id",
                "flow_run.created_by.display_value",
                "api_flow_run.created_by.id",
                "api_flow_run.created_by.display_value",
            ],
        )

    def test2(self) -> None:
        """
        Test that a starter dumped as its text is dropped whole, and that a
        run nobody is named on is left alone.
        """
        body = {
            "flow_run": {"created_by": "CreatedBy(display_value='a-handle')"},
            "api_flow_run": {"created_by": None},
        }
        out, excluded = cephooks.withhold_private(body)
        self.assertEqual(out["flow_run"], {})
        self.assertEqual(out["api_flow_run"], {"created_by": None})
        self.assertEqual(
            [each["path"] for each in excluded], ["flow_run.created_by"]
        )
        self.assertEqual(cephooks.withhold_private("text"), ("text", []))

    def test3(self) -> None:
        """
        Test that a persisted result's record is taken off every run in a
        list of runs, and named in what was excluded.
        """
        stored = {"storage_key": "/home/a-handle/results/abc"}
        body = {
            "api_task_runs": [
                {"id": "t1", "state": {"type": "COMPLETED", "data": None}},
                {"id": "t2", "state": {"type": "COMPLETED", "data": stored}},
                "a task run dumped as text",
            ],
            "api_flow_run_graph": [{"id": "t2", "state": {"data": stored}}],
        }
        out, excluded = cephooks.withhold_private(body)
        self.assertNotIn("a-handle", json.dumps(out))
        self.assertEqual(
            [each["path"] for each in excluded],
            ["api_task_runs[1].state.data", "api_flow_run_graph[0].state.data"],
        )

    def test4(self) -> None:
        """
        Test that an event sends neither, end to end: the API's flow run
        loses its state's data and its starter's name.
        """

        class _Named(_SyncClient):
            """A client whose flow run was started by a person."""

            def read_flow_run(self, flow_run_id: str) -> Any:
                """The flow run record, as Prefect Cloud returns it."""
                return {
                    "id": flow_run_id,
                    "created_by": {"id": "u-1", "type": "USER"},
                    "state": {"data": {"storage_key": "/home/a-handle/r"}},
                }

        recorder = _Recorder()
        with mock.patch.dict(sys.modules, _prefect_modules(_Named())):
            cephooks.emit_flow_run(
                flow=_Flow(), flow_run=_FlowRun(), state="S", emitter=recorder
            )
        sent = recorder.sent[0]
        record = sent["payload"]["api_flow_run"]
        self.assertEqual(record["created_by"], {"type": "USER"})
        self.assertNotIn("data", record["state"])
        paths = [each["path"] for each in sent["excluded"]]
        self.assertIn("api_flow_run.state.data", paths)
        self.assertIn("api_flow_run.created_by.id", paths)


# #############################################################################
# Test_masked_logs1
# #############################################################################


class Test_masked_logs1(unittest.TestCase):
    """
    Test that no line the plugin logs names a Cloud account or workspace.
    """

    # The waits, the registry and the mask are private to the plugin.
    # pylint: disable=protected-access

    def test1(self) -> None:
        """
        Test that a failed read, whose error quotes the URL it failed on,
        is logged with both ids masked.
        """

        class _Failing(_SyncClient):
            """A client whose raw reads fail the way Prefect Cloud's do."""

            def _request(self, method: str, path: str) -> Any:
                raise RuntimeError(
                    f"Server error '500' for url '{_CLOUD_URL}{path}'"
                )

        client = _Failing(has_read_flow=False)
        with self.assertLogs(cephooks._LOG, level="DEBUG") as logs:
            with mock.patch.dict(sys.modules, _prefect_modules(client)):
                cephooks.emit_flow_run(
                    flow=_Flow(),
                    flow_run=_FlowRun(),
                    state="S",
                    emitter=_Recorder(),
                )
        text = "\n".join(logs.output)
        self.assertIn("accounts/***/workspaces/***/flow_runs/a64690f5", text)
        self.assertNotIn("acct-1", text)
        self.assertNotIn("ws-1", text)

    def test2(self) -> None:
        """
        Test that every module of the plugin logs through the mask, and
        that the mask is put on a logger once.
        """
        import convalesce_emit_prefect._lineage as celin
        import convalesce_emit_prefect._mask as cemask
        import convalesce_emit_prefect.retry as ceretry

        for module in (cephooks, cecap, celin, ceretry):
            with self.subTest(module=module.__name__):
                log = cemask.logger(module.__name__)
                self.assertIs(log, module._LOG)
                masks = [
                    each
                    for each in log.filters
                    if isinstance(each, cemask._MaskIds)
                ]
                self.assertEqual(len(masks), 1)

    def test3(self) -> None:
        """
        Test that a record whose message cannot be formatted is passed on
        as it is.
        """
        import convalesce_emit_prefect._mask as cemask

        record = logging.LogRecord(
            "name", logging.DEBUG, __file__, 1, "%d", ("not a number",), None
        )
        self.assertTrue(cemask._MaskIds().filter(record))
        self.assertEqual(record.args, ("not a number",))

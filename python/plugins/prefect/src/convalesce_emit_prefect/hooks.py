"""
Prefect flow and task state hooks.

Exports plain functions rather than decorated hooks: Prefect's signatures
have moved between versions, and this package deliberately does not depend on
Prefect, so the customer wires them up themselves.

A task-run hook is handed the task, the task run and the state, and the task
run names its flow run only by id. The flow's name is what a receiver builds
the pipeline from, and the flow-run hook that carries it fires last, after
every task hook, so a task run on its own said nothing about what it belonged
to. Prefect puts both on the run context while a task hook runs, so they are
read from there and sent alongside. The import of that context is late and
guarded, so this module still imports where Prefect is absent.

Import as:

import convalesce_emit_prefect.hooks as cephooks
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit_prefect._lineage as celin

_LOG = logging.getLogger(__name__)

# Dagster and Prefect both forward a hook payload with the same five-line
# call to `cemit.send_one`, differing only in their constants. Pulling that
# into a shared wrapper would add an indirection carrying nothing, so the
# similarity stays and the check is turned off here rather than everywhere.
# pylint: disable=duplicate-code


TOOL = "prefect"

# Where Prefect keeps the running flow while a task hook fires. Both majors
# expose `FlowRunContext.get()` with `flow` and `flow_run` on it.
_FLOW_CONTEXT = "prefect.context"

# One flag gates every API read below, default on: a customer whose Prefect
# API is locked down to the process that owns it can turn this off and still
# get everything the hook's own arguments carry.
_API_READS_ENV = "CONVALESCE_PREFECT_API_READS"
_FALSY = frozenset({"0", "false", "no", "off"})

# How many task runs one page asks for, and how many pages a single event
# will follow before stopping -- a backstop against a flow with an
# unreasonable number of tasks, not a real limit on any run this was built
# against.
_TASK_RUN_PAGE_SIZE = 200
_TASK_RUN_PAGE_BACKSTOP = 50

# A state's data is what the flow or task returned -- the customer's own
# data, which never crosses -- or, on a failure, the exception, which crosses
# as `error_detail` instead. The run objects carry their own copy of it.
_SKIP = frozenset({"state.data", "flow_run.state.data", "task_run.state.data"})

# A flow's parameters are whatever launched the run typed -- a customer id, a
# date range, a bucket -- and nothing a receiver reads. Their names and types
# cross; their values only when a customer opts in.
_SEND_PARAMETERS_ENV = "CONVALESCE_PREFECT_SEND_PARAMETERS"
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_PARAMETER_VALUES = (("flow_run", "parameters"), ("api_flow_run", "parameters"))
# The flow's own parameter schema carries each parameter's default value.
_PARAMETER_SCHEMA = ("flow", "parameters", "properties")
_PARAMETERS_WITHHELD = "parameter values withheld"
# Stamped by `cemit.dump` on a mapping something else points at; kept as is.
_REF_ID = "$id"

# A Prefect Cloud API URL names the account and the workspace in its own
# path; nothing about a run has to be read to find them.
_CLOUD_URL_RE = re.compile(
    r"^https://api\.prefect\.cloud/api/accounts/(?P<account>[^/]+)"
    r"/workspaces/(?P<workspace>[^/]+)/?"
)


def _emit(
    event: str,
    payload: Dict[str, Any],
    emitter: Optional[cemit.EmitterLike] = None,
) -> None:
    """
    Send one Prefect event.

    :param event: which hook fired
    :param payload: Prefect's own objects
    :param emitter: emitter to send through
    :return: nothing
    """
    try:
        payload = {**payload, **api_state(payload)}
        budget = cemit.new_budget(skip=_SKIP)
        dumped = cemit.dump(payload, budget=budget)
        withheld: List[Dict[str, str]] = []
        if not send_parameters():
            dumped, withheld = withhold_parameters(dumped)
        # A run's job variables and, when sent, its parameters are whatever
        # launched it typed, and either can hold a literal credential.
        dumped, secrets = cemit.redact_secrets(dumped)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A Prefect hook that raises is logged by Prefect as the flow's own
        # failure; nothing about reporting on it is worth that.
        _LOG.warning("convalesce: could not shape %s: %s", event, exc)
        return
    cemit.send_one(
        tool=TOOL,
        event=event,
        payload=dumped,
        emitter=emitter,
        tool_version=cemit.version_of("prefect"),
        excluded=budget.excluded + withheld + secrets,
    )


# #############################################################################
# Parameters
# #############################################################################


def send_parameters() -> bool:
    """
    Whether flow parameter values cross, off by default.

    :return: whether `CONVALESCE_PREFECT_SEND_PARAMETERS` is set truthy
    """
    raw = os.environ.get(_SEND_PARAMETERS_ENV, "")
    return raw.strip().lower() in _TRUTHY


def withhold_parameters(
    body: Any,
) -> Tuple[Any, List[Dict[str, str]]]:
    """
    Replace each flow parameter's value, and each default, with its type.

    The names survive, so a receiver still sees what a run was called with,
    just not with what.

    :param body: the dumped payload
    :return: the same payload, values replaced, and what was withheld, by
        path and reason
    """
    excluded: List[Dict[str, str]] = []
    if not isinstance(body, dict):
        return body, excluded
    for carrier, name in _PARAMETER_VALUES:
        holder = body.get(carrier)
        if not isinstance(holder, dict) or not isinstance(
            holder.get(name), dict
        ):
            continue
        holder[name] = {
            key: value if key == _REF_ID else _type_marker(value)
            for key, value in holder[name].items()
        }
        excluded.append(
            {"path": f"{carrier}.{name}", "reason": _PARAMETERS_WITHHELD}
        )
    properties: Any = body
    for step in _PARAMETER_SCHEMA:
        properties = (
            properties.get(step) if isinstance(properties, dict) else None
        )
    if not isinstance(properties, dict):
        return body, excluded
    for key, schema in properties.items():
        if isinstance(schema, dict) and "default" in schema:
            schema["default"] = _type_marker(schema["default"])
            path = ".".join(_PARAMETER_SCHEMA + (str(key), "default"))
            excluded.append({"path": path, "reason": _PARAMETERS_WITHHELD})
    return body, excluded


def _type_marker(value: Any) -> str:
    """
    What stands in for a withheld value.

    :param value: the dumped value
    :return: its type, as `<str>`, `<int>` and so on
    """
    return f"<{type(value).__name__}>"


# #############################################################################
# API reads
# #############################################################################


def api_reads_enabled() -> bool:
    """
    Whether the four API reads below run at all.

    On by default; a customer with a locked-down Prefect API sets
    `CONVALESCE_PREFECT_API_READS=false` and still gets everything the
    hook's own arguments carry.

    :return: whether this event should also read the API
    """
    raw = os.environ.get(_API_READS_ENV, "")
    return raw.strip().lower() not in _FALSY


def api_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Everything the four API reads add, named so nothing collides with what
    the hook's own arguments already carry (`api_flow`, not `flow`).

    Each read is independently guarded: a Prefect that moved a method, an
    API that is unreachable, or Prefect being entirely absent all mean this
    returns less, never that the event fails to send.

    :param payload: the hook's own arguments, not yet dumped
    :return: whatever could be read, empty when reads are off, Prefect is
        absent, or nothing resolved
    """
    if not api_reads_enabled():
        return {}
    out: Dict[str, Any] = {}
    workspace = cloud_workspace()
    if workspace:
        out["api_cloud_workspace"] = workspace
    flow_run = payload.get("flow_run")
    flow = payload.get("flow")
    flow_run_id = _text_id(getattr(flow_run, "id", None))
    flow_id = _text_id(getattr(flow, "id", None)) or _text_id(
        getattr(flow_run, "flow_id", None)
    )
    if not flow_run_id and not flow_id:
        return out
    try:
        # pylint: disable=import-outside-toplevel
        from prefect.client.orchestration import get_client
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: prefect client unavailable: %s", exc)
        return out
    try:
        with get_client(sync_client=True) as client:
            if flow_id:
                flow_record = _read_flow(client, flow_id)
                if flow_record is not None:
                    out["api_flow"] = flow_record
            if flow_run_id:
                flow_run_record = _call(client, "read_flow_run", flow_run_id)
                if flow_run_record is not None:
                    out["api_flow_run"] = flow_run_record
                graph = _read_graph(client, flow_run_id)
                if graph is not None:
                    out["api_flow_run_graph"] = graph
                task_runs = _read_task_runs(client, flow_run_id)
                if task_runs:
                    out["api_task_runs"] = task_runs
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # An API that is down or refuses the request is the customer's
        # infrastructure, not a reason to lose the event itself.
        _LOG.debug("convalesce: could not read the Prefect API: %s", exc)
    return out


def _text_id(value: Any) -> Optional[str]:
    """
    An id as text, whatever type Prefect gave it.

    :param value: a `UUID`, a string, or nothing
    :return: the text, or None
    """
    return str(value) if value else None


def _call(client: Any, method_name: str, *args: Any) -> Any:
    """
    Call one client method if this Prefect still has it.

    :param client: the sync API client
    :param method_name: the method to call
    :param args: positional arguments to call it with
    :return: what it returned, or None when the method is missing or raised
    """
    method = getattr(client, method_name, None)
    if not callable(method):
        return None
    try:
        return method(*args)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not call %s: %s", method_name, exc)
        return None


def _read_flow(client: Any, flow_id: str) -> Any:
    """
    The flow record, by id.

    `read_flow` is on the async client on every version this was checked
    against, but missing from Prefect 2's *synchronous* client -- found by
    reading the two versions' own source, not assumed from one of them. The
    raw path is the fallback everywhere that method is absent.

    :param client: the sync API client
    :param flow_id: the flow's id
    :return: the flow, forwarded whole; None when it could not be read
    """
    record = _call(client, "read_flow", flow_id)
    if record is not None:
        return record
    return _get(client, f"/flows/{flow_id}")


def _read_graph(client: Any, flow_run_id: str) -> Any:
    """
    The run's task-to-task graph -- the only source of per-node upstream
    dependencies once a run has finished.

    Neither supported major exposes a typed method for this endpoint, on
    the sync client or the async one, so it is always the raw path.

    :param client: the sync API client
    :param flow_run_id: the flow run's id
    :return: the graph, as the API returned it; None when it could not be
        read
    """
    return _get(client, f"/flow_runs/{flow_run_id}/graph")


def _get(client: Any, path: str) -> Any:
    """
    One raw `GET` against the client's own configured API, parsed as JSON.

    `client.request` is the documented way to do this on the clients that
    have it; where it is missing, the underlying HTTP client Prefect itself
    builds is used directly, the same one every typed method already calls
    through.

    :param client: the sync API client
    :param path: the path to request, relative to the API's base URL
    :return: the parsed body; None on any failure
    """
    request = getattr(client, "request", None)
    response = None
    if callable(request):
        try:
            response = request("GET", path)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: GET %s failed: %s", path, exc)
            response = None
    if response is None:
        http_client = getattr(client, "_client", None)
        get = getattr(http_client, "get", None)
        if not callable(get):
            return None
        try:
            response = get(path)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: GET %s failed: %s", path, exc)
            return None
    try:
        return response.json()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug(
            "convalesce: could not parse the response for %s: %s", path, exc
        )
        return None


def _read_task_runs(client: Any, flow_run_id: str) -> List[Any]:
    """
    Every task run belonging to the flow run, paged rather than capped at
    whatever the API's own default page size is.

    :param client: the sync API client
    :param flow_run_id: the flow run's id
    :return: the task runs, in the pages the API returned them; stops after
        `_TASK_RUN_PAGE_BACKSTOP` pages, a safety net against an
        unreasonable run rather than a real limit
    """
    method = getattr(client, "read_task_runs", None)
    if not callable(method):
        return []
    try:
        # pylint: disable=import-outside-toplevel
        from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: prefect filters unavailable: %s", exc)
        return []
    flow_run_filter = FlowRunFilter(id=FlowRunFilterId(any_=[flow_run_id]))
    out: List[Any] = []
    for page in range(_TASK_RUN_PAGE_BACKSTOP):
        try:
            page_runs = method(
                flow_run_filter=flow_run_filter,
                limit=_TASK_RUN_PAGE_SIZE,
                offset=page * _TASK_RUN_PAGE_SIZE,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not read task runs: %s", exc)
            break
        if not page_runs:
            break
        out.extend(page_runs)
        if len(page_runs) < _TASK_RUN_PAGE_SIZE:
            break
    return out


def cloud_workspace() -> Dict[str, str]:
    """
    The Cloud account and workspace this process is configured to talk to.

    Read from `PREFECT_API_URL` itself, which names both in its own path on
    Prefect Cloud (`.../accounts/<id>/workspaces/<id>`) -- nothing about a
    run has to be read to find them, and a self-hosted server's URL simply
    does not match.

    :return: `account_id` and `workspace_id`, or empty when this process is
        not talking to Prefect Cloud
    """
    url = os.environ.get("PREFECT_API_URL", "")
    match = _CLOUD_URL_RE.match(url)
    if not match:
        return {}
    return {
        "account_id": match.group("account"),
        "workspace_id": match.group("workspace"),
    }


def emit_flow_run(
    flow: Any = None,
    flow_run: Any = None,
    state: Any = None,
    emitter: Optional[cemit.EmitterLike] = None,
    **kwargs: Any,
) -> None:
    """
    Flow state-change hook, wired up on the flow itself::

        @flow(on_completion=[emit_flow_run], on_failure=[emit_flow_run])
        def my_flow(): ...

    :param flow: Prefect's flow object
    :param flow_run: Prefect's flow run object
    :param state: the state it moved to
    :param emitter: emitter to send through
    :param kwargs: whatever else Prefect passes; forwarded untouched
    :return: nothing
    """
    payload = {"flow": flow, "flow_run": flow_run, "state": state, **kwargs}
    detail = error_detail(state)
    if detail is not None:
        payload.setdefault("error_detail", detail)
    _emit("flow_run", payload, emitter)


def running_flow() -> Dict[str, Any]:
    """
    The flow and flow run Prefect is running, while a hook fires.

    Read from Prefect's own run context rather than from the task run,
    which names its flow run by id and nothing else. Imported late and
    guarded: outside a flow run, and where Prefect is not installed at all,
    there is simply nothing to add.

    :return: the flow and its run, or nothing when neither is at hand
    """
    try:
        import importlib

        context = importlib.import_module(_FLOW_CONTEXT).FlowRunContext.get()
    except Exception:  # pylint: disable=broad-exception-caught
        # No Prefect, or a Prefect that moved its context: the task run
        # still crosses, it just cannot name its flow.
        return {}
    if context is None:
        return {}
    out: Dict[str, Any] = {}
    for name in ("flow", "flow_run"):
        value = getattr(context, name, None)
        if value is not None:
            out[name] = value
    return out


def emit_task_run(
    task: Any = None,
    task_run: Any = None,
    state: Any = None,
    emitter: Optional[cemit.EmitterLike] = None,
    **kwargs: Any,
) -> None:
    """
    Task state-change hook, wired up the same way via `on_completion`.

    :param task: Prefect's task object
    :param task_run: Prefect's task run object
    :param state: the state it moved to
    :param emitter: emitter to send through
    :param kwargs: whatever else Prefect passes; forwarded untouched
    :return: nothing
    """
    payload = {"task": task, "task_run": task_run, "state": state, **kwargs}
    declared = celin.take(task_run)
    if declared is not None:
        payload.setdefault("lineage", declared)
    detail = error_detail(state)
    if detail is not None:
        payload.setdefault("error_detail", detail)
    # The hook's own arguments win: what the context holds is a fallback for
    # what the task run does not name, never a replacement for it.
    for name, value in running_flow().items():
        payload.setdefault(name, None)
        if payload[name] is None:
            payload[name] = value
    _emit("task_run", payload, emitter)


# #############################################################################
# Failure detail
# #############################################################################


def error_detail(state: Any) -> Optional[Dict[str, Any]]:
    """
    The exception a failed state holds, where it is already in memory.

    A failed state's `message` names the exception but not where it was
    raised. The exception itself is the state's data: on Prefect 3 a result
    record whose `result` is the exception, on Prefect 2 a result holding it
    in its cache, or on either the bare exception. `state.result()` is
    deliberately not called: where results are persisted it reads them back
    from storage, which is the customer's, and may be remote.

    :param state: the state the hook was given
    :return: `cemit.error_detail()` of the exception, or None when the state
        is not a failure or holds no exception here
    """
    try:
        for check in ("is_failed", "is_crashed"):
            method = getattr(state, check, None)
            if callable(method) and method():
                break
        else:
            if callable(getattr(state, "is_failed", None)):
                return None
        exc = _held_exception(getattr(state, "data", None))
        if exc is None:
            return None
        detail: Dict[str, Any] = cemit.error_detail(exc)
        return detail
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the failure: %s", exc)
        return None


def _held_exception(data: Any) -> Optional[BaseException]:
    """
    The exception a state's data holds in memory, without loading anything.

    :param data: `state.data`
    :return: the exception, or None
    """
    if isinstance(data, BaseException):
        return data
    # Read off the instance's own storage, not through attributes: a result
    # that is not in memory may load it from storage on attribute access.
    # Pydantic 2 keeps a private attribute such as `_cache` apart.
    fields: Dict[str, Any] = {}
    for store in ("__dict__", "__pydantic_private__"):
        try:
            fields.update(object.__getattribute__(data, store) or {})
        except (AttributeError, TypeError, ValueError):
            continue
    for name in ("result", "_cache"):
        value = fields.get(name)
        if isinstance(value, BaseException):
            return value
    return None

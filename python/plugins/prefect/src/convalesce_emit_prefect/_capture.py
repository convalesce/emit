"""
What a flow or task ran: its source, what it was called with, its SQL.

Prefect records that a task ran and how it ended, not what the task was.
Three things say that, and each is read here from the running process: the
text of the function, the values it was called with, and the SQL statements
it sent to a database.

The SQL is the awkward one. A flow runs its tasks side by side in one
process -- threads by default, or tasks on one event loop -- so "what ran
between the task starting and ending" would hand one task its neighbour's
statements. Prefect keeps the running task run and flow run on context
variables, which follow the code into each thread and each coroutine, so
the statement is tagged where it passes, with the id of the run that sent
it, and each hook takes only its own. A statement sent inside a flow but
outside any task belongs to the flow run.

All three are on by default and each has its own switch:
`CONVALESCE_SEND_SOURCE`, `CONVALESCE_SEND_ARGUMENTS` and
`CONVALESCE_SQL_CAPTURE`. Nothing here may break the run it reads.

Import as:

import convalesce_emit_prefect._capture as cecap
"""

import itertools
import json
import sys
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit.source as cesource
import convalesce_emit.sqlcapture as cesqlcap
import convalesce_emit_prefect._env as ceprefenv
import convalesce_emit_prefect._mask as cemask

_LOG = cemask.logger(__name__)

# Where Prefect keeps the running task and flow. Read from the modules
# already loaded, never imported: where it is not loaded, nothing is running.
_CONTEXT = "prefect.context"
_SETTINGS = "prefect.settings"

_SEND_ARGUMENTS_ENV = "CONVALESCE_SEND_ARGUMENTS"
_FALSY = frozenset({"0", "false", "no", "off"})

# A task is handed whatever the task before it returned, which is as often
# a list of a million rows as it is a date. What it was called with is worth
# sending for the values a person typed or a schedule chose, so a value is
# kept whole only while it is small, and the cut is declared in `excluded`.
MAX_ITEMS = 50
MAX_CHARS = 2_000
MAX_DEPTH = 8
# One argument, as JSON, after the cuts above.
MAX_ARGUMENT_CHARS = 20_000
# Objects walked for one event's arguments: enough for any configuration
# object, far short of a client or a session and everything it holds.
_ARGUMENT_NODES = 2_000

ARGUMENTS = "arguments"
_CUT = "argument cut"
_TOO_LARGE = "argument too large"

Excluded = List[Dict[str, str]]


# #############################################################################
# Source
# #############################################################################


def source_of(runnable: Any) -> Optional[Dict[str, Any]]:
    """
    The text of the function a flow or task runs.

    :param runnable: Prefect's flow or task, which keeps the decorated
        function as `fn`
    :return: what `cesource.of` gives; None when sending is off, the object
        keeps no function, or its source cannot be read
    """
    try:
        return cesource.of(getattr(runnable, "fn", None))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the source: %s", exc)
        return None


def send_source() -> bool:
    """
    Whether source may be sent.

    :return: False only when `CONVALESCE_SEND_SOURCE` says so
    """
    return cesource.enabled()


# #############################################################################
# Arguments
# #############################################################################


def send_arguments() -> bool:
    """
    Whether what a flow or task was called with may be sent.

    :return: False only when `CONVALESCE_SEND_ARGUMENTS` says so
    """
    raw = ceprefenv.read(_SEND_ARGUMENTS_ENV) or ""
    return raw.strip().lower() not in _FALSY


def arguments_of(task_run: Any) -> Optional[Dict[str, Any]]:
    """
    What the running task was called with, by parameter name.

    A task run records which upstream runs fed it, never the values. Those
    are on the run context Prefect keeps while the task and its hooks run,
    already resolved: an argument that was another task's future is the
    value that task returned.

    :param task_run: the task run the hook was given
    :return: the arguments, not yet shaped; None when sending is off, or
        the context is not this task run's
    """
    if not send_arguments():
        return None
    try:
        context = _context("TaskRunContext")
        running = getattr(getattr(context, "task_run", None), "id", None)
        wanted = getattr(task_run, "id", None)
        if context is None or running is None or str(running) != str(wanted):
            return None
        parameters = getattr(context, "parameters", None)
        return dict(parameters) if isinstance(parameters, dict) else None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the arguments: %s", exc)
        return None


def shape(values: Any, path: str = ARGUMENTS) -> Tuple[Any, Excluded]:
    """
    Arguments as they are sent: JSON, and small.

    Cut first, so a long list is never walked; then dumped by the core,
    which names a frame or an array instead of reading it; then cut again,
    for what an object turned out to hold.

    :param values: the arguments, by name
    :param path: where they sit in the payload, for `excluded`
    :return: the arguments, and every cut made, by path and reason
    """
    excluded: Excluded = []
    budget = cemit.new_budget()
    budget.nodes = _ARGUMENT_NODES
    dumped = cemit.dump(bound(values, path, excluded), budget=budget, path=path)
    excluded.extend(budget.excluded)
    dumped = bound(dumped, path, excluded)
    if not isinstance(dumped, dict):
        return dumped, excluded
    for name, value in list(dumped.items()):
        try:
            size = len(json.dumps(value, default=str))
        except (TypeError, ValueError):
            size = MAX_ARGUMENT_CHARS + 1
        if size > MAX_ARGUMENT_CHARS:
            dumped[name] = f"<{type(value).__name__}>"
            excluded.append({"path": f"{path}.{name}", "reason": _TOO_LARGE})
    return dumped, excluded


def bound(value: Any, path: str, excluded: Excluded, depth: int = 0) -> Any:
    """
    Cut a plain value down: long text, long lists, deep nesting.

    Only plain containers are walked. Anything else is left as it is, for
    the core serialiser to dump or to name.

    :param value: the value
    :param path: where it sits in the payload
    :param excluded: every cut made is added here, by path and reason
    :param depth: how far below the arguments this is
    :return: the value, cut where it was large
    """
    if type(value) not in (dict, list, tuple, set, frozenset):
        return _bound_leaf(value, path, excluded)
    if depth >= MAX_DEPTH:
        excluded.append({"path": path, "reason": _CUT})
        return f"<{type(value).__name__}>"
    if len(value) > MAX_ITEMS:
        excluded.append(
            {
                "path": path,
                "reason": f"{_CUT}: first {MAX_ITEMS} of {len(value)}",
            }
        )
    if isinstance(value, dict):
        return {
            key: bound(item, f"{path}.{key}", excluded, depth + 1)
            for key, item in itertools.islice(value.items(), MAX_ITEMS)
        }
    return [
        bound(item, f"{path}[{index}]", excluded, depth + 1)
        for index, item in enumerate(itertools.islice(value, MAX_ITEMS))
    ]


def _bound_leaf(value: Any, path: str, excluded: Excluded) -> Any:
    """
    Cut one value that holds no others.

    :param value: the value
    :param path: where it sits in the payload
    :param excluded: a cut made is added here, by path and reason
    :return: long text cut to its head, bytes named by their length,
        anything else as it is
    """
    if isinstance(value, str) and len(value) > MAX_CHARS:
        excluded.append({"path": path, "reason": _CUT})
        return value[:MAX_CHARS]
    if isinstance(value, (bytes, bytearray)):
        excluded.append({"path": path, "reason": _CUT})
        return f"<{type(value).__name__}: {len(value)} bytes>"
    return value


# #############################################################################
# SQL
# #############################################################################


def watch() -> None:
    """
    Start noting the SQL flows and tasks in this process run.

    Called once, as the package is imported, so a flow's author does nothing
    new: by the time a task opens a connection the hooks are in. Statements
    are kept per run; Prefect's own are left out.

    :return: nothing
    """
    try:
        if not cesqlcap.enabled():
            return
        cesqlcap.ignore(own_statement)
        cesqlcap.scope_by(running_scope)
        cesqlcap.install()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not watch SQL: %s", exc)


def take_sql(run: Any, state: Any) -> Optional[Dict[str, Any]]:
    """
    Hand over, and forget, the statements a run sent.

    :param run: the task run or flow run the hook was given
    :param state: the state it moved to; a run still running keeps its
        statements for the hook that ends it
    :return: what `cesqlcap.drain` gives; None when the run is not over, or
        it sent nothing
    """
    try:
        for check in ("is_running", "is_pending", "is_scheduled"):
            method = getattr(state, check, None)
            if callable(method) and method():
                return None
        run_id = getattr(run, "id", None)
        if run_id is None:
            return None
        return cesqlcap.drain(scope=str(run_id))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the SQL: %s", exc)
        return None


def running_scope() -> Optional[str]:
    """
    The run the calling code belongs to: the task run, else the flow run.

    Called where a statement passes, on the thread and in the context that
    sent it, which is what makes the answer that statement's own.

    :return: the run's id as text; None outside any run
    """
    for name, attribute in (
        ("TaskRunContext", "task_run"),
        ("FlowRunContext", "flow_run"),
    ):
        run_id = getattr(getattr(_context(name), attribute, None), "id", None)
        if run_id is not None:
            return str(run_id)
    return None


def own_statement(row: Dict[str, Any]) -> bool:
    """
    Whether a statement is Prefect talking to its own database.

    A flow run with no API to talk to starts Prefect's server inside its
    own process, on Prefect 2 in the very context of the task whose state
    is being written. Those statements pass an engine on Prefect's own
    database URL, which is how they are told apart.

    :param row: what was noted about the statement
    :return: True for a statement on the engine of Prefect's database
    """
    url = row.get("url")
    if not url:
        return False
    try:
        own = _own_database()
        return own is not None and _coordinates(str(url)) == own
    except Exception:  # pylint: disable=broad-exception-caught
        return False


def _own_database() -> Optional[Tuple[Any, ...]]:
    """
    Where Prefect's own database is, as this process is configured.

    Read each time, not kept: a profile, or Prefect's test harness, moves
    it while the process runs.

    :return: its coordinates, or None when Prefect names none
    """
    settings = sys.modules.get(_SETTINGS)
    setting = getattr(settings, "PREFECT_API_DATABASE_CONNECTION_URL", None)
    value = getattr(setting, "value", None)
    url = value() if callable(value) else None
    # A secret on some versions, plain text on others.
    reveal = getattr(url, "get_secret_value", None)
    if callable(reveal):
        url = reveal()
    return _coordinates(str(url)) if url else None


def _coordinates(url: str) -> Tuple[Any, ...]:
    """
    What makes two database URLs the same database, whatever the password.

    :param url: a database URL
    :return: its driver, host, port and database
    """
    parts = urllib.parse.urlsplit(url)
    return (parts.scheme, parts.hostname, parts.port, parts.path)


def _context(name: str) -> Any:
    """
    One of Prefect's run contexts, as the calling code sees it.

    :param name: `TaskRunContext` or `FlowRunContext`
    :return: the context; None outside a run, or without Prefect
    """
    module = sys.modules.get(_CONTEXT)
    get = getattr(getattr(module, name, None), "get", None)
    try:
        return get() if callable(get) else None
    except Exception:  # pylint: disable=broad-exception-caught
        return None

"""
Lineage a task declares about itself.

Prefect knows which task fed which, never which tables a task read or wrote:
that is in the task's own code. A task says so with one call::

    from convalesce_emit_prefect import lineage

    @task(on_completion=[emit_task_run], on_failure=[emit_task_run])
    def load():
        lineage(
            inputs=[{"platform": "postgres", "name": "shop.public.orders"}],
            outputs=["urn:li:dataset:(urn:li:dataPlatform:snowflake,a.b.c,PROD)"],
        )

and the task-run hook sends it as the payload's `lineage`. Declarations are
kept by task run id, not on a context variable: Prefect runs a task's body
and its hooks in the same process, but not always in the same context, so a
context variable set in the body can be gone by the time the hook reads it.

Import as:

import convalesce_emit_prefect._lineage as celin
"""

import collections
import importlib
import logging
import threading
from typing import Any, Dict, List, Optional, Sequence, Union

_LOG = logging.getLogger(__name__)

DEFAULT_ENV = "PROD"

# Where Prefect keeps the running task while its body runs.
_TASK_CONTEXT = "prefect.context"

# Declarations whose hook never fired -- a task wired without the hook --
# would otherwise be held for the life of the process.
_MAX_PENDING = 1024

Dataset = Union[str, Dict[str, Any]]

_LOCK = threading.Lock()
_PENDING: "collections.OrderedDict[str, Dict[str, List[Dataset]]]" = (
    collections.OrderedDict()
)


def lineage(
    inputs: Optional[Sequence[Dataset]] = None,
    outputs: Optional[Sequence[Dataset]] = None,
) -> None:
    """
    Declare what the running task read and wrote.

    Called more than once in a task, or again on a retry, the declarations
    add up, each dataset once. Outside a task run it does nothing. An item
    that is neither a urn nor a `{"platform", "name"}` mapping is dropped
    with a warning rather than raised: a task must not fail over this.

    :param inputs: datasets read, each a urn or `{"platform": str, "name":
        str, "env": str}`, `env` defaulting to `PROD`
    :param outputs: datasets written, the same way
    :return: nothing
    """
    task_run_id = _running_task_run_id()
    if task_run_id is None:
        _LOG.debug("convalesce: lineage() called outside a task run")
        return
    with _LOCK:
        declared = _PENDING.pop(task_run_id, {"inputs": [], "outputs": []})
        for side, items in (("inputs", inputs), ("outputs", outputs)):
            for item in items or ():
                dataset = normalise(item)
                if dataset is not None and dataset not in declared[side]:
                    declared[side].append(dataset)
        _PENDING[task_run_id] = declared
        while len(_PENDING) > _MAX_PENDING:
            _PENDING.popitem(last=False)


def take(task_run: Any) -> Optional[Dict[str, List[Dataset]]]:
    """
    Hand over, and forget, what a task run declared.

    :param task_run: the task run the hook was given
    :return: `{"inputs": [...], "outputs": [...]}`, or None when it declared
        nothing
    """
    task_run_id = getattr(task_run, "id", None)
    if task_run_id is None:
        task_run_id = _running_task_run_id()
    if task_run_id is None:
        return None
    with _LOCK:
        return _PENDING.pop(str(task_run_id), None)


def normalise(item: Any) -> Optional[Dataset]:
    """
    One declared dataset, as it is sent.

    :param item: a urn, or `{"platform", "name", "env"}`
    :return: the urn as given, or the mapping with `env` filled in; None
        for anything else
    """
    if isinstance(item, str) and item:
        return item
    if isinstance(item, dict):
        platform = item.get("platform")
        name = item.get("name")
        env = item.get("env") or DEFAULT_ENV
        if isinstance(platform, str) and isinstance(name, str):
            if platform and name and isinstance(env, str):
                return {"platform": platform, "name": name, "env": env}
    _LOG.warning("convalesce: ignoring lineage dataset %r", item)
    return None


def _running_task_run_id() -> Optional[str]:
    """
    The id of the task run Prefect is running, if any.

    :return: the id as text, or None outside a task run or without Prefect
    """
    try:
        module = importlib.import_module(_TASK_CONTEXT)
        context = module.TaskRunContext.get()
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    task_run_id = getattr(getattr(context, "task_run", None), "id", None)
    return str(task_run_id) if task_run_id is not None else None

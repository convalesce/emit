"""
Lineage a task declares about itself.

Prefect knows which task fed which, never which tables a task read or wrote:
that is in the task's own code. A task says so with one call::

    from convalesce_emit_prefect import lineage

    @task(on_completion=[emit_task_run], on_failure=[emit_task_run])
    def load():
        lineage(
            inputs=[{"platform": "postgres", "name": "shop.public.orders"}],
            outputs=["urn:cvl:dataset:(urn:cvl:dataPlatform:snowflake,a.b.c,PROD)"],
        )

and the task-run hook sends it as the payload's `lineage`. A task that starts
a run in another tool -- a Glue job, a Databricks job -- names it the same
way, with `launched("glue", run_id, job="lake_daily_agg")`, and the hook
sends it as the payload's `launched`. Declarations are
kept by task run id, not on a context variable: Prefect runs a task's body
and its hooks in the same process, but not always in the same context, so a
context variable set in the body can be gone by the time the hook reads it.

Import as:

import convalesce_emit_prefect._lineage as celin
"""

import collections
import importlib
import threading
from typing import Any, Dict, List, Optional, Sequence, Union

import convalesce_emit_prefect._mask as cemask

_LOG = cemask.logger(__name__)

DEFAULT_ENV = "PROD"

# Where Prefect keeps the running task while its body runs.
_TASK_CONTEXT = "prefect.context"

# Declarations whose hook never fired -- a task wired without the hook --
# would otherwise be held for the life of the process.
_MAX_PENDING = 1024

# A task starting more runs than this is fanning out, and the first ones are
# enough to link it; the length limit keeps a stray blob out of an id.
_MAX_LAUNCHED = 20
_MAX_LAUNCHED_TEXT = 200

Dataset = Union[str, Dict[str, Any]]

_LOCK = threading.Lock()
_PENDING: "collections.OrderedDict[str, Dict[str, List[Dataset]]]" = (
    collections.OrderedDict()
)
_LAUNCHED: "collections.OrderedDict[str, List[Dict[str, str]]]" = (
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
        str, "env": str}`, `env` defaulting to `PROD`, with
        `"platform_instance": str` where the platform has more than one
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


def launched(platform: str, run_id: str, job: str = "") -> None:
    """
    Declare a run the running task started in another tool.

    Called more than once in a task, or again on a retry, the declarations
    add up, each run once and at most `_MAX_LAUNCHED` of them. Outside a task
    run it does nothing. A platform or run id that is not a non-empty string
    of at most `_MAX_LAUNCHED_TEXT` characters, or a job that is not such a
    string or empty, is ignored with a warning rather than raised.

    :param platform: the tool the run is in, e.g. `glue`
    :param run_id: that tool's own id for the run, e.g. `jr_...`
    :param job: the job the run belongs to, if the tool names one
    :return: nothing
    """
    entry = _launched_entry(platform, run_id, job)
    if entry is None:
        return
    task_run_id = _running_task_run_id()
    if task_run_id is None:
        _LOG.debug("convalesce: launched() called outside a task run")
        return
    with _LOCK:
        declared = _LAUNCHED.pop(task_run_id, [])
        if entry not in declared and len(declared) < _MAX_LAUNCHED:
            declared.append(entry)
        _LAUNCHED[task_run_id] = declared
        while len(_LAUNCHED) > _MAX_PENDING:
            _LAUNCHED.popitem(last=False)


def take(task_run: Any) -> Optional[Dict[str, List[Dataset]]]:
    """
    Hand over, and forget, what a task run declared.

    :param task_run: the task run the hook was given
    :return: `{"inputs": [...], "outputs": [...]}`, or None when it declared
        nothing
    """
    task_run_id = _task_run_id_of(task_run)
    if task_run_id is None:
        return None
    with _LOCK:
        return _PENDING.pop(task_run_id, None)


def take_launched(task_run: Any) -> Optional[List[Dict[str, str]]]:
    """
    Hand over, and forget, the runs a task run said it started.

    :param task_run: the task run the hook was given
    :return: `[{"platform", "run_id", "job"}, ...]`, or None when it
        declared none
    """
    task_run_id = _task_run_id_of(task_run)
    if task_run_id is None:
        return None
    with _LOCK:
        return _LAUNCHED.pop(task_run_id, None)


def normalise(item: Any) -> Optional[Dataset]:
    """
    One declared dataset, as it is sent.

    :param item: a urn, or `{"platform", "name", "env"}`, with a
        `platform_instance` where the platform has more than one
    :return: the urn as given, or the mapping with `env` filled in and its
        `platform_instance` kept; None for anything else
    """
    if isinstance(item, str) and item:
        return item
    if isinstance(item, dict):
        platform = item.get("platform")
        name = item.get("name")
        env = item.get("env") or DEFAULT_ENV
        if isinstance(platform, str) and isinstance(name, str):
            if platform and name and isinstance(env, str):
                dataset = {"platform": platform, "name": name, "env": env}
                instance = item.get("platform_instance")
                if isinstance(instance, str) and instance:
                    dataset["platform_instance"] = instance
                return dataset
    _LOG.warning("convalesce: ignoring lineage dataset %r", item)
    return None


def _launched_entry(
    platform: Any, run_id: Any, job: Any
) -> Optional[Dict[str, str]]:
    """
    One declared run, as it is sent.

    :param platform: the tool the run is in
    :param run_id: that tool's id for the run
    :param job: the job it belongs to, or empty
    :return: `{"platform", "run_id", "job"}`, or None when any is invalid
    """
    for value, required in ((platform, True), (run_id, True), (job, False)):
        if not isinstance(value, str) or len(value) > _MAX_LAUNCHED_TEXT:
            break
        if required and not value:
            break
    else:
        return {"platform": platform, "run_id": run_id, "job": job}
    _LOG.warning("convalesce: ignoring launched run %r", (platform, run_id, job))
    return None


def _task_run_id_of(task_run: Any) -> Optional[str]:
    """
    The id of the task run a hook was given, or else of the running one.

    :param task_run: the task run the hook was given
    :return: the id as text, or None when neither names one
    """
    task_run_id = getattr(task_run, "id", None)
    if task_run_id is None:
        return _running_task_run_id()
    return str(task_run_id)


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

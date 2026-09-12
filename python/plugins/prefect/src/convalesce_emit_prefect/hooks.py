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
from typing import Any, Dict, Optional

import convalesce_emit as cemit

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
    cemit.send_one(
        tool=TOOL,
        event=event,
        payload=cemit.dump(payload),
        emitter=emitter,
        tool_version=cemit.version_of("prefect"),
    )


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
    # The hook's own arguments win: what the context holds is a fallback for
    # what the task run does not name, never a replacement for it.
    for name, value in running_flow().items():
        payload.setdefault(name, None)
        if payload[name] is None:
            payload[name] = value
    _emit("task_run", payload, emitter)

"""
Prefect flow and task state hooks.

Exports plain functions rather than decorated hooks: Prefect's signatures
have moved between versions, and this package deliberately does not depend on
Prefect, so the customer wires them up themselves.

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
    _emit("task_run", payload, emitter)

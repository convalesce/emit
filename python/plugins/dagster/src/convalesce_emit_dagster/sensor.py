"""
Dagster run-status forwarding.

Exports plain functions rather than a pre-decorated sensor: Dagster's
decorator signatures have moved between versions, and this package
deliberately does not depend on Dagster, so the customer applies the
decorator in their own definitions.

Import as:

import convalesce_emit_dagster.sensor as cedsens
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


TOOL = "dagster"

# What a run-status context carries. The context is a wrapper: its state is
# private and its run and event are properties, so forwarded whole it arrives
# as its repr and nothing else, no job name, no run id, no failure. Found by
# running a real daemon; a DagsterRun passed by hand had hidden it.
_CONTEXT_PARTS = ("dagster_run", "dagster_event", "sensor_name")


def emit_dagster_event(
    event: str, payload: Any, emitter: Optional[cemit.EmitterLike] = None
) -> None:
    """
    Send one Dagster event exactly as Dagster produced it.

    :param event: which callback fired
    :param payload: Dagster's own object
    :param emitter: emitter to send through; built from the environment when
        not given
    :return: nothing
    """
    cemit.send_one(
        tool=TOOL,
        event=event,
        payload=cemit.dump(payload),
        emitter=emitter,
        tool_version=cemit.version_of("dagster"),
    )


def convalesce_sensor(
    context: Any = None,
    emitter: Optional[cemit.EmitterLike] = None,
    **kwargs: Any,
) -> None:
    """
    Run-status sensor body, wired up in the customer's own definitions::

        from dagster import run_status_sensor, DagsterRunStatus
        from convalesce_emit_dagster import convalesce_sensor

        @run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
        def convalesce_success(context):
            convalesce_sensor(context)

    :param context: Dagster's run-status context
    :param emitter: emitter to send through
    :param kwargs: whatever else Dagster passes; forwarded untouched
    :return: nothing
    """
    target = emitter or cemit.Emitter()
    parts = unwrap_context(context)
    payload = {**parts, **kwargs} if parts else {"context": context, **kwargs}
    emit_dagster_event("run_status", payload, emitter=target)
    target.flush()


def unwrap_context(context: Any) -> Dict[str, Any]:
    """
    Take the run, event and sensor name off a run-status context.

    :param context: Dagster's run-status context, or anything else
    :return: the parts it exposes; empty when it exposes none
    """
    parts: Dict[str, Any] = {}
    for name in _CONTEXT_PARTS:
        try:
            value = getattr(context, name, None)
        except Exception:  # pylint: disable=broad-exception-caught
            # A property that raises is Dagster's business, not a reason to
            # lose the rest.
            value = None
        if value is not None:
            parts[name] = value
    return parts

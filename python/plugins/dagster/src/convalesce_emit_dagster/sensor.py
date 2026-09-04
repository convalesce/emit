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
from typing import Any, Optional

import convalesce_emit as cemit

_LOG = logging.getLogger(__name__)

# Dagster and Prefect both forward a hook payload with the same five-line
# call to `cemit.send_one`, differing only in their constants. Pulling that
# into a shared wrapper would add an indirection carrying nothing, so the
# similarity stays and the check is turned off here rather than everywhere.
# pylint: disable=duplicate-code


TOOL = "dagster"


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
    emit_dagster_event(
        "run_status", {"context": context, **kwargs}, emitter=target
    )
    target.flush()

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

# What the context can reach but does not hold, as the name to send it
# under, the method to call on the instance, and the id on the run to call
# it with. The run and the event say that a job ran and how it ended; these
# say what the job is made of, when the run and each step began and ended,
# and which step feeds which, none of which is anywhere else.
#
# The names and the one positional argument are the same on Dagster 1.7 and
# 1.13, and each lookup is by name on the instance, so a release that drops
# one leaves the others.
_FROM_INSTANCE = (
    ("job_snapshot", "get_job_snapshot", "job_snapshot_id"),
    (
        "execution_plan_snapshot",
        "get_execution_plan_snapshot",
        "execution_plan_snapshot_id",
    ),
    ("run_stats", "get_run_stats", "run_id"),
    ("step_stats", "get_run_step_stats", "run_id"),
)

# Fields of a job snapshot that describe Dagster's own type system rather
# than the job: every config type and every dagster type in the process.
# They are by far the largest part of a snapshot, they say nothing about
# what ran, and left in they crowd the ops out of the payload's budget.
_SNAPSHOT_NOISE = ("config_schema_snapshot", "dagster_type_namespace_snapshot")


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
    run = parts.get("dagster_run")
    if run is not None:
        parts.update(reach_instance(context, run))
    payload = {**parts, **kwargs} if parts else {"context": context, **kwargs}
    body = cemit.dump(payload)
    if isinstance(body, dict) and "job_snapshot" in body:
        body["job_snapshot"] = prune_snapshot(body["job_snapshot"])
    cemit.send_one(
        tool=TOOL,
        event="run_status",
        payload=body,
        emitter=target,
        tool_version=cemit.version_of("dagster"),
    )
    target.flush()


def reach_instance(context: Any, run: Any) -> Dict[str, Any]:
    """
    Read what the run points at, from the instance the context carries.

    A run names its job snapshot and execution plan by id and its stats by
    run id; the instance is what turns those into objects. Each is one call,
    and each failure is logged and dropped, because a customer's job must
    not fail over what we could not read about it.

    :param context: Dagster's run-status context
    :param run: the run the context exposed
    :return: what could be read, by the name to send it under
    """
    instance = getattr(context, "instance", None)
    if instance is None:
        return {}
    out: Dict[str, Any] = {}
    for name, method_name, id_name in _FROM_INSTANCE:
        method = getattr(instance, method_name, None)
        identifier = getattr(run, id_name, None)
        if not callable(method) or not identifier:
            continue
        try:
            value = method(identifier)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # A storage that cannot answer is Dagster's business; the rest
            # of the observation is still worth sending.
            _LOG.debug("convalesce: could not read %s: %s", name, exc)
            continue
        if value is not None:
            out[name] = value
    return out


def prune_snapshot(snapshot: Any) -> Any:
    """
    Drop what a job snapshot says about Dagster rather than about the job.

    :param snapshot: the dumped job snapshot
    :return: the same snapshot, lighter
    """
    if not isinstance(snapshot, dict):
        return snapshot
    return {
        name: value
        for name, value in snapshot.items()
        if name not in _SNAPSHOT_NOISE
    }


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

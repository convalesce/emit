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
import os
from typing import Any, Dict, List, Optional

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

# The event types the event log is read for: what an asset materialised or
# observed, what an IO manager handled or loaded, and a resource coming up --
# the facts the run's stats and snapshots above say nothing about, because
# none of them is a step's own output.
_EVENT_LOG_TYPE_NAMES = (
    "ASSET_MATERIALIZATION",
    "ASSET_OBSERVATION",
    "HANDLED_OUTPUT",
    "LOADED_INPUT",
    "RESOURCE_INIT_SUCCESS",
)

# The metadata on a materialisation or observation event is free text an
# asset's own author attaches to describe what ran -- row counts, sample
# values, arbitrary JSON -- and is by far the widest sensitive-data surface
# of any of these five packages. Redacted the same way a Great Expectations
# sample is, scoped to this call so no other package's "metadata" field is
# touched.
_EVENT_LOG_SAMPLE_KEYS = frozenset({"metadata"})

# Dagster Cloud's own build of the deployment: which one, and from which
# commit. Name-prefixed rather than read off any tool object, the same as
# the Cloud UI itself documents these. `_CLOUD_ENV_SECRET_MARKERS` is a
# second guard beyond the documented list, in case a future variable in this
# namespace ever carries a credential.
_CLOUD_ENV_PREFIX = "DAGSTER_CLOUD_"
_CLOUD_ENV_SECRET_MARKERS = ("TOKEN", "KEY", "SECRET", "PASSWORD")


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
    budget = cemit.new_budget()
    dumped = cemit.dump(payload, budget=budget)
    cemit.send_one(
        tool=TOOL,
        event=event,
        payload=dumped,
        emitter=emitter,
        tool_version=cemit.version_of("dagster"),
        excluded=budget.excluded,
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
    had_parts = bool(parts)
    run = parts.get("dagster_run")
    if run is not None:
        parts.update(reach_instance(context, run))
    groups = asset_group_names(context)
    if groups:
        parts["asset_group_names"] = groups
    cloud_env = cloud_environment()
    if cloud_env:
        parts["cloud_environment"] = cloud_env
    payload = (
        {**parts, **kwargs} if had_parts else {"context": context, **kwargs}
    )
    budget = cemit.new_budget()
    body = cemit.dump(payload, budget=budget)
    excluded = budget.excluded
    if isinstance(body, dict) and "job_snapshot" in body:
        body["job_snapshot"] = prune_snapshot(body["job_snapshot"])
    if isinstance(body, dict) and "event_log" in body:
        body["event_log"], redacted = cemit.redact_samples(
            body["event_log"],
            path="event_log",
            extra_keys=_EVENT_LOG_SAMPLE_KEYS,
        )
        excluded = excluded + redacted
    cemit.send_one(
        tool=TOOL,
        event="run_status",
        payload=body,
        emitter=target,
        tool_version=cemit.version_of("dagster"),
        excluded=excluded,
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
    run_id = getattr(run, "run_id", None)
    if run_id:
        event_log = read_event_log(instance, run_id)
        if event_log:
            out["event_log"] = event_log
    return out


def read_event_log(instance: Any, run_id: str) -> Optional[List[Any]]:
    """
    The run's own event log, filtered to what is worth forwarding.

    `instance.all_logs`, the call this was written against, is gone on
    Dagster 1.13 -- removed in favour of `get_records_for_run`, which both
    1.7 and 1.13 carry, found by reading the two versions' own source rather
    than assumed from the older one alone. Filtered the same way either
    method would have been: to asset materialisations and observations, what
    an IO manager handled or loaded, and a resource coming up.

    :param instance: the run-status context's own `instance`
    :param run_id: the run this event log belongs to
    :return: the matching records, forwarded whole; empty when Dagster is
        absent, the types could not be resolved, or nothing matched
    """
    of_type = _event_log_types()
    if not of_type:
        return None
    method = getattr(instance, "get_records_for_run", None)
    if not callable(method):
        return None
    try:
        connection = method(run_id, of_type=of_type)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the event log: %s", exc)
        return None
    records = getattr(connection, "records", None)
    return list(records) if records else None


def _event_log_types() -> Optional[Any]:
    """
    The `DagsterEventType` members the event log is filtered to.

    Imported here, not at module level: this package depends on nothing but
    the client, and must still import where Dagster is absent.

    :return: the members that exist on this Dagster, or None when Dagster is
        absent or named none of them
    """
    try:
        # pylint: disable=import-outside-toplevel
        from dagster import DagsterEventType
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    members = {
        getattr(DagsterEventType, name)
        for name in _EVENT_LOG_TYPE_NAMES
        if hasattr(DagsterEventType, name)
    }
    return members or None


def asset_group_names(context: Any) -> Dict[str, str]:
    """
    Asset key to group name, from the sensor's own repository definition.

    A group name lives on the `AssetsDefinition` an asset was declared with,
    not on a materialisation event, so it is reachable only from the
    repository the sensor is defined in -- never from the run or its event
    log.

    :param context: Dagster's run-status context
    :return: dot-joined asset key to group name, for every asset a group
        could be read for; empty when the context carries no repository
    """
    repository_def = getattr(context, "repository_def", None)
    if repository_def is None:
        return {}
    try:
        assets_defs = repository_def.asset_graph.assets_defs_by_key.values()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the asset graph: %s", exc)
        return {}
    out: Dict[str, str] = {}
    seen: set = set()
    for assets_def in assets_defs:
        if id(assets_def) in seen:
            # One multi-asset `AssetsDefinition` is the value for every key
            # it defines; reading its groups once is enough.
            continue
        seen.add(id(assets_def))
        try:
            groups = assets_def.group_names_by_key
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        for key, group in groups.items():
            out[_asset_key_text(key)] = group
    return out


def _asset_key_text(key: Any) -> str:
    """
    An `AssetKey` as the dot-joined text a receiver can key lineage off.

    :param key: the asset key
    :return: its path, dot-joined; its `str()` if it has no path
    """
    path = getattr(key, "path", None)
    if isinstance(path, (list, tuple)):
        return ".".join(str(part) for part in path)
    return str(key)


def cloud_environment() -> Dict[str, str]:
    """
    Dagster Cloud's own deployment and commit info, from its environment.

    Not read off any tool object: Dagster Cloud's agent sets these in the
    process before user code ever runs, the same way the Cloud UI itself
    documents them. `_CLOUD_ENV_SECRET_MARKERS` is a second guard beyond the
    documented, credential-free list, in case a future variable in this
    namespace ever carries one.

    :return: every `DAGSTER_CLOUD_*` variable found, minus anything whose
        name suggests a credential
    """
    out: Dict[str, str] = {}
    for name, value in os.environ.items():
        if not name.startswith(_CLOUD_ENV_PREFIX):
            continue
        if any(marker in name.upper() for marker in _CLOUD_ENV_SECRET_MARKERS):
            continue
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

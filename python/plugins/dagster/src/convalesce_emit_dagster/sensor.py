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
from typing import Any, Dict, List, Optional, Tuple

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
# observed, what an IO manager handled or loaded, a resource coming up, and
# what an asset check found -- the facts the run's stats and snapshots above
# say nothing about, because none of them is a step's own output. A step
# failure is here for its error: the run's own failure event usually carries
# none, only "steps failed", and the exception each step raised is on its
# `STEP_FAILURE` alone. Every other type is accounted for, with its reason,
# in `test/backward_exclusions.json`.
EVENT_LOG_TYPE_NAMES = (
    "ASSET_CHECK_EVALUATION",
    "ASSET_MATERIALIZATION",
    "ASSET_OBSERVATION",
    "HANDLED_OUTPUT",
    "LOADED_INPUT",
    "RESOURCE_INIT_SUCCESS",
    "STEP_FAILURE",
)

# The metadata on a materialisation or observation event is free text an
# asset's own author attaches to describe what ran -- row counts, sample
# values, arbitrary JSON -- and is by far the widest sensitive-data surface
# of any of these five packages. Redacted the same way a Great Expectations
# sample is, scoped to this call so no other package's "metadata" field is
# touched.
_EVENT_LOG_SAMPLE_KEYS = frozenset({"metadata"})

# Where materialisation events cross: the event log, and each step's stats,
# which carry the same events again under `materialization_events`.
_EVENT_CARRIERS = ("event_log", "step_stats")

# The metadata entries that describe a table rather than hold its rows: the
# ones Dagster itself writes for a materialisation, and the urns an asset's
# author sets to say which dataset it is. These cross as they are, alongside
# the redaction marker for whatever else the entry carried.
METADATA_ALLOWED = frozenset(
    {
        "dagster/row_count",
        "row_count",
        "dagster/table_name",
        "table_name",
        "dagster/uri",
        "uri",
        "path",
        "dagster/relation_identifier",
        "dagster/storage_kind",
        "dagster/partition_row_count",
        # Which upstream column feeds which of this asset's: asset keys and
        # column names, the same kind of fact as the column schema.
        "dagster/column_lineage",
        # How many rows a dbt test found failing: a count, never the rows.
        "dagster_dbt/failed_row_count",
        "size_in_bytes",
        "dagster/code_version",
        "convalesce_urn",
        "datahub_urn",
        "dagster/column_schema",
        # The SQL an asset ran: its text names tables, not rows, and is what
        # collect parses into table and column lineage.
        "Query",
        "query",
        "sql",
        "dagster/query",
        "convalesce/query",
    }
)

# Of a column schema, what names a column; a column's tags are the author's
# free text, like the metadata around them.
_SCHEMA_COLUMN_FIELDS = ("name", "type", "description", "constraints")

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
    try:
        budget = cemit.new_budget()
        dumped = cemit.dump(payload, budget=budget)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("convalesce: could not shape %s: %s", event, exc)
        return
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
    try:
        _sensor(context, emitter, **kwargs)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A sensor that raises fails its tick in the customer's Dagster UI;
        # nothing about reporting on a run is worth that.
        _LOG.warning("convalesce: could not emit run_status: %s", exc)


def _sensor(
    context: Any, emitter: Optional[cemit.EmitterLike], **kwargs: Any
) -> None:
    target = emitter or cemit.Emitter()
    parts = unwrap_context(context)
    had_parts = bool(parts)
    run = parts.get("dagster_run")
    if run is not None:
        parts.update(reach_instance(context, run))
    groups = asset_group_names(context)
    if groups:
        parts["asset_group_names"] = groups
    definitions = asset_metadata(context)
    if definitions:
        parts["asset_metadata"] = definitions
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
    if isinstance(body, dict) and isinstance(body.get("asset_metadata"), dict):
        body["asset_metadata"] = {
            key: _allowed_metadata(entry) if isinstance(entry, dict) else entry
            for key, entry in body["asset_metadata"].items()
        }
    for name in _EVENT_CARRIERS:
        if isinstance(body, dict) and name in body:
            events, withheld = redact_metadata(body[name], name)
            body[name], redacted = cemit.redact_samples(events, path=name)
            excluded = excluded + withheld + redacted
    # A run's config and tags are whatever launched it typed, and a step's
    # error or an asset's `Query` can quote a connection string: any of them
    # can hold a literal credential.
    body, secrets = cemit.redact_secrets(body)
    cemit.send_one(
        tool=TOOL,
        event="run_status",
        payload=body,
        emitter=target,
        tool_version=cemit.version_of("dagster"),
        excluded=excluded + secrets,
    )
    target.flush()


def redact_metadata(value: Any, path: str) -> Tuple[Any, List[Dict[str, str]]]:
    """
    Redact every `metadata` entry but the ones that describe a table.

    A `metadata` mapping keeps its allowed entries as they are; the rest are
    counted into the same marker a fully redacted one carries, so a receiver
    reads `redacted` and `count` wherever it did before. Anything else under
    that key is redacted whole, the way `cemit.redact_samples` does it.

    :param value: the dumped event log, or part of one
    :param path: dotted path of `value` from the envelope root
    :return: the same shape, redacted, and what was redacted, by path and
        reason
    """
    excluded: List[Dict[str, str]] = []
    if isinstance(value, list):
        items = []
        for i, item in enumerate(value):
            kept, withheld = redact_metadata(item, f"{path}[{i}]")
            items.append(kept)
            excluded.extend(withheld)
        return items, excluded
    if not isinstance(value, dict):
        return value, excluded
    out: Dict[str, Any] = {}
    for key, item in value.items():
        child = f"{path}.{key}" if path else str(key)
        if key in _EVENT_LOG_SAMPLE_KEYS and isinstance(item, dict):
            out[key] = _allowed_metadata(item)
            if "redacted" in out[key]:
                excluded.append({"path": child, "reason": "sample redacted"})
        elif key in _EVENT_LOG_SAMPLE_KEYS:
            summary, withheld = cemit.redact_samples(
                {key: item}, path=path, extra_keys=_EVENT_LOG_SAMPLE_KEYS
            )
            out[key] = summary[key]
            excluded.extend(withheld)
        else:
            out[key], withheld = redact_metadata(item, child)
            excluded.extend(withheld)
    return out, excluded


def _allowed_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    One `metadata` mapping, down to the entries that describe a table.

    :param metadata: the dumped mapping, entry name to value
    :return: the allowed entries, plus `redacted` and `count` for the rest
        when there were any
    """
    out: Dict[str, Any] = {}
    for name, entry in metadata.items():
        if name == "dagster/column_schema":
            out[name] = _schema_only(entry)
        elif name in METADATA_ALLOWED:
            out[name] = entry
    withheld = len(metadata) - len(out)
    if withheld:
        out.update({"redacted": True, "count": withheld})
    return out


def _schema_only(entry: Any) -> Any:
    """
    A column schema's columns, each down to what names it.

    Dagster's `TableSchemaMetadataValue` dumps as `{"schema": {"columns":
    [...]}}`, or `{"columns": [...]}` as a bare `TableSchema`; both are
    walked for `columns` and anything else in them is left behind.

    :param entry: the dumped metadata value
    :return: `{"schema": {"columns": [...]}}`, or `{"redacted": True}` when
        no columns could be found
    """
    schema = entry.get("schema", entry) if isinstance(entry, dict) else None
    columns = schema.get("columns") if isinstance(schema, dict) else None
    if not isinstance(columns, list):
        return {"redacted": True}
    kept = [
        {
            field: column[field]
            for field in _SCHEMA_COLUMN_FIELDS
            if field in column
        }
        for column in columns
        if isinstance(column, dict)
    ]
    return {"schema": {"columns": kept}}


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
        for name in EVENT_LOG_TYPE_NAMES
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
    return _by_asset_key(context, "group_names_by_key")


def asset_metadata(context: Any) -> Dict[str, Dict[str, Any]]:
    """
    Asset key to the definition metadata that names its table.

    What an asset was declared with -- `dagster/table_name`, a dbt model's
    `dagster/storage_kind` and column schema -- is on its definition, not on
    its materialisation. Dagster 1.7 and 1.9 copied it onto the job
    snapshot's outputs as well; 1.13 does not, so without this a dbt model
    arrives as an asset with no table. Only the allowed entries are taken:
    a dbt definition also holds the whole manifest and its translator.

    :param context: Dagster's run-status context
    :return: dot-joined asset key to its allowed definition metadata, for
        every asset that has any
    """
    out: Dict[str, Dict[str, Any]] = {}
    for key, metadata in _by_asset_key(context, "metadata_by_key").items():
        items = metadata.items() if hasattr(metadata, "items") else ()
        kept = {name: value for name, value in items if name in METADATA_ALLOWED}
        if kept:
            out[key] = kept
    return out


def _by_asset_key(context: Any, attribute: str) -> Dict[str, Any]:
    """
    One per-key mapping of every asset definition in the sensor's repository.

    :param context: Dagster's run-status context
    :param attribute: the `AssetsDefinition` mapping to read, keyed by
        `AssetKey`
    :return: dot-joined asset key to its value; empty when the context
        carries no repository or its definitions cannot be read
    """
    repository_def = getattr(context, "repository_def", None)
    if repository_def is None:
        return {}
    try:
        # On the repository, not its `asset_graph`: the graph dropped
        # `assets_defs_by_key` by 1.9, which the repository still carries
        # from 1.7 to 1.13. Found on a live 1.9 daemon, where every run had
        # crossed without a group.
        assets_defs = repository_def.assets_defs_by_key.values()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the asset definitions: %s", exc)
        return {}
    out: Dict[str, Any] = {}
    seen: set = set()
    for assets_def in assets_defs:
        if id(assets_def) in seen:
            # One multi-asset `AssetsDefinition` is the value for every key
            # it defines; reading it once is enough.
            continue
        seen.add(id(assets_def))
        try:
            values = dict(getattr(assets_def, attribute))
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        for key, value in values.items():
            out[_asset_key_text(key)] = value
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

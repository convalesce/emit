"""
What both Great Expectations actions do once they have a payload.

Import as:

import convalesce_emit_gx._common as cegxcom
"""

import logging
import os
from typing import Any, Dict, List, Optional

import convalesce_emit as cemit

_LOG = logging.getLogger(__name__)

TOOL = "great_expectations"

_TRUTHY = frozenset({"1", "true", "yes", "on"})

# Arguments that are the Great Expectations runtime rather than the result of
# a validation. `data_asset` on 0.x is a live Validator, and through its data
# context it owns every store's configuration, the execution engine, the
# batch cache and the frame that was validated: about 10 KB of an 11 KB
# observation, none of it read by a receiver, and the shortest path to the
# customer's rows. What is worth having from the batch is already on
# `validation_result_suite.meta` as `batch_spec`, `batch_markers` and
# `active_batch_definition`, which is where the 1.x path reads it from too.
_SKIP_ARGS = frozenset({"data_asset"})


def send_samples() -> bool:
    """
    Whether the operator has opted into sending row values.

    :return: True when `CONVALESCE_GX_SEND_SAMPLES` is set truthy
    """
    raw = os.environ.get("CONVALESCE_GX_SEND_SAMPLES", "")
    return raw.strip().lower() in _TRUTHY


def is_runtime(value: Any) -> bool:
    """
    Tell the Great Expectations runtime from a validation result.

    Named for what it owns rather than by class, so a validator, a data
    context or a checkpoint arriving positionally is recognised on both
    majors without importing any of them.

    :param value: one argument the action was handed
    :return: whether it is the runtime rather than a result
    """
    return any(
        hasattr(value, name)
        for name in ("execution_engine", "data_context", "_data_context")
    )


def shape(payload: Any) -> Any:
    """
    Drop the runtime from what the action was handed.

    Only the shape both actions build, `args` and `kwargs`, is reshaped;
    anything else is a payload this does not recognise and crosses as it is.

    :param payload: the action's own arguments
    :return: the same, minus the runtime
    """
    if not isinstance(payload, dict):
        return payload
    if "args" not in payload and "kwargs" not in payload:
        return payload
    args: List[Any] = [
        value for value in payload.get("args") or [] if not is_runtime(value)
    ]
    kwargs = {
        name: value
        for name, value in (payload.get("kwargs") or {}).items()
        if name not in _SKIP_ARGS and not is_runtime(value)
    }
    return {"args": args, "kwargs": kwargs}


def _v0_datasource_name(payload: Dict[str, Any]) -> Optional[str]:
    """
    The datasource a 0.x validation ran against, from its result's `meta`.

    :param payload: the action's own arguments
    :return: the name, or None
    """
    candidates: List[Any] = [
        (payload.get("kwargs") or {}).get("validation_result_suite")
    ]
    candidates.extend(payload.get("args") or [])
    for value in candidates:
        meta = _field(value, "meta")
        if not isinstance(meta, dict):
            continue
        name = _field(meta.get("active_batch_definition"), "datasource_name")
        if name:
            return str(name)
    return None


def runtime_platform(payload: Any) -> Dict[str, Any]:
    """
    What the execution engine says about the platform, read before the
    runtime that carries it is left behind.

    A receiver has only the datasource's *name* to go on otherwise
    (`meta.active_batch_definition.datasource_name`), and falls back to
    using that as the platform, which produces a urn like
    `urn:li:dataset:(urn:li:dataPlatform:orders,orders,PROD)` for a
    datasource named `orders`. On 0.x the engine's own dialect is the real
    platform -- `engine.dialect.name`, and of the engine's URL only the
    database, so no credential travels.

    :param payload: the action's own arguments, before `shape()` reshapes
        them
    :return: `execution_engine_class` and, where the engine is backed by a
        live SQLAlchemy engine, `dialect_name`, `database` and
        `datasources`; empty when nothing runtime was handed to this
        action, or it named no engine
    """
    if not isinstance(payload, dict):
        return {}
    candidates: List[Any] = list(payload.get("args") or [])
    candidates.extend((payload.get("kwargs") or {}).values())
    for value in candidates:
        if not is_runtime(value):
            continue
        engine = getattr(value, "execution_engine", None)
        if engine is None:
            continue
        out: Dict[str, Any] = {"execution_engine_class": type(engine).__name__}
        sa_engine = getattr(engine, "engine", None)
        dialect = getattr(sa_engine, "dialect", None)
        name = getattr(dialect, "name", None)
        if name:
            out["dialect_name"] = str(name)
            out.update(_v0_database(payload, sa_engine, str(name)))
        return out
    return {}


def _v0_database(
    payload: Dict[str, Any], sa_engine: Any, dialect: str
) -> Dict[str, Any]:
    """
    The database a 0.x engine is connected to, and the datasource it backs.

    Snowflake's engine is a `Connection` whose URL is on its own `engine`,
    as DataHub's own GX action reads it.

    :param payload: the action's own arguments
    :param sa_engine: the SQLAlchemy engine or connection
    :param dialect: its dialect's name
    :return: `database` and `datasources`, or empty
    """
    try:
        url = getattr(sa_engine, "url", None)
        if url is None:
            url = getattr(getattr(sa_engine, "engine", None), "url", None)
        platform = _platform(dialect)
        database = url_database(url, platform) if url is not None else None
        if not database:
            return {}
        out: Dict[str, Any] = {"database": database}
        source = _v0_datasource_name(payload)
        if source:
            out["datasources"] = {
                source: {"type": platform, "database": database}
            }
        return out
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("convalesce: could not name the database: %s", exc)
        return {}


# A fluent datasource's `type`, or a SQLAlchemy dialect, to the platform a
# receiver names datasets on. Absent means the name is already the platform;
# `sql` is generic and resolved from its engine's dialect instead.
_PLATFORMS = {
    "databricks_sql": "databricks",
    "postgresql": "postgres",
    "redshift_connector": "redshift",
}

# Datasource types with no SQL engine behind them: no database to name, and
# the type is a dataframe library rather than a platform.
_NOT_SQL = frozenset({"pandas", "spark"})


def _field(value: Any, name: str) -> Any:
    """
    Read one field of a GX object or of its dict form.

    :param value: an object or a dict
    :param name: the field
    :return: the field, or None
    """
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _platform(name: Optional[str]) -> Optional[str]:
    """
    Name a platform from a datasource type or a dialect.

    :param name: `postgres`, `postgresql+psycopg2`, `databricks_sql`, ...
    :return: the platform, or None
    """
    if not name:
        return None
    base = str(name).split("+", 1)[0].lower()
    return _PLATFORMS.get(base, base)


def url_database(url: Any, platform: Optional[str]) -> Optional[str]:
    """
    The database a SQLAlchemy URL names, and nothing else from it.

    BigQuery's URL is `bigquery://<project>/<dataset>`: the project is the
    database, the dataset the schema, as DataHub's own GX action reads it.
    Snowflake's may carry `<database>/<schema>`. SQLite's is a file path,
    which names no database and says more about the host than it should.

    :param url: a SQLAlchemy `URL`
    :param platform: the platform it belongs to
    :return: the database, or None
    """
    if platform == "bigquery":
        host = getattr(url, "host", None)
        return str(host) if host else None
    database = getattr(url, "database", None)
    if not database or platform == "sqlite":
        return None
    if platform == "snowflake":
        return str(database).split("/", 1)[0]
    return str(database)


def datasource_facts(datasource: Any) -> Optional[Dict[str, Optional[str]]]:
    """
    A 1.x fluent datasource's platform and database; never its connection.

    The engine is the datasource's own, cached from the validation that just
    ran, so reading its URL opens nothing new.

    :param datasource: a GX 1.x fluent datasource
    :return: `{"type", "database"}`, or None for a datasource with no SQL
        engine behind it
    """
    kind = _field(datasource, "type")
    if not kind or str(kind).lower() in _NOT_SQL:
        return None
    url = None
    get_engine = getattr(datasource, "get_engine", None)
    if callable(get_engine):
        try:
            url = get_engine().url
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: no engine for datasource: %s", exc)
    platform = _platform(str(kind))
    if platform == "sql":
        platform = _platform(getattr(url, "drivername", None))
    if not platform or platform == "sql":
        return None
    database = url_database(url, platform) if url is not None else None
    return {"type": platform, "database": database}


def _datasource_names(checkpoint_result: Any) -> List[str]:
    """
    Every datasource a 1.x checkpoint result's validations ran against.

    :param checkpoint_result: GX 1.x `CheckpointResult`
    :return: the names, in the order first seen
    """
    names: List[str] = []
    run_results = _field(checkpoint_result, "run_results") or {}
    for result in run_results.values():
        meta = _field(result, "meta") or {}
        batch = _field(meta, "active_batch_definition")
        name = _field(batch, "datasource_name")
        if name and str(name) not in names:
            names.append(str(name))
    return names


def _context_datasources(checkpoint_result: Any) -> Dict[str, Any]:
    """
    The datasources a checkpoint knows, by name.

    The checkpoint's own validation definitions hold their datasource, so
    the data context is only asked for one they do not account for.

    :param checkpoint_result: GX 1.x `CheckpointResult`
    :return: datasource name to datasource
    """
    out: Dict[str, Any] = {}
    checkpoint = _field(checkpoint_result, "checkpoint_config")
    for definition in _field(checkpoint, "validation_definitions") or []:
        try:
            source = definition.data_source
        except Exception:  # pylint: disable=broad-exception-caught
            # An unsaved or detached definition cannot reach its datasource.
            continue
        name = _field(source, "name")
        if name:
            out.setdefault(str(name), source)
    return out


def project_datasource(name: str) -> Any:
    """
    Look a datasource up on the data context GX is running under.

    :param name: the datasource's name
    :return: the datasource, or None
    """
    try:
        # pylint: disable=import-outside-toplevel
        from great_expectations.data_context.data_context.context_factory import (
            project_manager,
        )

        # Not `get_project()`: that builds a fresh context and replaces the
        # running one with it.
        return project_manager.get_datasources()[name]
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: no datasource %s in context: %s", name, exc)
        return None


def datasources_v1(checkpoint_result: Any) -> Dict[str, Dict[str, Any]]:
    """
    Name each datasource a 1.x checkpoint validated by platform and database.

    A 1.x action is handed a checkpoint result rather than an engine, so
    without this a receiver has only the datasource's *name* to go on. Only
    the type and the database name cross: the connection string, host and
    credentials stay here. Never raises; a checkpoint must not fail over it.

    :param checkpoint_result: GX 1.x `CheckpointResult`
    :return: `{<datasource name>: {"type", "database"}}`
    """
    out: Dict[str, Dict[str, Any]] = {}
    try:
        known = _context_datasources(checkpoint_result)
        for name in _datasource_names(checkpoint_result):
            source = known.get(name) or project_datasource(name)
            if source is None:
                continue
            facts = datasource_facts(source)
            if facts is not None:
                out[name] = facts
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("convalesce: could not name datasources: %s", exc)
    return out


def forward(
    payload: Any,
    emitter: Optional[cemit.EmitterLike] = None,
    datasources: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Redact and send one validation result.

    Redaction is on unless deliberately turned off: a GX result carries
    sample failing values, which are real rows from the customer's table,
    and the handbook promises we read "table shapes, run outcomes, row
    counts, lineage. Not the rows themselves."

    The Great Expectations runtime is left behind for the same reason and
    one more: a validator owns the frame it validated, and no receiver reads
    a word of the data context hanging off it.

    :param payload: whatever GX handed the action
    :param emitter: emitter to send through; built from the environment when
        not given
    :param datasources: each datasource's platform and database, where the
        action could name them
    :return: whether the observation was emitted, and whether it was redacted
    """
    redact = not send_samples()
    platform = runtime_platform(payload)
    if datasources:
        platform["datasources"] = datasources
    budget = cemit.new_budget()
    body = cemit.dump(shape(payload), budget=budget)
    if platform and isinstance(body, dict):
        body.update(platform)
    excluded = budget.excluded
    if redact:
        body, redacted = cemit.redact_samples(body)
        excluded = excluded + redacted
    try:
        target = emitter or cemit.Emitter()
        target.emit(
            tool=TOOL,
            event="validation_result",
            payload=body,
            tool_version=cemit.version_of("great_expectations"),
            excluded=excluded,
        )
        target.flush()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A checkpoint must not fail because we could not report on it.
        _LOG.warning("convalesce: could not emit validation result: %s", exc)
        return {"convalesce_emitted": False, "error": str(exc)}
    return {"convalesce_emitted": True, "redacted": redact}

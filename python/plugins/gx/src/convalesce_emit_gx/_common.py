"""
What both Great Expectations actions do once they have a payload.

Import as:

import convalesce_emit_gx._common as cegxcom
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit.settings as cesettin

_LOG = logging.getLogger(__name__)

TOOL = "great_expectations"

_FALSY = frozenset({"0", "false", "no", "off"})

# A query asset's query is code, so it follows the setting every plugin
# sends code under. Read here rather than through `convalesce_emit.source`,
# which a `convalesce-emit` older than that module does not have.
_SEND_SOURCE_ENV = "CONVALESCE_SEND_SOURCE"

# A query longer than this is cut, keeping its head, as the SQL the other
# plugins see a step run is.
MAX_QUERY_CHARS = 20_000

_QUERY_NOT_SENT = "query not sent"
_QUERY_CUT = "query cut"

# Arguments that are the Great Expectations runtime rather than the result of
# a validation. `data_asset` on 0.x is a live Validator, and through its data
# context it owns every store's configuration, the execution engine, the
# batch cache and the frame that was validated: about 10 KB of an 11 KB
# observation, none of it read by a receiver, and the shortest path to the
# customer's rows. What is worth having from the batch is already on
# `validation_result_suite.meta` as `batch_spec`, `batch_markers` and
# `active_batch_definition`, which is where the 1.x path reads it from too.
_SKIP_ARGS = frozenset({"data_asset"})


# What a platform may put in front of a setting's name, as AWS Glue does
# with the variables it hands a job. Read here for the same reason.
_PLATFORM_PREFIX = "CUSTOMER_"


def read_setting(name: str) -> Optional[str]:
    """
    Read one of this plugin's settings, under either of its names.

    :param name: the setting, such as `CONVALESCE_SEND_SOURCE`
    :return: its value as set, or None when it is set under neither name
    """
    return os.environ.get(name, os.environ.get(_PLATFORM_PREFIX + name))


def send_samples() -> bool:
    """
    Whether failing sample values and observed column values may be sent.

    They are what says *which* rows broke a check, so they cross unless the
    operator switches them off.

    :return: False only when `CONVALESCE_GX_SEND_SAMPLES` says so
    """
    raw = read_setting("CONVALESCE_GX_SEND_SAMPLES") or ""
    return raw.strip().lower() not in _FALSY


def named_dataset(
    platform: Optional[str] = None,
    dataset_name: Optional[str] = None,
    platform_instance: Optional[str] = None,
) -> Dict[str, str]:
    """
    What an action was told about where its checkpoint's batch lives.

    A dataframe has no table behind it that GX knows of, and a datasource
    may be named for anything, so the action can be told the platform, the
    table and the platform instance outright. They cross as written, and a
    receiver files this validation's results under them before anything it
    worked out itself.

    :param platform: the platform, as the catalogue names it (`postgres`)
    :param dataset_name: the table, as the catalogue names it
        (`my_db.my_schema.orders`)
    :param platform_instance: the instance of the platform, where the
        catalogue tells several apart
    :return: those that were given, by name
    """
    named = {
        "platform": platform,
        "dataset_name": dataset_name,
        "platform_instance": platform_instance,
    }
    return {name: str(value) for name, value in named.items() if value}


def send_query() -> bool:
    """
    Whether a query asset's query may be sent.

    :return: False only when `CONVALESCE_SEND_SOURCE` says so
    """
    raw = read_setting(_SEND_SOURCE_ENV) or ""
    return raw.strip().lower() not in _FALSY


def bound_queries(payload: Any) -> Tuple[Any, List[Dict[str, str]]]:
    """
    Cut, or leave out, the query each query asset's batch was read with.

    GX keeps it on the batch spec of every result validated over a query
    asset or a runtime query, on both majors. It is what a receiver reads
    the tables and columns behind the batch from, so it crosses whole up to
    `MAX_QUERY_CHARS`; one cut short is marked `query_truncated`, because
    the head of a query can read as a different, complete one.

    :param payload: the dumped payload
    :return: the same shape, and what was left out or cut, by path and
        reason
    """
    excluded: List[Dict[str, str]] = []
    return _walk_queries(payload, "", send_query(), excluded), excluded


def _walk_queries(
    value: Any, path: str, send: bool, excluded: List[Dict[str, str]]
) -> Any:
    """
    Recurse through one value, bounding the query of each batch spec in it.

    :param value: the value being walked
    :param path: dotted path of `value` from the payload root
    :param send: whether a query may cross at all
    :param excluded: accumulator every change is appended to
    :return: the same shape, queries bounded
    """
    if isinstance(value, list):
        return [
            _walk_queries(item, f"{path}[{i}]", send, excluded)
            for i, item in enumerate(value)
        ]
    if not isinstance(value, dict):
        return value
    out = {
        key: _walk_queries(
            item, f"{path}.{key}" if path else str(key), send, excluded
        )
        for key, item in value.items()
    }
    spec = out.get("batch_spec")
    if isinstance(spec, dict) and isinstance(spec.get("query"), str):
        where = f"{path}.batch_spec.query" if path else "batch_spec.query"
        spec = dict(spec)
        if not send:
            del spec["query"]
            excluded.append({"path": where, "reason": _QUERY_NOT_SENT})
        elif len(spec["query"]) > MAX_QUERY_CHARS:
            spec["query"] = spec["query"][:MAX_QUERY_CHARS]
            spec["query_truncated"] = True
            excluded.append({"path": where, "reason": _QUERY_CUT})
        out["batch_spec"] = spec
    return out


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
    `urn:cvl:dataset:(urn:cvl:dataPlatform:orders,orders,PROD)` for a
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
            facts: Dict[str, Any] = {"type": platform, "database": database}
            schema = url_schema(url, platform)
            if schema:
                facts["schema"] = schema
            out["datasources"] = {source: facts}
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

# Datasource types that read files. Their batch's own path names the dataset,
# so the type crosses as GX named it and nothing else is looked up.
_FILE_TYPE_PREFIXES = ("pandas_", "spark_")


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
    if platform == "databricks":
        # Unity Catalog's three levels: the catalog is on the query string.
        catalog = _url_query(url, "catalog")
        return catalog or (str(database) if database else None)
    if not database or platform == "sqlite":
        return None
    if platform == "snowflake":
        return str(database).split("/", 1)[0]
    return str(database)


def url_schema(url: Any, platform: Optional[str]) -> Optional[str]:
    """
    The schema a SQLAlchemy URL makes the default, where it names one.

    A table asset with no `schema_name` lives in it, and a receiver cannot
    otherwise tell `analytics.orders` from `public.orders`.

    :param url: a SQLAlchemy `URL`
    :param platform: the platform it belongs to
    :return: the schema, or None
    """
    if platform == "bigquery":
        dataset = getattr(url, "database", None)
        return str(dataset) if dataset else None
    if platform == "snowflake":
        database = str(getattr(url, "database", None) or "")
        if "/" in database:
            return database.split("/", 1)[1] or None
        return _url_query(url, "schema")
    if platform == "databricks":
        return _url_query(url, "schema")
    return None


def _url_query(url: Any, name: str) -> Optional[str]:
    """
    One query-string parameter of a SQLAlchemy URL.

    :param url: a SQLAlchemy `URL`
    :param name: the parameter
    :return: its value, or None
    """
    query = getattr(url, "query", None) or {}
    try:
        value = query.get(name)
    except AttributeError:
        return None
    if isinstance(value, (tuple, list)):
        value = value[0] if value else None
    return str(value) if value else None


def _datasource_url(datasource: Any) -> Any:
    """
    The SQLAlchemy URL behind a fluent datasource, without connecting.

    The engine is preferred, being the datasource's own and already cached
    by the validation that just ran. Where it cannot be built here -- a
    BigQuery engine wants credentials, a `sql` datasource a driver this
    process may not import -- the connection string is parsed instead, which
    needs neither.

    :param datasource: a GX 1.x fluent datasource
    :return: the URL, or None
    """
    get_engine = getattr(datasource, "get_engine", None)
    if callable(get_engine):
        try:
            return get_engine().url
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: no engine for datasource: %s", exc)
    raw = _field(datasource, "connection_string")
    resolve = getattr(raw, "get_config_value", None)
    if callable(resolve):
        try:
            # pylint: disable-next=protected-access
            raw = resolve(datasource._data_context.config_provider)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: unresolved connection string: %s", exc)
            return None
    if not isinstance(raw, str):
        return None
    try:
        # pylint: disable-next=import-outside-toplevel
        from sqlalchemy.engine import make_url

        return make_url(raw)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: unreadable connection string: %s", exc)
        return None


def datasource_facts(datasource: Any) -> Optional[Dict[str, Optional[str]]]:
    """
    A 1.x fluent datasource's platform, database and schema; never its
    connection.

    :param datasource: a GX 1.x fluent datasource
    :return: `{"type", "database"}` and, where the connection names a
        default schema, `"schema"`; the type alone for a datasource that
        reads files; None for one with nothing behind it but a frame
    """
    kind = _field(datasource, "type")
    if not kind or str(kind).lower() in _NOT_SQL:
        return None
    if str(kind).lower().startswith(_FILE_TYPE_PREFIXES):
        return {"type": str(kind).lower(), "database": None}
    url = _datasource_url(datasource)
    platform = _platform(str(kind))
    if platform == "sql":
        platform = _platform(getattr(url, "drivername", None))
    if not platform or platform == "sql":
        return None
    out: Dict[str, Optional[str]] = {"type": platform, "database": None}
    if url is not None:
        out["database"] = url_database(url, platform)
        schema = url_schema(url, platform)
        if schema:
            out["schema"] = schema
    return out


def _run_results(checkpoint_result: Any) -> Dict[Any, Any]:
    """
    A 1.x checkpoint result's validation results, by identifier.

    :param checkpoint_result: GX 1.x `CheckpointResult`
    :return: the mapping, empty when it has none
    """
    run_results = _field(checkpoint_result, "run_results")
    return run_results if isinstance(run_results, dict) else {}


def _datasource_names(results: List[Any]) -> List[str]:
    """
    Every datasource some validation results ran against.

    :param results: GX validation results
    :return: the names, in the order first seen
    """
    names: List[str] = []
    for result in results:
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


def datasources_v1(
    checkpoint_result: Any, results: Optional[List[Any]] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Name each datasource a 1.x validation ran against by platform, database
    and schema.

    A 1.x action is handed a checkpoint result rather than an engine, so
    without this a receiver has only the datasource's *name* to go on. Only
    the type and the database and schema names cross: the connection string,
    host and credentials stay here. Never raises; a checkpoint must not fail
    over it.

    :param checkpoint_result: GX 1.x `CheckpointResult`, or None
    :param results: validation results to name instead, for one run outside
        a checkpoint
    :return: `{<datasource name>: {"type", "database", "schema"?}}`
    """
    out: Dict[str, Dict[str, Any]] = {}
    try:
        if results is None:
            results = list(_run_results(checkpoint_result).values())
        known = _context_datasources(checkpoint_result)
        for name in _datasource_names(results):
            source = known.get(name) or project_datasource(name)
            if source is None:
                continue
            facts = datasource_facts(source)
            if facts is not None:
                out[name] = facts
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("convalesce: could not name datasources: %s", exc)
    return out


def result_urls_v1(checkpoint_result: Any) -> Dict[str, str]:
    """
    Each validation result's GX Cloud page, by the identifier it is keyed by.

    GX Cloud sets `result_url` on a validation result it stored, but the
    result's own serialiser leaves the field out, so it would never cross
    with the rest of the result. Keyed by the identifier as the dump renders
    it, the same key the result itself crosses under.

    :param checkpoint_result: GX 1.x `CheckpointResult`
    :return: identifier to page; empty outside GX Cloud
    """
    out: Dict[str, str] = {}
    try:
        for key, result in _run_results(checkpoint_result).items():
            url = _field(result, "result_url")
            if isinstance(url, str) and url:
                out[str(key)] = url
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: no result urls: %s", exc)
    return out


# Expectations whose observed value is a set of the column's own values:
# the distinct values, or the most common ones. Redacted whatever those
# values are, numbers included, because a column of ids is numbers.
_VALUE_EXPECTATIONS = frozenset(
    {
        "expect_column_distinct_values_to_be_in_set",
        "expect_column_distinct_values_to_contain_set",
        "expect_column_distinct_values_to_equal_set",
        "expect_column_most_common_value_to_be_in_set",
    }
)

# Expectations whose observed value is the table's shape -- its column names
# -- rather than anything in its rows. Kept.
_SCHEMA_EXPECTATIONS = frozenset(
    {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_columns_to_match_set",
        "expect_column_to_exist",
    }
)

_VALUES_REASON = "column values redacted"


def redact_values(payload: Any) -> Tuple[Any, List[Dict[str, str]]]:
    """
    Replace the column values an expectation observed with their count.

    `redact_samples` catches the failing rows GX samples, by name. This is
    the other way values leave: a distinct-values or most-common-value
    expectation reports the values it saw as `observed_value`, and GX 0.x
    adds each value's count as `details.value_counts`. A numeric aggregate
    (a mean, a row count, a column's max) is the substance of a result and
    is kept; so is a list of column names.

    :param payload: the dumped payload
    :return: the same shape with observed values summarised, and what was
        redacted, by path and reason
    """
    excluded: List[Dict[str, str]] = []
    return _walk_values(payload, "", excluded), excluded


def _walk_values(value: Any, path: str, excluded: List[Dict[str, str]]) -> Any:
    """
    Recurse through one value, redacting each expectation result in it.

    :param value: the value being walked
    :param path: dotted path of `value` from the payload root
    :param excluded: accumulator every redaction is appended to
    :return: the same shape, redacted
    """
    if isinstance(value, list):
        return [
            _walk_values(item, f"{path}[{i}]", excluded)
            for i, item in enumerate(value)
        ]
    if not isinstance(value, dict):
        return value
    out = {
        key: _walk_values(item, f"{path}.{key}" if path else str(key), excluded)
        for key, item in value.items()
    }
    result = out.get("result")
    config = out.get("expectation_config")
    if isinstance(result, dict) and isinstance(config, dict):
        kind = config.get("type") or config.get("expectation_type")
        out["result"] = _redact_result(
            result,
            str(kind or ""),
            f"{path}.result" if path else "result",
            excluded,
        )
    return out


def _redact_result(
    result: Dict[str, Any],
    kind: str,
    path: str,
    excluded: List[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Redact one expectation result's observed values.

    :param result: the expectation result's `result`
    :param kind: the expectation's type
    :param path: dotted path of `result`
    :param excluded: accumulator every redaction is appended to
    :return: the result, redacted
    """
    out = dict(result)
    observed = out.get("observed_value")
    if isinstance(observed, (list, dict)) and kind not in _SCHEMA_EXPECTATIONS:
        if kind in _VALUE_EXPECTATIONS or not _numeric(observed):
            out["observed_value"] = {"redacted": True, "count": len(observed)}
            excluded.append(
                {"path": f"{path}.observed_value", "reason": _VALUES_REASON}
            )
    details = out.get("details")
    if isinstance(details, dict) and "value_counts" in details:
        counts = details["value_counts"]
        details = dict(details)
        details["value_counts"] = {
            "redacted": True,
            "count": len(counts) if isinstance(counts, (list, dict)) else 0,
        }
        out["details"] = details
        excluded.append(
            {"path": f"{path}.details.value_counts", "reason": _VALUES_REASON}
        )
    return out


def _numeric(value: Any) -> bool:
    """
    Whether every leaf of a value is a number: quantiles, a histogram.

    :param value: an observed value
    :return: True when nothing in it is text
    """
    if isinstance(value, dict):
        return all(_numeric(item) for item in value.values())
    if isinstance(value, list):
        return all(_numeric(item) for item in value)
    return value is None or (
        isinstance(value, (int, float)) and not isinstance(value, bool)
    )


def forward_validation_result(
    result: Any,
    emitter: Optional[cemit.EmitterLike] = None,
    validator: Any = None,
    platform: Optional[str] = None,
    dataset_name: Optional[str] = None,
    platform_instance: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Forward one validation result that ran outside a checkpoint.

    GX runs actions only from a checkpoint: `ValidationDefinition.run()`,
    `Batch.validate()` and a 0.x `Validator.validate()` return their result
    to the caller and fire nothing. This sends that result the way the 0.x
    action sends its own, so a receiver reads it the same way.

    :param result: the `ExpectationSuiteValidationResult` GX returned
    :param emitter: emitter to send through; built from the environment when
        not given
    :param validator: on 0.x, the validator that produced it: its engine
        names the platform, as a checkpoint's runtime does, and is then left
        behind. 1.x looks its datasources up on the running context instead
    :param platform: the platform the batch lives on, to name it outright
    :param dataset_name: the table the batch is, to name it outright
    :param platform_instance: the instance of the platform it lives on
    :return: whether the observation was emitted, and whether it was redacted
    """
    urls: Dict[str, str] = {}
    url = _field(result, "result_url")
    if isinstance(url, str) and url:
        # No identifier exists outside a checkpoint; the receiver reads a
        # lone result under the empty one.
        urls[""] = url
    return forward(
        {
            "args": [validator] if validator is not None else [],
            "kwargs": {"validation_result_suite": result},
        },
        emitter,
        datasources=datasources_v1(None, results=[result]),
        result_urls=urls,
        named=named_dataset(platform, dataset_name, platform_instance),
    )


def add_settings(body: Any, emitter: Any) -> List[Dict[str, str]]:
    """
    Put the settings of the process the checkpoint ran in on its event.

    Nothing is added unless `CONVALESCE_SEND_SETTINGS` is on. A secret
    crosses as a hash keyed in this process; see `convalesce_emit.settings`.

    :param body: the payload about to be sent, changed in place
    :param emitter: the emitter it goes through, whose ingest key a hash
        is keyed from where no fingerprint key is set
    :return: every setting left out, by path and reason
    """
    if not isinstance(body, dict):
        return []
    key = getattr(getattr(emitter, "config", None), "ingest_key", None)
    found, left_out = cesettin.collect(
        key if isinstance(key, str) and key else None,
        {cesettin.ENVIRONMENT: cesettin.environment(), **cesettin.noted()},
    )
    if found:
        body[cesettin.FIELD] = found
    return list(left_out)


def forward(
    payload: Any,
    emitter: Optional[cemit.EmitterLike] = None,
    datasources: Optional[Dict[str, Dict[str, Any]]] = None,
    result_urls: Optional[Dict[str, str]] = None,
    named: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Send one validation result.

    A GX result carries the failing values it sampled, and a distinct-values
    expectation the values it observed. They are sent, because they are what
    says which rows broke the check, unless `CONVALESCE_GX_SEND_SAMPLES` is
    set false: then each is replaced by its count; see `redact_values`.

    The Great Expectations runtime is left behind whatever that setting: a
    validator owns the whole frame it validated, and no receiver reads a
    word of the data context hanging off it.

    :param payload: whatever GX handed the action
    :param emitter: emitter to send through; built from the environment when
        not given
    :param datasources: each datasource's platform and database, where the
        action could name them
    :param result_urls: each validation result's GX Cloud page, by
        identifier
    :param named: the platform, table and platform instance the action was
        told, as `named_dataset` returns them
    :return: whether the observation was emitted, and whether it was redacted
    """
    redact = not send_samples()
    try:
        # Inside the guard, not before it: GX fails the whole checkpoint when
        # an action raises.
        platform = runtime_platform(payload)
        if datasources:
            platform["datasources"] = datasources
        if result_urls:
            platform["result_urls"] = result_urls
        if named:
            platform.update(named)
        budget = cemit.new_budget()
        body = cemit.dump(shape(payload), budget=budget)
        if platform and isinstance(body, dict):
            body.update(platform)
        # Always, samples or not: a pandas `read_sql_*` asset keeps its `con`,
        # the connection string, in the batch spec every result carries.
        body, secrets = cemit.redact_secrets(body)
        # After the masking, so a password a query names is hidden before
        # the query is cut and cannot be left half masked.
        body, queries = bound_queries(body)
        excluded = budget.excluded + secrets + queries
        if redact:
            body, redacted = cemit.redact_samples(body)
            body, values = redact_values(body)
            excluded = excluded + redacted + values
        target = emitter or cemit.Emitter()
        # Last, past every pass above: each is for what a result or a
        # datasource carries, and a setting is already a value that is no
        # credential or a hash.
        excluded = excluded + add_settings(body, target)
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

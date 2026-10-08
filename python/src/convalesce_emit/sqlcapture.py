"""
Note the SQL a step runs, so its tables and columns can be worked out.

A step written as plain Python declares nothing: it opens a connection and
runs statements, and no tool records which. This module listens where
those statements pass -- SQLAlchemy's engines and the common database
drivers -- and keeps the text of each, for the plugin to send with the
step's own event. The receiver parses them into table and column lineage.

Only the statement text is kept, never the values bound to it. A plugin
brackets a step with `start()` and `drain()`; nothing is kept outside
that. Statements a tool runs against its own metadata database are told
apart by `ignore()`.

Where one process runs many steps at once -- threads, or tasks on one event
loop -- a single bracket cannot say whose a statement is. A plugin for such
a tool names, with `scope_by()`, a function that says which step the calling
code is in; statements are then kept apart per step and handed over with
`drain(scope=...)`.

On unless `CONVALESCE_SQL_CAPTURE` is false. Nothing here may break the
step it watches: every hook swallows its own failures and counts them.

Import as:

import convalesce_emit.sqlcapture as cesqlcap
"""

import collections
import importlib
import importlib.abc
import importlib.util
import logging
import re
import sys
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

import convalesce_emit.config as ceconfig

_LOG = logging.getLogger(__name__)

_ENV = "CONVALESCE_SQL_CAPTURE"
_FALSY = frozenset({"0", "false", "no", "off"})

MAX_STATEMENTS = 50
MAX_CHARS = 20_000
# How many steps' statements are held at once. A step whose plugin never
# asks for them would otherwise hold them for the life of the process.
MAX_SCOPES = 256

# What moves or shapes data. Session chatter (`SET`, `SHOW`, `BEGIN`,
# `SELECT 1`) says nothing about lineage.
_WORTH = re.compile(
    r"^\s*(?:/\*.*?\*/\s*)*(with|select|insert|update|delete|merge|create|"
    r"copy|call|truncate|alter|drop|replace|unload|export|refresh)\b",
    re.IGNORECASE | re.DOTALL,
)
# A driver or an ORM looking at the database's own catalogue.
_CATALOGUE = re.compile(
    r"\b(pg_catalog|information_schema|pg_class|pg_type|pg_namespace|"
    r"sqlite_master|sqlite_schema|alembic_version)\b",
    re.IGNORECASE,
)
_TRIVIAL = re.compile(r"^\s*select\s+[\w'\"()*]+\s*;?\s*$", re.IGNORECASE)

_lock = threading.Lock()
_statements: List[Dict[str, Any]] = []
# Module state, not constants: the hooks are process-wide by nature.
_dropped = 0  # pylint: disable=invalid-name
_failures = 0  # pylint: disable=invalid-name
_active = False  # pylint: disable=invalid-name
_installed = False  # pylint: disable=invalid-name
_ignored: List[Callable[[Dict[str, Any]], bool]] = []
# Says which step the calling code is in, where a plugin named one.
_scope: Optional[Callable[[], Optional[str]]] = (  # pylint: disable=invalid-name
    None
)
# What each step ran, oldest step first: `{"statements", "dropped"}`.
_scoped: "collections.OrderedDict[str, Dict[str, Any]]" = (
    collections.OrderedDict()
)
# Set on a function this module put in place, so it is never wrapped twice.
_MARK = "convalesce_noting"


def enabled() -> bool:
    """
    Whether statements may be noted.

    :return: False only when the setting says so
    """
    return (ceconfig.read_setting(_ENV) or "").strip().lower() not in _FALSY


def ignore(predicate: Callable[[Dict[str, Any]], bool]) -> None:
    """
    Leave out statements a tool runs for itself.

    :param predicate: given `{"statement", "dialect", "database", "url",
        "via"}`, says whether to leave it out
    """
    _ignored.append(predicate)


def scope_by(resolver: Optional[Callable[[], Optional[str]]]) -> None:
    """
    Keep statements apart by the step that ran them.

    The resolver is called where a statement passes, on the thread and in
    the context that ran it, so it can read whatever the tool keeps there
    to name the running step. A statement it names a step for is kept
    under that step, with no `start()` needed, until `drain(scope=...)`
    takes it; one it returns None for is kept only between `start()` and
    `drain()`, as without a resolver.

    :param resolver: returns the running step's id, or None outside any
        step; None here goes back to one bracket for the whole process
    """
    global _scope  # pylint: disable=global-statement
    with _lock:
        _scope = resolver
        _scoped.clear()


def _scope_key() -> Optional[str]:
    """
    The step the calling code is in, as the plugin's resolver names it.

    :return: its id, or None when no resolver is set, it names none or
        fails, or noting is off
    """
    global _failures  # pylint: disable=global-statement
    resolver = _scope
    if resolver is None or not enabled():
        return None
    try:
        key = resolver()
    except Exception:  # pylint: disable=broad-exception-caught
        _failures += 1
        return None
    return str(key) if key else None


def start() -> None:
    """Begin noting statements, forgetting any noted before."""
    global _active, _dropped  # pylint: disable=global-statement
    with _lock:
        _statements.clear()
        _dropped = 0
        _active = enabled()


def drain(scope: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Stop noting and hand over what was noted.

    :param scope: a step's id, as the `scope_by()` resolver names it, to
        hand over only what that step ran and leave everything else as it
        is; None for what was noted between `start()` and now
    :return: `{"statements": [{"statement", "dialect", "database",
        "schema", "via", "count"}], "dropped", "failures"}`; None when
        nothing was noted
    """
    global _active  # pylint: disable=global-statement
    with _lock:
        if scope is not None:
            held = _scoped.pop(str(scope), None) or {}
            taken = list(held.get("statements", ()))
            dropped, failures = int(held.get("dropped", 0)), _failures
        else:
            _active = False
            taken = list(_statements)
            _statements.clear()
            dropped, failures = _dropped, _failures
    if not taken and not dropped:
        return None
    return {"statements": taken, "dropped": dropped, "failures": failures}


def record(  # pylint: disable=too-many-arguments
    statement: Any,
    *,
    dialect: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    url: Optional[str] = None,
    via: str = "",
) -> None:
    """
    Note one statement, if it is worth noting.

    Safe to call from any hook: it never raises.

    :param statement: the SQL text, or something whose `str` is it
    :param dialect: the database kind, as the driver or engine names it
    :param database: the database connected to, when known
    :param schema: the default schema, when known
    :param url: where the connection goes, with no password; used only to
        tell a tool's own database apart, never sent
    :param via: which hook saw it
    """
    global _dropped, _failures  # pylint: disable=global-statement
    try:
        key = _scope_key()
        if key is None and not _active:
            return
        text = statement if isinstance(statement, str) else str(statement)
        if not _WORTH.match(text) or _TRIVIAL.match(text):
            return
        if _CATALOGUE.search(text):
            return
        entry: Dict[str, Any] = {
            "statement": text[:MAX_CHARS],
            "dialect": dialect,
            "database": database,
            "schema": schema,
            "via": via,
            "count": 1,
        }
        probe = {**entry, "url": url}
        if any(predicate(probe) for predicate in list(_ignored)):
            return
        with _lock:
            held = None if key is None else _held(key)
            noted = _statements if held is None else held["statements"]
            for seen in noted:
                # One statement often passes two hooks (an engine and its
                # driver), and a loop runs the same one many times.
                if seen["statement"] == entry["statement"]:
                    if seen["via"] == via:
                        seen["count"] += 1
                    seen["dialect"] = seen["dialect"] or dialect
                    seen["database"] = seen["database"] or database
                    seen["schema"] = seen["schema"] or schema
                    return
            if len(noted) >= MAX_STATEMENTS:
                if held is None:
                    _dropped += 1
                else:
                    held["dropped"] += 1
                return
            noted.append(entry)
    except Exception:  # pylint: disable=broad-exception-caught
        _failures += 1


def _held(key: str) -> Dict[str, Any]:
    """
    What one step has run so far, made on first use. Call with the lock.

    :param key: the step's id
    :return: its `{"statements", "dropped"}`
    """
    held = _scoped.get(key)
    if held is None:
        held = _scoped[key] = {"statements": [], "dropped": 0}
        while len(_scoped) > MAX_SCOPES:
            _scoped.popitem(last=False)
    return held


# #############################################################################
# hooks
# #############################################################################


def _safe_url(url: Any) -> Optional[str]:
    """A connection URL with its password left out."""
    try:
        render = getattr(url, "render_as_string", None)
        if callable(render):
            return str(render(hide_password=True))
        return re.sub(r"://([^:/@]+):[^@]*@", r"://\1:***@", str(url))
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def _hook_sqlalchemy() -> bool:
    """Listen on every SQLAlchemy engine, present and future."""
    try:
        from sqlalchemy import event  # pylint: disable=import-outside-toplevel
        from sqlalchemy.engine import (  # pylint: disable=import-outside-toplevel
            Engine,
        )
    except Exception:  # pylint: disable=broad-exception-caught
        return False

    def before(  # pylint: disable=too-many-arguments,too-many-positional-arguments,unused-argument
        conn: Any,
        cursor: Any,
        statement: Any,
        parameters: Any,
        context: Any,
        many: Any,
    ) -> None:
        try:
            engine = conn.engine
            url = engine.url
            record(
                statement,
                dialect=getattr(engine.dialect, "name", None),
                database=getattr(url, "database", None),
                url=_safe_url(url),
                via="sqlalchemy",
            )
        except Exception:  # pylint: disable=broad-exception-caught
            record(statement, via="sqlalchemy")

    event.listen(Engine, "before_cursor_execute", before)
    return True


# Drivers whose connection does not say which database it is on, and the one
# query that does. DuckDB names a file database by the file's stem.
_ASKED = {"duckdb": "select current_database(), current_schema()"}
# What each such connection answered, by its id: asked once, kept small.
_ANSWERED: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
_MAX_ANSWERED = 64


def _where(connection: Any, execute: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    The database and schema a connection is on, asked of it once.

    The question runs on the step's own connection just before its first
    statement, which replaces the answer as the connection's result. A
    connection that cannot be asked is not asked again.

    :param connection: the driver's connection
    :param execute: its own, unwrapped `execute`
    :return: the database and the schema, or None for either
    """
    key = id(connection)
    if key in _ANSWERED:
        return _ANSWERED[key]
    found: Tuple[Optional[str], Optional[str]] = (None, None)
    try:
        row = execute(connection, _ASKED["duckdb"]).fetchone()
        found = (
            str(row[0]) if row and row[0] else None,
            str(row[1]) if row and row[1] else None,
        )
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    if len(_ANSWERED) >= _MAX_ANSWERED:
        _ANSWERED.clear()
    _ANSWERED[key] = found
    return found


def _wrap_method(owner: Any, name: str, dialect: str, via: str) -> bool:
    """
    Replace one `execute`-like method on a Python-level class with one
    that notes its first argument and then does what it did.
    """
    original = getattr(owner, name, None)
    if original is None or getattr(original, _MARK, False):
        return False

    def noted(self: Any, operation: Any, *args: Any, **kwargs: Any) -> Any:
        database, schema = (
            _where(self, original) if dialect in _ASKED else (None, None)
        )
        record(
            operation, dialect=dialect, database=database, schema=schema, via=via
        )
        return original(self, operation, *args, **kwargs)

    setattr(noted, _MARK, True)
    noted.__name__ = getattr(original, "__name__", name)
    noted.__doc__ = getattr(original, "__doc__", None)
    try:
        setattr(owner, name, noted)
    except TypeError:
        # A class written in C takes no new attributes.
        return False
    return True


def _hook_psycopg2() -> bool:
    """
    psycopg2's cursor is written in C, so its methods cannot be replaced;
    a connection is asked for cursors of a subclass instead.
    """
    try:
        import psycopg2  # pylint: disable=import-outside-toplevel
        import psycopg2.extensions as ext  # pylint: disable=import-outside-toplevel
    except Exception:  # pylint: disable=broad-exception-caught
        return False
    if getattr(psycopg2.connect, _MARK, False):
        return False

    def cursor_of(base: Any) -> Any:
        class Noting(base):  # type: ignore[misc,valid-type]
            """A cursor that notes what it is asked to run."""

            def execute(self, query: Any, params: Any = None) -> Any:
                """Note the statement, then run it."""
                record(
                    (
                        query.as_string(self)
                        if hasattr(query, "as_string")
                        else query
                    ),
                    dialect="postgres",
                    database=getattr(self.connection.info, "dbname", None),
                    via="psycopg2",
                )
                return super().execute(query, params)

        return Noting

    def connection_of(base: Any) -> Any:
        class Noting(base):  # type: ignore[misc,valid-type]
            """A connection whose cursors note what they run."""

            def cursor(self, *args: Any, **kwargs: Any) -> Any:
                """Hand out a cursor of the noting kind."""
                chosen = kwargs.get("cursor_factory") or self.cursor_factory
                if not args or kwargs.get("cursor_factory"):
                    kwargs["cursor_factory"] = cursor_of(chosen or ext.cursor)
                return super().cursor(*args, **kwargs)

        return Noting

    original = psycopg2.connect

    def connect(*args: Any, **kwargs: Any) -> Any:
        try:
            kwargs["connection_factory"] = connection_of(
                kwargs.get("connection_factory") or ext.connection
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return original(*args, **kwargs)

    setattr(connect, _MARK, True)
    psycopg2.connect = connect
    return True


# Drivers whose cursors are Python classes: their `execute` is wrapped.
# The module to wait for, where the class is, its name and the dialect.
_PYTHON_DRIVERS = (
    ("psycopg", "psycopg", "Cursor", "postgres"),
    ("psycopg", "psycopg", "AsyncCursor", "postgres"),
    (
        "snowflake.connector",
        "snowflake.connector.cursor",
        "SnowflakeCursor",
        "snowflake",
    ),
    ("pymysql", "pymysql.cursors", "Cursor", "mysql"),
    ("mysql.connector", "mysql.connector.cursor", "MySQLCursor", "mysql"),
    ("redshift_connector", "redshift_connector", "Cursor", "redshift"),
    ("trino.dbapi", "trino.dbapi", "Cursor", "trino"),
    ("duckdb", "duckdb", "DuckDBPyConnection", "duckdb"),
)


def _hook(module: str) -> bool:
    """
    Hook one library that is already imported.

    :param module: the module that was waited for
    :return: whether anything was hooked
    """
    global _failures  # pylint: disable=global-statement
    try:
        if module == "sqlalchemy":
            return _hook_sqlalchemy()
        if module == "psycopg2":
            return _hook_psycopg2()
        hooked = False
        for waited, where, cls, dialect in _PYTHON_DRIVERS:
            if waited != module:
                continue
            owner = getattr(importlib.import_module(where), cls, None)
            if owner is not None:
                hooked = (
                    _wrap_method(owner, "execute", dialect, module) or hooked
                )
        return hooked
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _failures += 1
        _LOG.debug("convalesce: could not hook %s: %s", module, exc)
        return False


_WATCHED = ("sqlalchemy", "psycopg2") + tuple(
    dict.fromkeys(row[0] for row in _PYTHON_DRIVERS)
)


class _AfterImport(importlib.abc.MetaPathFinder):
    """
    Hooks a database library the moment the step first imports it.

    Importing every driver up front to hook it would cost each process a
    second or more for libraries it never uses. This finder lets the
    ordinary import happen and steps in once it has.
    """

    def __init__(self) -> None:
        self._waiting = set(_WATCHED)
        self._busy = threading.local()

    def find_spec(self, fullname: str, path: Any, target: Any = None) -> Any:
        """Let the real finders answer, then hook the module once loaded."""
        del path, target
        if fullname not in self._waiting or getattr(self._busy, "on", False):
            return None
        self._busy.on = True
        try:
            spec = importlib.util.find_spec(fullname)
        except Exception:  # pylint: disable=broad-exception-caught
            spec = None
        finally:
            self._busy.on = False
        if spec is None or spec.loader is None:
            return None
        loader = spec.loader
        execute = getattr(loader, "exec_module", None)
        if execute is None:
            return None
        waiting = self._waiting

        def exec_module(module: Any) -> None:
            execute(module)
            waiting.discard(fullname)
            _hook(fullname)

        try:
            loader.exec_module = exec_module  # type: ignore[method-assign]
        except Exception:  # pylint: disable=broad-exception-caught
            return None
        return spec


def install() -> List[str]:
    """
    Put the hooks in, once per process.

    A library already imported is hooked now; one imported later is hooked
    as it loads. Nothing is imported for the sake of hooking it.

    :return: the libraries hooked on this call
    """
    global _installed  # pylint: disable=global-statement
    if not enabled() or _installed:
        return []
    _installed = True
    hooked = [name for name in _WATCHED if name in sys.modules and _hook(name)]
    try:
        sys.meta_path.insert(0, _AfterImport())
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    return hooked

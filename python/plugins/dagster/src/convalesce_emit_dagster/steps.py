"""
What each step of a run ran: its code, its SQL and its configuration.

A run-status sensor sees a run from outside and after the fact. It can read
the definitions, so it can read the source of the function behind each
step. It cannot see what a step did while it ran: the SQL an op sent and
the configuration it was resolved with exist only in the process that ran
the step, which under the default executor is not even the run's own.

So this module works in two places. In a step's process it brackets the
step, notes the statements `convalesce_emit.sqlcapture` saw, the step's
resolved configuration and, where asked for, the settings its process had,
and writes them to the run's event log as one engine event. In the sensor
it reads those events back, reads each executed step's source from the
repository, and hands the sensor one entry per step.

The bracket is put in when this package is imported, which every step
process does, because it imports the definitions the sensor is declared
in. It wraps the one function every executor runs a step through. Where a
Dagster release has moved that function, nothing is wrapped and a run
crosses without its SQL; a step is never failed over it.

Import as:

import convalesce_emit_dagster.steps as cedsteps
"""

import importlib
import json
import logging
import sys
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional

import convalesce_emit as cemit
import convalesce_emit.config as ceconfig
import convalesce_emit.settings as cesettin
import convalesce_emit.source as cesource
import convalesce_emit.sqlcapture as cesqlcap
import convalesce_emit_dagster._env as cedagenv

_LOG = logging.getLogger(__name__)

_ARGUMENTS_ENV = "CONVALESCE_SEND_ARGUMENTS"
_FALSY = frozenset({"0", "false", "no", "off"})

# The metadata entry, on an engine event, that a step's note is written
# under. Namespaced like the `convalesce/query` an asset's author may set.
NOTE_KEY = "convalesce/step"
_NOTE_MESSAGE = "Convalesce noted what this step ran."

# Every executor runs a step through this one function: the in-process
# executor directly, the multiprocess one in the child it starts, a step
# launcher in the process it launches. The name is the same from Dagster
# 1.7 to 1.13.
_SEAM_MODULE = "dagster._core.execution.plan.execute_plan"
_SEAM_NAME = "dagster_event_sequence_for_step"
# A step launcher's remote process reaches the same function through a
# name this module took at its own import, before anything of ours ran.
_SEAM_HOLDERS = ("dagster._core.execution.plan.external_step",)
# Set on the function this module put in place, so it is never wrapped twice.
_MARK = "convalesce_bracketed"

# A note lives in the customer's event log, so it is kept far smaller than
# what the capture itself allows.
MAX_NOTE_CHARS = 200_000

# Where Dagster talks to its own database: the run, event-log and schedule
# storages, and the packages that put them on Postgres or MySQL. An IO
# manager lives beside them under `dagster._core.storage` and is not here,
# because the SQL it runs is the step's.
_OWN_STORAGE = (
    "dagster._core.storage.event_log",
    "dagster._core.storage.runs",
    "dagster._core.storage.schedules",
    "dagster._core.storage.sql",
    "dagster._core.storage.legacy_storage",
    "dagster._core.storage.partition_status_cache",
    "dagster._core.storage.migration",
    "dagster._core.storage.alembic",
    "dagster_postgres.",
    "dagster_mysql.",
    "dagster_cloud",
)
# Below this the frames are Dagster running the step; a statement that got
# here without passing a storage frame is the step's own.
_STEP_BOUNDARY = "dagster._core.execution.plan"
_MAX_FRAMES = 200

# What a step's note keeps the settings it could not send under, by path and
# reason. The sensor moves them to the event's `excluded`; they are in the
# note because only the step's process knows them.
SETTINGS_EXCLUDED = "settings_excluded"
# The sensor has an emitter and its key; a step's process has neither, only
# the variable an emitter would read.
_INGEST_KEY_ENV = "CONVALESCE_INGEST_KEY"

# Module state, not a constant: the bracket is process-wide by nature.
_installed = False  # pylint: disable=invalid-name


def arguments_enabled() -> bool:
    """
    Whether the values a step ran with may be sent.

    :return: False only when the setting says so
    """
    return (cedagenv.read(_ARGUMENTS_ENV) or "").strip().lower() not in _FALSY


# #############################################################################
# In the step's process
# #############################################################################


def install() -> bool:
    """
    Bracket every step this process runs, once.

    :return: whether the bracket was put in on this call
    """
    global _installed  # pylint: disable=global-statement
    if _installed:
        return False
    if not (
        cesqlcap.enabled()
        or arguments_enabled()
        or cesource.enabled()
        or cesettin.enabled()
    ):
        return False
    _installed = True
    try:
        module = importlib.import_module(_SEAM_MODULE)
        original = getattr(module, _SEAM_NAME, None)
        if not callable(original) or getattr(original, _MARK, False):
            return False
        wrapped = bracketed(original)
        setattr(module, _SEAM_NAME, wrapped)
        for name in _SEAM_HOLDERS:
            holder = sys.modules.get(name)
            if getattr(holder, _SEAM_NAME, None) is original:
                setattr(holder, _SEAM_NAME, wrapped)
        cesqlcap.ignore(own_traffic)
        cesqlcap.install()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # No Dagster, or one that keeps its step execution elsewhere: the
        # sensor still reports the run, without what the steps ran.
        _LOG.debug("convalesce: steps are not bracketed: %s", exc)
        return False
    return True


def bracketed(original: Callable[..., Iterator[Any]]) -> Callable[..., Any]:
    """
    Wrap Dagster's step execution so each step is noted as it ends.

    The wrapper yields exactly what the original does and lets every
    exception through untouched; what it adds happens before the first
    event and after the last, and cannot raise.

    :param original: Dagster's `dagster_event_sequence_for_step`
    :return: the same generator function, bracketed
    """

    def sequence(step_context: Any, *args: Any, **kwargs: Any) -> Iterator[Any]:
        _begin()
        try:
            yield from original(step_context, *args, **kwargs)
        finally:
            _finish(step_context)

    setattr(sequence, _MARK, True)
    sequence.__name__ = getattr(original, "__name__", _SEAM_NAME)
    sequence.__doc__ = getattr(original, "__doc__", None)
    return sequence


def _begin() -> None:
    """Start noting statements for the step about to run."""
    try:
        cesqlcap.start()
    except Exception:  # pylint: disable=broad-exception-caught
        pass


def _finish(step_context: Any) -> None:
    """
    Write what the step ran to the run's event log.

    :param step_context: Dagster's context for the step that just ended
    """
    try:
        note = note_of(step_context, cesqlcap.drain())
        if note:
            write_note(step_context, note)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not note a step: %s", exc)


def note_of(
    step_context: Any, noted: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    What one step ran, as plain data with its secrets masked.

    :param step_context: Dagster's context for the step
    :param noted: what `sqlcapture.drain` handed over, if anything
    :return: `{"sql", "config", "ran": {"sha256", "file_sha256"},
        "settings", "settings_excluded"}`, each only when there is one;
        empty when the step left nothing to say
    """
    note: Dict[str, Any] = {}
    if noted and cesqlcap.enabled():
        note["sql"] = _fitted(noted)
    if arguments_enabled():
        config = _plain(getattr(step_context, "op_config", None))
        if config not in (None, {}, []):
            note["config"] = config
    if note:
        # Only the hashes: the text is read by the sensor, and a copy of it
        # per step per run has no place in the customer's event log. They
        # say whether the code that ran is the code the sensor then read.
        source = cesource.of(_compute_fn(getattr(step_context, "op_def", None)))
        if source:
            note["ran"] = {
                name: source[name]
                for name in ("sha256", "file_sha256")
                if source.get(name)
            }
    masked, _ = cemit.redact_secrets(note)
    out = masked if isinstance(masked, dict) else {}
    # After the masking, not before: a setting is already either a value
    # that is no credential or a hash, and neither is to be changed here.
    out.update(_settings())
    return out


def _settings() -> Dict[str, Any]:
    """
    The settings this step's process had, as a note carries them.

    Read here and not in the sensor, whose environment is the daemon's:
    under the multiprocess executor or a step launcher a step has its own.

    :return: `{"settings", "settings_excluded"}`, each only when there is
        one; empty unless `CONVALESCE_SEND_SETTINGS` is on
    """
    # Trimmed as an emitter trims it, so a hash made here and one made
    # where the key is held by an emitter are under the same key.
    ingest_key = (ceconfig.read_setting(_INGEST_KEY_ENV) or "").strip()
    found, left_out = cesettin.collect(
        ingest_key or None,
        {cesettin.ENVIRONMENT: cesettin.environment(), **cesettin.noted()},
    )
    out: Dict[str, Any] = {}
    if found:
        out[cesettin.FIELD] = found
    if left_out:
        out[SETTINGS_EXCLUDED] = left_out
    return out


def _fitted(noted: Dict[str, Any]) -> Dict[str, Any]:
    """
    Noted statements, cut to what one event-log entry should hold.

    :param noted: what `sqlcapture.drain` handed over
    :return: the same shape, with the statements past `MAX_NOTE_CHARS`
        counted into `dropped`
    """
    kept: List[Any] = []
    used = 0
    dropped = int(noted.get("dropped") or 0)
    for entry in noted.get("statements") or []:
        size = len(str(entry.get("statement", "")))
        if used + size > MAX_NOTE_CHARS:
            dropped += 1
            continue
        used += size
        kept.append(entry)
    return {**noted, "statements": kept, "dropped": dropped}


def _plain(value: Any) -> Any:
    """
    A resolved configuration as JSON can carry it.

    :param value: what Dagster resolved, which may hold enum members or
        other Python objects a config type mapped to
    :return: the same, as plain data; None when it cannot be made plain
    """
    if value is None:
        return None
    try:
        return json.loads(json.dumps(cemit.dump(value), default=str))
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def write_note(step_context: Any, note: Dict[str, Any]) -> None:
    """
    Put one step's note in the run's event log, as an engine event.

    An engine event is the one event a step can carry structured data on
    without declaring an asset or an output for it, so it is there for a
    plain op and for a step that failed alike.

    :param step_context: Dagster's context for the step
    :param note: what `note_of` returned
    """
    # pylint: disable=import-outside-toplevel
    from dagster import DagsterEvent, MetadataValue
    from dagster._core.events import EngineEventData

    DagsterEvent.engine_event(
        step_context,
        _NOTE_MESSAGE,
        EngineEventData(metadata={NOTE_KEY: MetadataValue.json(note)}),
    )


def own_traffic(probe: Dict[str, Any]) -> bool:
    """
    Whether a statement is Dagster talking to its own database.

    Told by who is asking, not by what is asked: a step may well write to
    the database Dagster keeps its runs in, and Dagster's table names are
    ordinary words. The stack is walked outward from the driver; Dagster's
    storage code on it means the statement is Dagster's, and reaching the
    code that runs the step without passing any means it is the step's.

    :param probe: the statement as `sqlcapture.ignore` describes it; not
        looked at
    :return: True for a statement to leave out
    """
    del probe
    try:
        frame: Any = sys._getframe(1)  # pylint: disable=protected-access
    except Exception:  # pylint: disable=broad-exception-caught
        return False
    for _ in range(_MAX_FRAMES):
        if frame is None:
            return False
        name = frame.f_globals.get("__name__") or ""
        if name.startswith(_OWN_STORAGE):
            return True
        if name.startswith(_STEP_BOUNDARY):
            return False
        frame = frame.f_back
    return False


# #############################################################################
# In the sensor
# #############################################################################


def describe(
    context: Any, run: Any, step_stats: Any = None
) -> List[Dict[str, Any]]:
    """
    One entry per step the run executed, with what that step ran.

    :param context: Dagster's run-status context
    :param run: the run the context exposed
    :param step_stats: the run's step stats, when already read
    :return: `[{"step_key", "source", "sql", "config", "ran", "settings",
        "settings_excluded"}]`, each field only where there is one; empty
        when nothing could be read
    """
    run_id = getattr(run, "run_id", None)
    notes = read_notes(getattr(context, "instance", None), run_id)
    keys = [
        key
        for key in (getattr(stat, "step_key", None) for stat in step_stats or ())
        if isinstance(key, str)
    ]
    keys += [key for key in notes if key not in keys]
    if not keys:
        return []
    functions = (
        step_functions(context, getattr(run, "job_name", None))
        if cesource.enabled()
        else {}
    )
    arguments = arguments_enabled()
    out: List[Dict[str, Any]] = []
    sent: set = set()
    read: Dict[str, Optional[Dict[str, Any]]] = {}
    for key in keys:
        entry: Dict[str, Any] = {"step_key": key}
        node = node_of(key)
        if node not in read:
            read[node] = cesource.of(functions.get(node))
        source = read[node]
        if source:
            if source.get("sha256") in sent:
                # A mapped step runs one function many times; its text
                # crosses once and the rest point at it by hash.
                source = {k: v for k, v in source.items() if k != "text"}
            sent.add(source.get("sha256"))
            entry["source"] = source
        for name, value in notes.get(key, {}).items():
            if name == "config" and not arguments:
                continue
            entry[name] = value
        if len(entry) > 1:
            out.append(entry)
    return out


def take_excluded(steps: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Move what each step's settings left out from its entry to one list.

    :param steps: what `describe` returned, changed in place
    :return: what was left out, by path from the payload's root and reason
    """
    out: List[Dict[str, str]] = []
    for index, entry in enumerate(steps):
        left_out = entry.pop(SETTINGS_EXCLUDED, None)
        if not isinstance(left_out, list):
            continue
        for item in left_out:
            # Read back from the event log, so only the shape it was
            # written in is passed on.
            path = item.get("path") if isinstance(item, dict) else None
            reason = item.get("reason") if isinstance(item, dict) else None
            if isinstance(path, str) and isinstance(reason, str):
                out.append({"path": f"steps[{index}].{path}", "reason": reason})
    return out


def node_of(step_key: str) -> str:
    """
    The node a step key names, without the mapping key of a mapped step.

    :param step_key: a step key such as `load`, `refresh.tidy` or
        `load[eu]`
    :return: the node's handle, dot-joined through any graphs
    """
    out: List[str] = []
    depth = 0
    for char in step_key:
        if char == "[":
            depth += 1
        elif char == "]":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(char)
    return "".join(out)


def read_notes(instance: Any, run_id: Any) -> Dict[str, Dict[str, Any]]:
    """
    The notes the run's steps wrote, by step key.

    A step tried more than once writes one note per try; their statements
    are put together and the last try's configuration and hashes kept.

    :param instance: the run-status context's own `instance`
    :param run_id: the run to read
    :return: step key to its note; empty when none could be read
    """
    method = getattr(instance, "get_records_for_run", None)
    if not callable(method) or not run_id:
        return {}
    try:
        # pylint: disable=import-outside-toplevel
        from dagster import DagsterEventType

        connection = method(run_id, of_type={DagsterEventType.ENGINE_EVENT})
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the step notes: %s", exc)
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for record in getattr(connection, "records", None) or ():
        entry = getattr(record, "event_log_entry", None)
        event = getattr(entry, "dagster_event", None)
        key = getattr(event, "step_key", None)
        note = _note_in(event)
        if isinstance(key, str) and note:
            out[key] = _merged(out.get(key), note)
    return out


def _note_in(event: Any) -> Optional[Dict[str, Any]]:
    """
    The note an engine event carries, if it is one of ours.

    :param event: a `DagsterEvent`
    :return: the note, or None for any other engine event
    """
    data = getattr(event, "event_specific_data", None)
    metadata = getattr(data, "metadata", None)
    if not isinstance(metadata, Mapping):
        return None
    value = metadata.get(NOTE_KEY)
    note = getattr(value, "data", None)
    return note if isinstance(note, dict) else None


def _merged(
    earlier: Optional[Dict[str, Any]], later: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Two tries of one step as one note.

    :param earlier: what the tries before left, if any
    :param later: the next try's note
    :return: the later note, with the earlier statements it did not repeat
    """
    if not earlier:
        return dict(later)
    out = {**earlier, **later}
    before = earlier.get("sql")
    after = later.get("sql")
    if isinstance(before, dict) and isinstance(after, dict):
        statements = list(before.get("statements") or [])
        seen = {entry.get("statement") for entry in statements}
        statements += [
            entry
            for entry in after.get("statements") or []
            if entry.get("statement") not in seen
        ]
        out["sql"] = {
            **after,
            "statements": statements,
            "dropped": int(before.get("dropped") or 0)
            + int(after.get("dropped") or 0),
        }
    elif isinstance(before, dict):
        out["sql"] = before
    return out


def step_functions(context: Any, job_name: Any) -> Dict[str, Any]:
    """
    Node handle to the function behind it, for the job a run ran.

    Read from the repository the sensor is declared in, the only place the
    definitions can be reached from. A job in another code location is not
    there, and its steps cross without their source.

    :param context: Dagster's run-status context
    :param job_name: the run's job
    :return: dot-joined node handle to the decorated function; empty when
        the repository or the job cannot be read
    """
    repository_def = getattr(context, "repository_def", None)
    if repository_def is None:
        return {}
    out: Dict[str, Any] = {}
    try:
        # Each asset's own node first, so that a job the repository cannot
        # hand over by name still has the functions of its assets.
        for assets_def in repository_def.assets_defs_by_key.values():
            node_def = getattr(assets_def, "node_def", None)
            name = getattr(node_def, "name", None)
            if isinstance(name, str) and name not in out:
                _walk(node_def, name, out)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read the asset definitions: %s", exc)
    try:
        job_def = repository_def.get_job(job_name)
        for node in getattr(job_def, "nodes", None) or ():
            _walk(node.definition, node.name, out)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not read job %s: %s", job_name, exc)
    return out


def _walk(
    node_def: Any, handle: str, out: Dict[str, Any], depth: int = 0
) -> None:
    """
    Collect the functions under one node, through any graphs.

    :param node_def: an op's or a graph's definition
    :param handle: the node's handle so far
    :param out: where the functions are collected, by handle
    :param depth: how many graphs deep this node is
    """
    function = _compute_fn(node_def)
    if function is not None:
        out[handle] = function
        return
    if depth > 20:
        return
    for node in getattr(node_def, "nodes", None) or ():
        _walk(node.definition, f"{handle}.{node.name}", out, depth + 1)


def _compute_fn(op_def: Any) -> Any:
    """
    The function an op's author wrote.

    :param op_def: an `OpDefinition`, or anything else
    :return: the decorated function; None when this is not an op
    """
    compute_fn = getattr(op_def, "compute_fn", None)
    if compute_fn is None:
        return None
    return getattr(compute_fn, "decorated_fn", compute_fn)

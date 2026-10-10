"""
The settings a task ran with: its worker's environment and the Airflow
Variables it read.

Sent when a task ends and only where `CONVALESCE_SEND_SETTINGS` is on. What
crosses as a value and what crosses as a keyed hash is the core's decision;
see `convalesce_emit.settings`.

The Variables are the ones the task read, noted as it read them. Airflow
renders a task's templates before it fires `on_task_instance_running`, so a
note started from that hook would miss `{{ var.value.my_var }}`, the
commonest read there is. The reader is wrapped as the plugin loads instead,
which is before any template is rendered: `Variable.get` on Airflow 2, and
on Airflow 3 the task SDK's `_get_variable`, which the template accessor and
`Variable.get` both go through.

A Variable a task depends on without reading it through Airflow, or one read
where the wrapping could not be put in place, can be named in
`CONVALESCE_SETTINGS_VARIABLES`, a comma-separated list of exact names. Each
is read once when the task ends.

Import as:

import convalesce_emit_airflow.runsettings as cealruns
"""

import importlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import convalesce_emit.settings as cesettin
import convalesce_emit_airflow._env as cealenv

_LOG = logging.getLogger(__name__)

VARIABLES_SETTING = "CONVALESCE_SETTINGS_VARIABLES"
# How many names the list may hold. Each is a read of the metadata database
# or a secrets backend, made in the task's own process.
MAX_NAMES = 50
# How long the listed Variables may take to read between them. A task that
# has ended is not held up longer than this by a backend slow to answer.
_READ_SECONDS = 5.0
# The SDK first: on Airflow 3 the model's `get` only forwards to it, with a
# deprecation warning in the task's log.
_VARIABLE_MODULES = ("airflow.sdk", "airflow.models.variable")
_SDK_CONTEXT = "airflow.sdk.execution_time.context"
_NOTING = "_convalesce_noting"

_STARTED: Dict[str, bool] = {}


def start() -> None:
    """
    Note that a task is running in this process.

    The scheduler fails a task it found dead from its own process, where
    the environment is the scheduler's and not the worker's the task ran
    on. Only a process that saw a task start sends settings.

    :return: nothing
    """
    _STARTED["on"] = True


def watch_variables() -> None:
    """
    Note each Airflow Variable as it is read, from here on.

    Safe to call more than once, and where Airflow is absent or shaped
    otherwise: what cannot be wrapped is left as it is.

    :return: nothing
    """
    try:
        _watch_sdk()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: sdk variables not noted: %s", type(exc).__name__)
    try:
        _watch_model()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: variables not noted: %s", type(exc).__name__)


def collect(
    ingest_key: Optional[str],
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """
    What to send for the settings of the task that just ended.

    :param ingest_key: the key this process sends with, if it has one
    :return: what to send under `settings`, empty when there is nothing to
        send, and everything that was left out, by path and reason
    """
    try:
        if not _STARTED or not cesettin.enabled():
            # What was noted is another task's by the next one's end.
            cesettin.noted()
            return {}, []
        excluded = _read_listed()
        kinds: Dict[str, Dict[str, str]] = {
            cesettin.ENVIRONMENT: cesettin.environment()
        }
        kinds.update(cesettin.noted())
        found, left_out = cesettin.collect(ingest_key, kinds)
        return found, excluded + left_out
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A task must not fail because its settings could not be read. The
        # exception's class and not its message, which a secrets backend
        # may have written a value into.
        _LOG.warning(
            "convalesce: could not collect settings: %s", type(exc).__name__
        )
        return {}, []


def _note(name: Any, value: Any) -> None:
    """
    Remember one Variable a task read.

    :param name: the Variable's key
    :param value: what the read returned; text, or something already parsed
    :return: nothing
    """
    if not isinstance(name, str) or value is None:
        return
    if not isinstance(value, str):
        # `deserialize_json`, or a secrets backend of the customer's own.
        value = json.dumps(value, sort_keys=True, default=str)
    cesettin.note(cesettin.VARIABLE, name, value)


def _watch_sdk() -> None:
    """
    Wrap the one function every Variable read goes through on Airflow 3.

    :return: nothing
    """
    module = importlib.import_module(_SDK_CONTEXT)
    original = getattr(module, "_get_variable", None)
    if original is None or getattr(original, _NOTING, False):
        return

    def noted(key: Any, *args: Any, **kwargs: Any) -> Any:
        value = original(key, *args, **kwargs)
        try:
            _note(key, value)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return value

    setattr(noted, _NOTING, True)
    setattr(module, "_get_variable", noted)


def _watch_model() -> None:
    """
    Wrap `Variable.get` on Airflow 2, which templates and tasks both call.

    On Airflow 3 the model's `get` forwards to the SDK, which is wrapped
    already, so it is left alone there.

    :return: nothing
    """
    try:
        importlib.import_module(_SDK_CONTEXT)
        return
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    module = importlib.import_module("airflow.models.variable")
    variable = getattr(module, "Variable", None)
    held = getattr(variable, "__dict__", {}).get("get")
    original = getattr(held, "__func__", None)
    if variable is None or original is None:
        return
    if getattr(original, _NOTING, False):
        return

    def noted(cls: Any, key: Any, *args: Any, **kwargs: Any) -> Any:
        value = original(cls, key, *args, **kwargs)
        try:
            _note(key, value)
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        return value

    setattr(noted, _NOTING, True)
    variable.get = classmethod(noted)


def _names() -> List[str]:
    """
    The Variables `CONVALESCE_SETTINGS_VARIABLES` names.

    :return: its names in the order given, each once
    """
    out: List[str] = []
    for name in (cealenv.read(VARIABLES_SETTING) or "").split(","):
        name = name.strip()
        if name and name not in out:
            out.append(name)
    return out


def _read_listed() -> List[Dict[str, str]]:
    """
    Read each listed Variable once, so it is noted like any other read.

    :return: the listed names that could not be read, by path and reason
    """
    excluded: List[Dict[str, str]] = []
    listed = _names()
    field = f"{cesettin.FIELD}.{cesettin.VARIABLE}"
    if len(listed) > MAX_NAMES:
        listed = listed[:MAX_NAMES]
        excluded.append(
            {"path": field, "reason": f"limited to {MAX_NAMES} listed names"}
        )
    deadline = time.monotonic() + _READ_SECONDS
    for name in listed:
        path = f"{field}.{name}"
        if time.monotonic() > deadline:
            excluded.append({"path": path, "reason": "not read in time"})
            continue
        try:
            _note(name, _get_variable(name))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # A backend that cannot answer for one name may for the next.
            _LOG.debug(
                "convalesce: could not read variable %s: %s",
                name,
                type(exc).__name__,
            )
            excluded.append({"path": path, "reason": "could not be read"})
    return excluded


def _get_variable(name: str) -> Any:
    """
    Ask Airflow for one Variable, by name.

    :param name: the Variable's key
    :return: its value, or None when Airflow has no such Variable
    """
    for module_name in _VARIABLE_MODULES:
        try:
            module = importlib.import_module(module_name)
        except Exception:  # pylint: disable=broad-exception-caught
            # Airflow 2 has no task SDK; the model is there instead.
            continue
        variable = getattr(module, "Variable", None)
        if variable is not None:
            # The default is passed by position: the SDK calls it `default`
            # and the model `default_var`.
            return variable.get(name, None)
    return None

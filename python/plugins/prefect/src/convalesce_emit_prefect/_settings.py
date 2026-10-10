"""
The settings a run had: its process's environment and the Prefect Variables
it read.

Sent with the event that ends a flow run or a task run, and only where
`CONVALESCE_SEND_SETTINGS` is on. What crosses as a value and what crosses
as a keyed hash is the core's decision; see `convalesce_emit.settings`.

The Variables are the ones the run's process read, noted as it read them.
Nothing lists them afterwards: a workspace holds every flow's Variables,
and only the read says which of them this run turned on. `Variable.get` is
wrapped as the package is imported. On Prefect 2.20 it is an async function
made callable from both worlds; from 3.1 it is a plain function that hands
back a coroutine when called from async code, with `Variable.aget` beside
it. The wrapper calls the original exactly as it was called and hands back
what it returned, noting the value once there is one.

Variables are noted by process, not by run. A task run's event carries the
ones read so far; the flow run's last event carries them and forgets them.

Import as:

import convalesce_emit_prefect._settings as ceset
"""

import functools
import importlib
import inspect
import json
from typing import Any, Callable, Dict, List, Optional, Tuple

import convalesce_emit.config as ceconfig
import convalesce_emit.settings as cesettin
import convalesce_emit_prefect._mask as cemask

_LOG = cemask.logger(__name__)

_VARIABLES = "prefect.variables"
# `get` on every supported release; `aget` beside it from Prefect 3.1.
_READERS = ("get", "aget")
# Set on each function this module put in place, so none is wrapped twice.
_NOTING = "_convalesce_noting"
# The variable an emitter built from the environment reads its key from.
_INGEST_KEY_ENV = "CONVALESCE_INGEST_KEY"


def watch_variables() -> None:
    """
    Note each Prefect Variable as it is read, from here on.

    Safe to call more than once, and where Prefect is absent or shaped
    otherwise: what cannot be wrapped is left as it is. With settings off
    Prefect is not touched at all.

    :return: nothing
    """
    try:
        if not cesettin.enabled():
            return
        module = importlib.import_module(_VARIABLES)
        variable = getattr(module, "Variable", None)
        for name in _READERS:
            _watch_classmethod(variable, name)
        _watch_function(module, "get")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # The class and not the message: nothing a reader raised is logged.
        _LOG.debug("convalesce: variables not noted: %s", type(exc).__name__)


def collect(
    ingest_key: Optional[str], forget: bool
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """
    What to send for the settings of the run that just ended.

    :param ingest_key: the key this process sends with, if it has one
    :param forget: whether the Variables noted so far are forgotten, as
        they are once the flow run that read them is over
    :return: what to send under `settings`, empty when there is nothing to
        send, and everything that was left out, by path and reason
    """
    try:
        kinds: Dict[str, Dict[str, str]] = {
            cesettin.ENVIRONMENT: cesettin.environment()
        }
        kinds.update(cesettin.noted(clear=forget))
        found, left_out = cesettin.collect(ingest_key, kinds)
        return dict(found), list(left_out)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A run must not fail because its settings could not be read. The
        # exception's class and not its message, which may hold a value.
        _LOG.warning(
            "convalesce: could not collect settings: %s", type(exc).__name__
        )
        return {}, []


def ingest_key_of(emitter: Any) -> Optional[str]:
    """
    The key an event is sent with, which a setting's hash is keyed from.

    :param emitter: the emitter the hook was given, if it was given one
    :return: its ingest key; the environment's, as the emitter built for a
        hook given none would read it; None where there is neither
    """
    if emitter is None:
        key: Any = (ceconfig.read_setting(_INGEST_KEY_ENV) or "").strip()
    else:
        key = getattr(getattr(emitter, "config", None), "ingest_key", None)
    return key if isinstance(key, str) and key else None


def _watch_classmethod(variable: Any, name: str) -> None:
    """
    Wrap one reader on `Variable`, where it is the classmethod expected.

    :param variable: Prefect's `Variable` class, or whatever is there
    :param name: `get` or `aget`
    :return: nothing
    """
    held = getattr(variable, "__dict__", {}).get(name)
    if not isinstance(held, classmethod):
        return
    original = held.__func__
    if not callable(original) or getattr(original, _NOTING, False):
        return
    # Past `cls`, the Variable's name comes first.
    setattr(variable, name, classmethod(_noting(original, 1)))


def _watch_function(module: Any, name: str) -> None:
    """
    Wrap the module's own reader, which older code still calls.

    :param module: `prefect.variables`
    :param name: `get`
    :return: nothing
    """
    original = getattr(module, name, None)
    if not inspect.isfunction(original) or getattr(original, _NOTING, False):
        return
    setattr(module, name, _noting(original, 0))


def _noting(original: Callable[..., Any], position: int) -> Callable[..., Any]:
    """
    A reader that notes what it read and is otherwise the original.

    :param original: one of Prefect's Variable readers
    :param position: where the Variable's name is among its arguments
    :return: a function called the same way and returning the same thing:
        a value, or an awaitable of one where the original returned that
    """
    # Told by the function's own code, never by a mark put on it: a reader
    # that serves both worlds can pass for a coroutine function, and one
    # awaited here would hand sync code a coroutine where it had a value.
    flags = getattr(getattr(original, "__code__", None), "co_flags", 0)
    # pylint: disable-next=no-member
    if inspect.isfunction(original) and flags & inspect.CO_COROUTINE:
        # Kept a coroutine function, for whatever asks whether it is one.
        @functools.wraps(original)
        async def read_async(*args: Any, **kwargs: Any) -> Any:
            value = await original(*args, **kwargs)
            _note(_name_in(args, kwargs, position), value)
            return value

        wrapped: Callable[..., Any] = read_async
    else:

        @functools.wraps(original)
        def read(*args: Any, **kwargs: Any) -> Any:
            value = original(*args, **kwargs)
            name = _name_in(args, kwargs, position)
            if inspect.isawaitable(value):
                # Called from async code: the value is not there yet.
                return _resolved(value, name)
            _note(name, value)
            return value

        wrapped = read
    setattr(wrapped, _NOTING, True)
    return wrapped


async def _resolved(awaitable: Any, name: Any) -> Any:
    """
    Await a read on the caller's behalf, noting what it resolves to.

    :param awaitable: what the original reader returned
    :param name: the Variable's name
    :return: what the awaitable resolved to
    """
    value = await awaitable
    _note(name, value)
    return value


def _name_in(
    args: Tuple[Any, ...], kwargs: Dict[str, Any], position: int
) -> Any:
    """
    The Variable's name, however the reader was handed it.

    :param args: the reader's positional arguments
    :param kwargs: its keyword arguments
    :param position: where the name is when it is positional
    :return: the name, or None when it was not given
    """
    if len(args) > position:
        return args[position]
    return kwargs.get("name")


def _note(name: Any, value: Any) -> None:
    """
    Remember one Variable a run read.

    Called from inside Prefect's own reader, which must still return.

    :param name: the Variable's name
    :param value: what the read returned: text on Prefect 2, any JSON value
        on Prefect 3, or a `Variable` holding either
    :return: nothing
    """
    try:
        if not isinstance(name, str):
            return
        if type(value).__name__ == "Variable" and hasattr(value, "value"):
            value = value.value
        if value is None:
            return
        if not isinstance(value, str):
            value = json.dumps(value, sort_keys=True, default=str)
        cesettin.note(cesettin.VARIABLE, name, value)
    except Exception:  # pylint: disable=broad-exception-caught
        pass

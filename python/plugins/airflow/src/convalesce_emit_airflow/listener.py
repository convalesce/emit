"""
Airflow listener that forwards each callback's payload.

Upstream's plugin needs a `_airflow_version_specific` module because it reads
named attributes off a TaskInstance, and Airflow renames them between
releases. This one forwards the object whole, so a rename cannot break it.

The listener API itself is another matter, and three things about it were
found only by registering with a real Airflow rather than by reading:

- Airflow registers listeners through pluggy, which sees only methods marked
  with its `hookimpl`. Without the marker the plugin loads, reports nothing,
  and never fires.
- pluggy matches a hook's parameters by inspecting its code object, so
  `**kwargs` captures nothing. A hook declared without `error` silently drops
  it, and `error` is the failure message detection exists to read.
- Declaring a hook this Airflow does not specify makes pluggy reject the
  plugin, taking the scheduler down at startup.

So the implementations are generated from the hookspecs of the Airflow in
this process: exactly the parameters it passes, forwarded whole.

Import as:

import convalesce_emit_airflow.listener as cealist
"""

import functools
import importlib
import inspect
import logging
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple, Type

import convalesce_emit as cemit

_LOG = logging.getLogger(__name__)

TOOL = "airflow"

# Where Airflow declares what it will pass. Read as plain modules: asking the
# listener manager instead builds it, which loads plugins, which imports this
# module again.
_SPEC_MODULES = (
    # Airflow 2.5 - 2.11.
    "airflow.listeners.spec.taskinstance",
    "airflow.listeners.spec.dagrun",
    "airflow.listeners.spec.lifecycle",
    # Airflow 3 moved the task specs; the dag-run ones stayed put. Both are
    # listed rather than switched on a version number, so a release that
    # moves one and not the other still resolves.
    "airflow._shared.listeners.spec.taskinstance",
    "airflow._shared.listeners.spec.dagrun",
    "airflow.sdk._shared.listeners.spec.taskinstance",
)


# The events worth forwarding. Anything else Airflow offers is either noise
# or not about a run.
WANTED = (
    "on_task_instance_running",
    "on_task_instance_success",
    "on_task_instance_failed",
    "on_dag_run_running",
    "on_dag_run_success",
    "on_dag_run_failed",
    # Airflow 3 only; absent from 2.x, which the spec read handles.
    "on_task_instance_skipped",
)

# Used when the spec modules cannot be read; the shape these have carried
# since the listener API landed in Airflow 2.5.
FALLBACK_SPECS: Dict[str, Tuple[str, ...]] = {
    "on_task_instance_running": ("previous_state", "task_instance", "session"),
    "on_task_instance_success": ("previous_state", "task_instance", "session"),
    "on_task_instance_failed": (
        "previous_state",
        "task_instance",
        "error",
        "session",
    ),
}

try:
    from airflow.listeners import hookimpl
except Exception:  # pylint: disable=broad-exception-caught
    # A no-op stand-in purely so this module imports where Airflow is absent,
    # such as in tests and linting. It never fakes registration.
    def hookimpl(fn: Callable[..., Any]) -> Callable[..., Any]:  # type: ignore[misc]
        """Return the function unchanged."""
        return fn


# #############################################################################
# _Base
# #############################################################################


class _Base:
    """
    Everything a listener does that is not a hook.

    :param emitter: emitter to send through; built from the environment on
        first use when not given
    """

    def __init__(self, emitter: Optional[cemit.EmitterLike] = None) -> None:
        self._emitter = emitter
        self._version = cemit.version_of("airflow")

    @property
    def emitter(self) -> Optional[cemit.EmitterLike]:
        """
        The shared emitter, built on first use.

        :return: the emitter, or None if one could not be built
        """
        if self._emitter is None:
            try:
                self._emitter = cemit.Emitter()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                # A misconfigured emitter must not stop Airflow starting.
                _LOG.warning(
                    "convalesce: emitter unavailable, not emitting: %s", exc
                )
                return None
        return self._emitter

    def send(self, event: str, **payload: Any) -> None:
        """
        Forward one callback payload.

        :param event: which hook fired
        :param payload: whatever Airflow handed the hook
        :return: nothing
        """
        emitter = self.emitter
        if emitter is None:
            return
        try:
            emitter.emit(
                tool=TOOL,
                event=event,
                payload={k: cemit.dump(v) for k, v in payload.items()},
                tool_version=self._version,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # A task must not fail because we could not report on it.
            _LOG.warning("convalesce: could not emit %s: %s", event, exc)

    def close(self) -> None:
        """
        Flush anything queued.

        Flush rather than close: closing is not part of what a plugin needs
        from an emitter, and `Emitter.close` only flushes anyway.

        :return: nothing
        """
        if self._emitter is not None:
            self._emitter.flush()


def read_specs() -> Dict[str, Tuple[str, ...]]:
    """
    Read what this Airflow passes each hook.

    :return: hook name to its parameter names, for the wanted hooks only
    """
    found: Dict[str, Tuple[str, ...]] = {}
    for module_name in _SPEC_MODULES:
        try:
            module = importlib.import_module(module_name)
        except Exception:  # pylint: disable=broad-exception-caught
            # An Airflow that moved or dropped a spec module; the others may
            # still be readable.
            continue
        for name, fn in vars(module).items():
            if name in WANTED and callable(fn):
                found[name] = tuple(inspect.signature(fn).parameters)
    return found or dict(FALLBACK_SPECS)


def make_hook(event: str, argnames: Sequence[str]) -> Callable[..., None]:
    """
    Build a hook whose parameters match Airflow's hookspec exactly.

    Generated rather than written because pluggy reads a hook's parameters
    from its code object: a `**kwargs` catch-all receives nothing, so a
    hand-written signature silently drops whatever Airflow adds.

    :param event: the event name to forward under
    :param argnames: the parameters Airflow will pass
    :return: the hook implementation
    """
    # No defaults: pluggy treats a parameter with one as optional and leaves
    # it out of `argnames`, so a fully-defaulted signature receives nothing
    # and every field arrives as None.
    params = ", ".join(argnames)
    forwards = ", ".join(f"{name}={name}" for name in argnames)
    source = f"def _hook(self, {params}):\n    self.send(event, {forwards})\n"
    namespace: Dict[str, Any] = {"event": event}
    # The only way to give a function a signature pluggy will accept; the
    # source is built from Airflow's own parameter names, never from input.
    exec(source, namespace)  # nosec B102 # pylint: disable=exec-used
    hook: Callable[..., None] = namespace["_hook"]
    hook.__name__ = event
    hook.__doc__ = f"Forward Airflow's {event} payload."
    return hook


def build_listener_class(
    specs: Optional[Mapping[str, Sequence[str]]] = None,
) -> Type[_Base]:
    """
    Build a listener carrying only the hooks this Airflow specifies.

    :param specs: hook name to parameter names; read from Airflow when not
        given
    :return: the listener class
    """
    resolved = specs if specs is not None else read_specs()
    attached: Dict[str, Any] = {
        name: hookimpl(make_hook(name, argnames))
        for name, argnames in resolved.items()
    }
    return type("ConvalesceListener", (_Base,), attached)


@functools.lru_cache(maxsize=1)
def get_listener() -> Optional[_Base]:
    """
    The process-wide listener.

    Built on first use rather than at import: reading Airflow at import time
    is what made this module and Airflow's plugin loader import each other.

    :return: the listener, or None if one could not be built
    """
    try:
        return build_listener_class()()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # Airflow must start even if we cannot.
        _LOG.warning("convalesce: listener unavailable: %s", exc)
        return None

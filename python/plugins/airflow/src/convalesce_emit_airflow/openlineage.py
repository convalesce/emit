"""
Forward the OpenLineage events Airflow's own provider builds.

The OpenLineage provider (`apache-airflow-providers-openlineage`) runs each
operator's extractor inside the task: it parses the operator's SQL, asks the
hook which tables it touched, and reports columns, row counts and job
hierarchy that nothing downstream can recompute from the listener payloads
alone. This module is an OpenLineage transport: the provider hands it each
`RunEvent` it would otherwise post to a lineage backend, and it goes to
Convalesce as observation `openlineage`, payload `{"run_event": ...}`,
through the same emitter the listener uses.

How the transport gets switched on, found by reading provider 2.0.0 on
Airflow 2.10.5 rather than by assuming:

- The provider disables itself entirely -- no listener, no events -- unless
  one of `[openlineage] transport`, `[openlineage] config_path`,
  `OPENLINEAGE_CONFIG` or `OPENLINEAGE_URL` is set, and decides this once,
  in its plugin's class body, through `functools.cache`d readers.
- Its adapter builds the OpenLineage client from `[openlineage] transport`,
  and the client resolves a `type` it does not know as a class path.

So `enable()` sets `AIRFLOW__OPENLINEAGE__TRANSPORT` to this module's
transport. An environment variable because Airflow's configuration reads
those on every lookup, in every Airflow release with the provider, and
every process the scheduler or worker starts inherits it. It then clears
the provider's cached configuration readers, in case something read them
before this plugin loaded, and hands back the provider's own listener for
this plugin to register as well. Both plugins are `airflow.plugins` entry
points, loaded in whatever order the installed distributions list them; if
the provider's loaded first it has already decided it is disabled and
registered nothing. Airflow's listener manager skips a listener that is
already registered, and the provider's listener is a process-wide
singleton, so registering it from both plugins registers it once.

Never over a user's own setup: any OpenLineage configuration at all, or
OpenLineage switched off, leaves everything as it was. Nothing happens
where the provider is not installed, where `CONVALESCE_ENABLED=false`, or
where `CONVALESCE_OPENLINEAGE=false`.

Import as:

import convalesce_emit_airflow.openlineage as cealol
"""

import importlib
import importlib.util
import json
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit_airflow.listener as cealist

_LOG = logging.getLogger(__name__)

EVENT = "openlineage"
TRANSPORT_TYPE = f"{__name__}.ConvalesceTransport"
OPT_OUT_ENV = "CONVALESCE_OPENLINEAGE"
TRANSPORT_ENV = "AIRFLOW__OPENLINEAGE__TRANSPORT"

_PROVIDER = "airflow.providers.openlineage"
PROVIDER_CONF = "airflow.providers.openlineage.conf"
_PROVIDER_LISTENER = "airflow.providers.openlineage.plugins.listener"
_SECTION = "openlineage"
_FALSY = frozenset({"0", "false", "no", "off"})
_TRUTHY = frozenset({"1", "true", "t", "yes", "on"})

try:
    from openlineage.client.transport import Config, Transport
except Exception:  # pylint: disable=broad-exception-caught
    # Stand-ins purely so this module imports where OpenLineage is absent,
    # such as in tests and linting. The client only ever instantiates this
    # transport where the real classes exist.

    class Config:  # type: ignore[no-redef]
        """Stand-in for OpenLineage's transport config base."""

        @classmethod
        def from_dict(cls, params: Dict[str, Any]) -> Any:
            """Build the config; there is nothing to read."""
            del params
            return cls()

    class Transport:  # type: ignore[no-redef]
        """Stand-in for OpenLineage's transport base."""

        kind: Optional[str] = None
        config_class: Any = Config


# #############################################################################
# ConvalesceConfig
# #############################################################################


class ConvalesceConfig(Config):  # type: ignore[misc]
    """
    Configuration for `ConvalesceTransport`: none.

    Where to send and with which key are the emitter's, read from the
    `CONVALESCE_*` environment like every other observation.
    """

    @classmethod
    def from_dict(cls, params: Dict[str, Any]) -> "ConvalesceConfig":
        """
        Build the config from the transport's settings, which it ignores.

        :param params: the `transport` mapping, `type` included
        :return: the config
        """
        del params
        return cls()


# #############################################################################
# ConvalesceTransport
# #############################################################################


class ConvalesceTransport(Transport):  # type: ignore[misc]
    """
    OpenLineage transport forwarding each event as an observation.

    :param config: this transport's config, which carries nothing
    :param emitter: emitter to send through; the listener's when not given
    """

    kind = "convalesce"
    config_class = ConvalesceConfig

    def __init__(
        self,
        config: Optional[ConvalesceConfig] = None,
        emitter: Optional[cemit.EmitterLike] = None,
    ) -> None:
        self.config = config
        self._emitter = emitter
        self._version = cemit.version_of("airflow")

    @property
    def emitter(self) -> Optional[cemit.EmitterLike]:
        """
        The emitter to send through, shared with the listener.

        :return: the emitter, or None if one could not be built
        """
        if self._emitter is not None:
            return self._emitter
        listener = cealist.get_listener()
        return listener.emitter if listener is not None else None

    def emit(self, event: Any) -> None:
        """
        Forward one OpenLineage event.

        Flushed before returning: the provider emits task events from a
        process it forks and ends with `os._exit`, so nothing still queued
        would survive the task.

        :param event: the event the provider built, a `RunEvent` usually
        :return: nothing
        """
        emitter = self.emitter
        if emitter is None:
            return
        try:
            payload, excluded = shape(event)
            emitter.emit(
                tool="airflow",
                event=EVENT,
                payload=payload,
                tool_version=self._version,
                excluded=excluded,
            )
            emitter.flush()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # The provider already swallows a transport's failure, but a
            # task must not fail on our account whatever the caller does.
            _LOG.warning("convalesce: could not emit %s: %s", EVENT, exc)


def shape(event: Any) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """
    The observation payload for one OpenLineage event.

    :param event: the event the provider built
    :return: `{"run_event": ...}` with credentials redacted, and everything
        left out of it, by path and reason
    """
    budget = cemit.new_budget()
    dumped = cemit.dump(to_dict(event), budget=budget, path="run_event")
    redacted, secrets = cemit.redact_secrets(dumped, path="run_event")
    return {"run_event": redacted}, budget.excluded + secrets


def to_dict(event: Any) -> Any:
    """
    An OpenLineage event as the spec's JSON, via OpenLineage's own serde.

    :param event: the event the provider built
    :return: the event as JSON-shaped data; the event itself when the
        OpenLineage client is not importable, for `dump()` to walk
    """
    try:
        # pylint: disable=import-outside-toplevel
        from openlineage.client.serde import Serde
    except Exception:  # pylint: disable=broad-exception-caught
        return event
    return Serde.to_dict(event)


# #############################################################################
# enabling
# #############################################################################


def enable(
    environ: Optional[Dict[str, str]] = None,
    airflow_conf: Optional[Callable[[str], Optional[str]]] = None,
) -> List[Any]:
    """
    Point the provider at this transport, unless someone already chose.

    :param environ: the environment to read and write; the process's own
        when not given
    :param airflow_conf: reads one `[openlineage]` option, None when unset;
        Airflow's own configuration when not given
    :return: listeners for this plugin to register: the provider's own,
        when this enabled it, else none
    """
    env = os.environ if environ is None else environ
    read = airflow_conf or _airflow_option
    try:
        reason = skip_reason(env, read)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # Airflow must start even if we cannot read its configuration.
        _LOG.warning("convalesce: not enabling OpenLineage: %s", exc)
        return []
    if reason:
        _LOG.debug("convalesce: not enabling OpenLineage: %s", reason)
        return []
    env[TRANSPORT_ENV] = json.dumps({"type": TRANSPORT_TYPE})
    _clear_provider_cache()
    listener = _provider_listener()
    return [listener] if listener is not None else []


def skip_reason(
    env: Mapping[str, str], read: Callable[[str], Optional[str]]
) -> Optional[str]:
    """
    Why the transport must not be enabled here, if it must not.

    :param env: the environment
    :param read: reads one `[openlineage]` option, None when unset
    :return: the reason, or None when it should be enabled
    """
    if _flag(env, OPT_OUT_ENV) is False:
        return f"{OPT_OUT_ENV}=false"
    if _flag(env, "CONVALESCE_ENABLED") is False:
        return "CONVALESCE_ENABLED=false"
    if not provider_installed():
        return "provider not installed"
    configured = configured_by_user(env, read)
    if configured:
        return f"already configured by {configured}"
    return None


def configured_by_user(
    env: Mapping[str, str], read: Callable[[str], Optional[str]]
) -> Optional[str]:
    """
    The first sign the user set OpenLineage up, or switched it off.

    :param env: the environment
    :param read: reads one `[openlineage]` option, None when unset
    :return: what was found, or None when OpenLineage is untouched
    """
    transport = (read("transport") or "").strip()
    signs = [
        (bool(transport) and not _is_ours(transport), "[openlineage] transport"),
        (bool((read("config_path") or "").strip()), "[openlineage] config_path"),
        (_is_true(read("disabled")), "[openlineage] disabled"),
        (bool(env.get("OPENLINEAGE_URL", "").strip()), "OPENLINEAGE_URL"),
        (bool(env.get("OPENLINEAGE_CONFIG", "").strip()), "OPENLINEAGE_CONFIG"),
        (_is_true(env.get("OPENLINEAGE_DISABLED")), "OPENLINEAGE_DISABLED"),
        # The OpenLineage client's own per-key variables, which it reads
        # when it is built with no configuration handed to it.
        (
            any(name.startswith("OPENLINEAGE__TRANSPORT") for name in env),
            "OPENLINEAGE__TRANSPORT*",
        ),
    ]
    # Where the OpenLineage client looks for its own file when nothing names
    # one. The provider never reads either, but a user who wrote one meant
    # something by it.
    signs.extend(
        (os.path.isfile(path), path)
        for path in (
            os.path.join(os.getcwd(), "openlineage.yml"),
            os.path.join(
                os.path.expanduser("~/.openlineage"), "openlineage.yml"
            ),
        )
    )
    return next((what for found, what in signs if found), None)


def _is_true(value: Optional[str]) -> bool:
    """
    Whether a setting reads as true, the way the provider reads its own.

    :param value: the setting, None when unset
    :return: whether it is set to a true value
    """
    return (value or "").strip().lower() in _TRUTHY


def _is_ours(transport: str) -> bool:
    """
    Whether a configured transport is the one `enable()` sets.

    A process the scheduler or worker starts inherits the variable
    `enable()` set in its parent, and must still register the listener
    rather than mistake the setting for the user's.

    :param transport: the `[openlineage] transport` value, as JSON text
    :return: whether it names this module's transport
    """
    try:
        parsed = json.loads(transport)
    except ValueError:
        return False
    return isinstance(parsed, dict) and parsed.get("type") == TRANSPORT_TYPE


def provider_installed() -> bool:
    """
    Whether Airflow's OpenLineage provider is importable here.

    :return: whether it is
    """
    try:
        return importlib.util.find_spec(_PROVIDER) is not None
    except Exception:  # pylint: disable=broad-exception-caught
        # `find_spec` imports the parents, and an Airflow that is half
        # there raises rather than answering.
        return False


def _flag(env: Mapping[str, str], name: str) -> Optional[bool]:
    """
    Read a boolean variable the way the core configuration does.

    :param env: the environment
    :param name: the variable
    :return: False when it is set to a false value, True when set to
        anything else, None when unset
    """
    raw = env.get(name)
    if raw is None:
        return None
    return raw.strip().lower() not in _FALSY


def _airflow_option(option: str) -> Optional[str]:
    """
    Read one `[openlineage]` option from Airflow's configuration.

    :param option: the option name
    :return: its value, or None when unset or unreadable
    """
    try:
        # pylint: disable=import-outside-toplevel
        from airflow.configuration import conf

        value = conf.get(_SECTION, option, fallback=None)
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    return None if value is None else str(value)


def _clear_provider_cache() -> None:
    """
    Forget any configuration the provider already read and cached.

    :return: nothing
    """
    module = sys.modules.get(PROVIDER_CONF)
    if module is None:
        return
    for value in vars(module).values():
        clear = getattr(value, "cache_clear", None)
        if callable(clear):
            try:
                clear()
            except Exception:  # pylint: disable=broad-exception-caught
                continue


def _provider_listener() -> Any:
    """
    The provider's own process-wide listener.

    :return: the listener, or None if the provider could not build one
    """
    try:
        module = importlib.import_module(_PROVIDER_LISTENER)
        return module.get_openlineage_listener()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("convalesce: OpenLineage listener unavailable: %s", exc)
        return None

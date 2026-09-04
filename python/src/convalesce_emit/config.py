"""
Where to send observations, and how hard to try.

Read from the environment by default so a pipeline picks it up without
editing code: an operator sets the variables once on the worker, and every
DAG, flow or suite in that process emits without knowing this exists.

The ingest key is held as a plain string rather than a `pydantic.SecretStr`,
which is the house rule elsewhere. This package has no dependencies on
purpose -- it installs into a customer's Airflow and must not disturb a
resolution that already works -- so pydantic is not available to it. The key
is therefore never logged and never placed in the envelope; it appears only
in the Authorization header.

Import as:

import convalesce_emit.config as ceconfig
"""

import dataclasses
import logging
import os
from typing import Any, Optional

import convalesce_emit.errors as ceerrors

_LOG = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "https://api.convalesce.dev"
DEFAULT_TIMEOUT = 10.0
DEFAULT_MAX_RETRIES = 3
# Fifty keeps a busy scheduler to roughly one request a second while staying
# small enough that a crash loses little.
DEFAULT_BATCH_SIZE = 50

_TRUTHY = frozenset({"1", "true", "yes", "on"})


# #############################################################################
# Config
# #############################################################################


@dataclasses.dataclass(frozen=True)
class Config:
    """
    Everything the emitter needs to reach us.

    :param endpoint: base URL to post observations to
    :param ingest_key: write-only key a pipeline presents; never logged
    :param workspace: which account these observations belong to
    :param timeout: seconds to wait on a single request
    :param max_retries: attempts after the first, for transient failures
    :param batch_size: observations to hold before sending
    :param dry_run: build and log envelopes, send nothing
    :param enabled: when False, emitting is a no-op
    """

    endpoint: str = DEFAULT_ENDPOINT
    ingest_key: Optional[str] = None
    workspace: Optional[str] = None
    timeout: float = DEFAULT_TIMEOUT
    max_retries: int = DEFAULT_MAX_RETRIES
    batch_size: int = DEFAULT_BATCH_SIZE
    dry_run: bool = False
    enabled: bool = True

    @classmethod
    def from_env(cls, **overrides: Any) -> "Config":
        """
        Read configuration from the environment, then apply overrides.

        :param overrides: field values that win over the environment
        :return: a configuration, not yet validated
        :raises ConfigError: if a numeric variable is not a number
        """
        config = cls(
            endpoint=_read_str("CONVALESCE_ENDPOINT") or DEFAULT_ENDPOINT,
            ingest_key=_read_str("CONVALESCE_INGEST_KEY"),
            workspace=_read_str("CONVALESCE_WORKSPACE"),
            timeout=_read_num("CONVALESCE_TIMEOUT", DEFAULT_TIMEOUT),
            max_retries=int(
                _read_num("CONVALESCE_MAX_RETRIES", DEFAULT_MAX_RETRIES)
            ),
            batch_size=int(
                _read_num("CONVALESCE_BATCH_SIZE", DEFAULT_BATCH_SIZE)
            ),
            dry_run=_read_bool("CONVALESCE_DRY_RUN", False),
            enabled=_read_bool("CONVALESCE_ENABLED", True),
        )
        if overrides:
            config = dataclasses.replace(config, **overrides)
        return config

    def validate(self) -> None:
        """
        Check this configuration can actually send.

        :return: nothing
        :raises ConfigError: if the key is missing or the endpoint is not an
            http(s) URL
        """
        # Neither mode reaches the network, so neither needs a key.
        if not self.enabled or self.dry_run:
            return
        if not self.ingest_key:
            raise ceerrors.ConfigError(
                "No ingest key. Set CONVALESCE_INGEST_KEY, or set "
                "CONVALESCE_DRY_RUN=true to build envelopes without sending."
            )
        if not self.endpoint.startswith(("http://", "https://")):
            raise ceerrors.ConfigError(
                f"endpoint must be an http(s) URL, got {self.endpoint!r}"
            )


def _read_str(name: str) -> Optional[str]:
    """
    Read a string variable, trimmed.

    :param name: environment variable to read
    :return: its value, or None when unset
    """
    value = os.environ.get(name)
    return value.strip() if value is not None else None


def _read_bool(name: str, default: bool) -> bool:
    """
    Read a boolean variable.

    :param name: environment variable to read
    :param default: value when unset
    :return: whether the variable reads as true
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


def _read_num(name: str, default: float) -> float:
    """
    Read a numeric variable.

    :param name: environment variable to read
    :param default: value when unset
    :return: the number
    :raises ConfigError: if the value is not a number
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ceerrors.ConfigError(
            f"{name} must be a number, got {raw!r}"
        ) from exc

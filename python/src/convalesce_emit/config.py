"""
Where to send observations, and how hard to try.

Read from the environment by default so a pipeline picks it up without
editing code: an operator sets the variables once on the worker, and every
DAG, flow or suite in that process emits without knowing this exists.

There is no account or tenant setting, deliberately. The ingest key is what
identifies the caller, and it is the only thing that does. A separate setting
naming the account would be an unauthenticated claim sitting next to the
credential that actually proves it, and the two could disagree.

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
import tempfile
from typing import Any, Optional

import convalesce_emit.errors as ceerrors

_LOG = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "https://api.convalesce.io"
DEFAULT_TIMEOUT = 10.0
DEFAULT_MAX_RETRIES = 3
# Fifty keeps a busy scheduler to roughly one request a second while staying
# small enough that a crash loses little.
DEFAULT_BATCH_SIZE = 50
# The receiver takes at most fifty observations and five megabytes in one
# request and refuses the whole request past either, and a refusal is not
# retried, so a configured batch may not exceed them.
RECEIVER_MAX_OBSERVATIONS = 50
RECEIVER_MAX_BODY_BYTES = 5_000_000
# Well under the receiver's limit, so a batch is closed long before a request
# could be refused for its size.
DEFAULT_MAX_BODY_BYTES = 1_000_000
# Where undelivered batches wait to be sent again, and how much of the disk
# they may take before a new one is refused rather than filling it.
DEFAULT_SPOOL_DIR = os.path.join(tempfile.gettempdir(), "convalesce-emit-spool")
DEFAULT_SPOOL_MAX_BYTES = 1_000_000_000

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
    :param api_key: the api-scoped key the gate endpoint requires; a
        different key from `ingest_key`, never logged. Only the blocking
        operator (`convalesce_emit.gate`) reads this
    :param timeout: seconds to wait on a single request
    :param max_retries: attempts after the first, for transient failures
    :param batch_size: observations to hold before sending
    :param max_body_bytes: encoded size a batch is sent before reaching
    :param dry_run: build and log envelopes, send nothing
    :param enabled: when False, emitting is a no-op
    :param spool_dir: where batches that could not be delivered are kept;
        `CONVALESCE_SPOOL_DIR`, else a directory under the system temp dir,
        when not given
    :param spool_max_bytes: how large the waiting batches may grow on disk
    """

    endpoint: str = DEFAULT_ENDPOINT
    ingest_key: Optional[str] = None
    api_key: Optional[str] = None
    timeout: float = DEFAULT_TIMEOUT
    max_retries: int = DEFAULT_MAX_RETRIES
    batch_size: int = DEFAULT_BATCH_SIZE
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
    dry_run: bool = False
    enabled: bool = True
    spool_dir: Optional[str] = None
    spool_max_bytes: int = DEFAULT_SPOOL_MAX_BYTES

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
            api_key=_read_str("CONVALESCE_API_KEY"),
            timeout=_read_num("CONVALESCE_TIMEOUT", DEFAULT_TIMEOUT),
            max_retries=int(
                _read_num("CONVALESCE_MAX_RETRIES", DEFAULT_MAX_RETRIES)
            ),
            batch_size=int(
                _read_num("CONVALESCE_BATCH_SIZE", DEFAULT_BATCH_SIZE)
            ),
            max_body_bytes=int(
                _read_num("CONVALESCE_MAX_BODY_BYTES", DEFAULT_MAX_BODY_BYTES)
            ),
            dry_run=_read_bool("CONVALESCE_DRY_RUN", False),
            enabled=_read_bool("CONVALESCE_ENABLED", True),
            spool_max_bytes=int(
                _read_num("CONVALESCE_SPOOL_MAX_BYTES", DEFAULT_SPOOL_MAX_BYTES)
            ),
        )
        if overrides:
            config = dataclasses.replace(config, **overrides)
        return config

    def spool_directory(self) -> str:
        """
        Where undelivered batches are kept.

        Read when asked rather than when built, so a `Config()` made in code
        still honours the variable an operator set.

        :return: the directory
        """
        return (
            self.spool_dir
            or _read_str("CONVALESCE_SPOOL_DIR")
            or DEFAULT_SPOOL_DIR
        )

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
        if not 0 < self.batch_size <= RECEIVER_MAX_OBSERVATIONS:
            raise ceerrors.ConfigError(
                f"CONVALESCE_BATCH_SIZE must be 1 to {RECEIVER_MAX_OBSERVATIONS},"
                f" the receiver's limit, got {self.batch_size}"
            )
        if not 0 < self.max_body_bytes <= RECEIVER_MAX_BODY_BYTES:
            raise ceerrors.ConfigError(
                f"CONVALESCE_MAX_BODY_BYTES must be 1 to {RECEIVER_MAX_BODY_BYTES},"
                f" the receiver's limit, got {self.max_body_bytes}"
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

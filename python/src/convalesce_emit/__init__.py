"""
Send a tool's own output to Convalesce, unchanged.

This package connects and forwards. It does not parse, map or resolve
anything: whatever the tool handed us goes across as-is, and every bit of
interpretation happens after it arrives. That is the point -- improving how a
payload is understood never requires anyone to upgrade this package.

The names below are re-exported for callers embedding the emitter directly.
Within the package, modules import each other by alias, never by name.

Import as:

import convalesce_emit as cemit
"""

import logging

from convalesce_emit._version import __version__
from convalesce_emit.client import Emitter, send_one
from convalesce_emit.config import Config
from convalesce_emit.envelope import ENVELOPE_VERSION, Observation, build
from convalesce_emit.errors import (
    ConfigError,
    EmitError,
    TransportError,
    error_detail,
)
from convalesce_emit.protocols import EmitterLike
from convalesce_emit.redact import redact_samples, redact_secrets
from convalesce_emit.serialize import DEFAULT_SKIP, Budget, dump, new_budget
from convalesce_emit.toolinfo import version_of

_LOG = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_SKIP",
    "ENVELOPE_VERSION",
    "Budget",
    "Config",
    "ConfigError",
    "EmitError",
    "Emitter",
    "EmitterLike",
    "Observation",
    "TransportError",
    "__version__",
    "build",
    "dump",
    "error_detail",
    "new_budget",
    "redact_samples",
    "redact_secrets",
    "send_one",
    "version_of",
]

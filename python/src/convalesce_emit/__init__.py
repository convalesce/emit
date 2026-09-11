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
from convalesce_emit.errors import ConfigError, EmitError, TransportError
from convalesce_emit.protocols import EmitterLike
from convalesce_emit.redact import redact_samples
from convalesce_emit.serialize import dump
from convalesce_emit.toolinfo import version_of

_LOG = logging.getLogger(__name__)

__all__ = [
    "ENVELOPE_VERSION",
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
    "redact_samples",
    "send_one",
    "version_of",
]

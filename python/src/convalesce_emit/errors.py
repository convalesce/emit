"""
Errors this package raises.

Every one of them is caught before it reaches a customer's pipeline; they
exist so the transport can tell a refusal apart from an outage, and so a
caller embedding the emitter directly can still see what went wrong.

Import as:

import convalesce_emit.errors as ceerrors
"""

import logging
from typing import Optional

_LOG = logging.getLogger(__name__)


# #############################################################################
# EmitError
# #############################################################################


class EmitError(Exception):
    """Base for every error this package raises."""


# #############################################################################
# ConfigError
# #############################################################################


class ConfigError(EmitError):
    """Configuration is missing or unusable."""


# #############################################################################
# TransportError
# #############################################################################


class TransportError(EmitError):
    """
    An observation could not be delivered.

    :param message: what went wrong, and where the reader can act, what to
        do about it
    :param status: the HTTP status, where there was one; None when the
        endpoint could not be reached at all
    """

    def __init__(self, message: str, *, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status

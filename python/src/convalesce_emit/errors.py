"""
Errors this package raises, and how a tool's own errors are described.

Every error raised here is caught before it reaches a customer's pipeline;
they exist so the transport can tell a refusal apart from an outage, and so a
caller embedding the emitter directly can still see what went wrong.

A tool's exception is another matter. `serialize.dump()` turns one into its
message, because an exception object is mostly runtime plumbing, and the
message alone drops the two things a receiver cannot recompute from anything
else it holds: which class was raised and where. `error_detail()` carries
those, in one shape every plugin shares.

Import as:

import convalesce_emit.errors as ceerrors
"""

import logging
import traceback
from typing import Any, Dict, Optional

_LOG = logging.getLogger(__name__)

# Characters of formatted traceback kept per exception. The tail is kept: the
# frame that raised is at the bottom, and a deep stack loses its outermost,
# least specific frames first.
TRACEBACK_LIMIT = 16_000


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


# #############################################################################
# error_detail
# #############################################################################


def error_detail(exc: BaseException) -> Dict[str, Any]:
    """
    Describe a tool's exception: its class, message, traceback and cause.

    The cause is described one level deep and no further, without its own
    cause: the exception the tool reported and the one directly behind it
    are what a reader acts on, and a longer chain is still in the task log.

    :param exc: the exception the tool handed its callback
    :return: `type` as `<module>.<Class>`, `message`, `traceback` as text
        (its last `TRACEBACK_LIMIT` characters), and `cause` when the
        exception was raised from, or while handling, another one
    """
    detail = _describe_one(exc)
    cause = _cause_of(exc)
    if cause is not None:
        detail["cause"] = _describe_one(cause)
    return detail


def _describe_one(exc: BaseException) -> Dict[str, Any]:
    """
    Describe one exception, without following its chain.

    :param exc: the exception to describe
    :return: its type, message and traceback
    """
    kind = type(exc)
    return {
        "type": f"{kind.__module__}.{kind.__qualname__}",
        "message": _message(exc),
        "traceback": _traceback(exc),
    }


def _cause_of(exc: BaseException) -> Optional[BaseException]:
    """
    The exception directly behind this one, as Python itself would show it.

    :param exc: the exception whose chain to read
    :return: the explicit cause, else the implicit context unless it was
        suppressed with `from None`, else None
    """
    if exc.__cause__ is not None:
        return exc.__cause__
    if exc.__suppress_context__:
        return None
    return exc.__context__


def _message(exc: BaseException) -> str:
    """
    An exception's message, even when its `__str__` raises.

    :param exc: the exception
    :return: its message, or its type name when that could not be read
    """
    try:
        return str(exc)
    except Exception:  # pylint: disable=broad-exception-caught
        # The exception is already being reported on; its own rendering
        # failing must not lose the rest of the description.
        return f"<{type(exc).__name__}>"


def _traceback(exc: BaseException) -> str:
    """
    An exception's formatted traceback, capped from the front.

    :param exc: the exception
    :return: the traceback text as Python prints it, chain excluded (the
        cause is described separately), at most `TRACEBACK_LIMIT` long
    """
    try:
        text = "".join(
            traceback.format_exception(
                type(exc), exc, exc.__traceback__, chain=False
            )
        )
    except Exception:  # pylint: disable=broad-exception-caught
        # Formatting reads the exception's own `__str__` and the source
        # files; either can fail, and the type and message still stand.
        return ""
    if len(text) > TRACEBACK_LIMIT:
        return text[-TRACEBACK_LIMIT:]
    return text

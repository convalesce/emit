"""
What a plugin needs from an emitter.

A plugin is defined by what it sends, not by which class it sends through,
so it takes this rather than the concrete `Emitter`. That keeps a test's
recorder a first-class substitute instead of something the type checker has
to be talked out of, and leaves room for a caller to supply their own.

Import as:

import convalesce_emit.protocols as ceproto
"""

import logging
from typing import Any, Optional, Protocol

_LOG = logging.getLogger(__name__)


# #############################################################################
# EmitterLike
# #############################################################################


class EmitterLike(Protocol):
    """Anything that can accept an observation and send it."""

    def emit(
        self,
        *,
        tool: str,
        event: str,
        payload: Any,
        tool_version: Optional[str] = None,
    ) -> None:
        """
        Queue one observation.

        :param tool: which tool produced this
        :param event: which callback fired
        :param payload: the tool's own output, untouched
        :param tool_version: the tool's version, where it could be read
        :return: nothing
        """

    def flush(self) -> None:
        """
        Send whatever is queued.

        :return: nothing
        """

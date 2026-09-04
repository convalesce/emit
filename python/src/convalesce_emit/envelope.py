"""
The wrapper an observation travels in.

The payload inside is the tool's own output and crosses untouched. Every
other field exists so the receiver knows what it is holding: which tool
produced it, which callback fired, and when. Nothing here parses or reshapes
the payload, which is what lets us improve how it is understood without a
customer upgrading anything.

Import as:

import convalesce_emit.envelope as ceenvelo
"""

import dataclasses
import datetime
import logging
import uuid
from typing import Any, Dict, Optional

import convalesce_emit._version as ceversio

_LOG = logging.getLogger(__name__)

# Bumped only when the envelope's own shape changes. The payload inside is
# versioned by `tool_version`, not by this.
ENVELOPE_VERSION = 1


def _now() -> str:
    """
    Timestamp this observation.

    :return: the current UTC time, ISO 8601
    """
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat()


def _new_id() -> str:
    """
    Identify this observation.

    :return: a fresh UUID, so a redelivered batch can be deduplicated
    """
    return str(uuid.uuid4())


# #############################################################################
# Observation
# #############################################################################


@dataclasses.dataclass(frozen=True)
class Observation:
    """
    One thing a pipeline tells us, wrapped for transport.

    :param tool: which tool produced this, such as "airflow"
    :param event: which callback fired, such as "task_instance_failed"
    :param payload: the tool's own output, untouched
    :param tool_version: the tool's version, where it could be read
    :param workspace: which account this belongs to
    :param envelope_version: shape of this wrapper, not of the payload
    :param client_version: which release of this package sent it
    :param observation_id: unique per observation
    :param emitted_at: when this was built, not when it was sent
    """

    tool: str
    event: str
    payload: Any
    tool_version: Optional[str] = None
    workspace: Optional[str] = None
    envelope_version: int = ENVELOPE_VERSION
    client_version: str = ceversio.__version__
    observation_id: str = dataclasses.field(default_factory=_new_id)
    emitted_at: str = dataclasses.field(default_factory=_now)

    def to_dict(self) -> Dict[str, Any]:
        """
        Flatten for JSON encoding.

        :return: this observation as a plain dict
        """
        return dataclasses.asdict(self)


def build(
    *,
    tool: str,
    event: str,
    payload: Any,
    tool_version: Optional[str] = None,
    workspace: Optional[str] = None,
) -> Observation:
    """
    Wrap a tool's payload for transport.

    :param tool: which tool produced this
    :param event: which callback fired
    :param payload: the tool's own output, untouched
    :param tool_version: the tool's version, where it could be read
    :param workspace: which account this belongs to
    :return: the observation to send
    """
    return Observation(
        tool=tool,
        event=event,
        payload=payload,
        tool_version=tool_version,
        workspace=workspace,
    )

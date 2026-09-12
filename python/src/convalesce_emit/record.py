"""
Recording: telling us the outcome of an operation or an assertion, explicitly.

The author of a pipeline already knows things this package cannot read on
its own: that a load finished, that a check passed or failed. This is the
one operator that lets them say so directly, rather than emit inferring it
from a tool's own callback.

Same clean-room discipline as every other plugin here: this builds no urns
and reads nothing beyond what the caller hands it. `dataset` and `outcome`
cross exactly as given; naming the dataset, resolving it to a urn, and
deciding what an outcome means all happen in collect's translators.

Import as:

import convalesce_emit.record as cerecord
"""

import logging
from typing import Any, Dict, Optional

import convalesce_emit.client as ceclient
import convalesce_emit.protocols as ceproto

_LOG = logging.getLogger(__name__)

# The two envelope `tool` values this operator can post under. Each gets its
# own translator in collect, the same way each of the five tool plugins does;
# there is no tool-state to read here, so `event` is the same single value
# ("reported") for both -- see the module docstring's clean-room note.
OPERATION = "operation"
ASSERTION = "assertion"
_VALID_EVENTS = frozenset({OPERATION, ASSERTION})
_REPORTED_EVENT = "reported"


def record(
    *,
    dataset: str,
    event: str,
    outcome: str,
    details: Optional[Dict[str, Any]] = None,
    emitter: Optional[ceproto.EmitterLike] = None,
) -> None:
    """
    Record an operation's or an assertion's outcome against a dataset.

    :param dataset: the dataset's identity, exactly as the caller names it;
        never parsed, never turned into a urn here
    :param event: which kind of outcome this is: `OPERATION` ("operation")
        or `ASSERTION` ("assertion")
    :param outcome: the result, exactly as the caller states it (e.g.
        "SUCCESS", "FAILURE"); never interpreted here
    :param details: anything else the caller wants attached -- forwarded
        verbatim, read by nothing in this package
    :param emitter: emitter to send through; built from the environment when
        not given
    :return: nothing
    :raises ValueError: if `event` is not `OPERATION` or `ASSERTION`. This is
        the one check this function does not swallow: an unrecognized event
        name is a caller mistake made before anything is sent, not a
        transport failure, and the two are not the same kind of problem the
        rest of this package exists to protect a pipeline from
    """
    if event not in _VALID_EVENTS:
        raise ValueError(
            f"event must be one of {sorted(_VALID_EVENTS)}, got {event!r}"
        )
    payload: Dict[str, Any] = {"dataset": dataset, "outcome": outcome}
    if details:
        payload["details"] = dict(details)
    ceclient.send_one(
        tool=event, event=_REPORTED_EVENT, payload=payload, emitter=emitter
    )

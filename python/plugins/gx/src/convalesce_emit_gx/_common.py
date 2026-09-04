"""
What both Great Expectations actions do once they have a payload.

Import as:

import convalesce_emit_gx._common as cegxcom
"""

import logging
import os
from typing import Any, Dict, Optional

import convalesce_emit as cemit

_LOG = logging.getLogger(__name__)

TOOL = "great_expectations"

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def send_samples() -> bool:
    """
    Whether the operator has opted into sending row values.

    :return: True when `CONVALESCE_GX_SEND_SAMPLES` is set truthy
    """
    raw = os.environ.get("CONVALESCE_GX_SEND_SAMPLES", "")
    return raw.strip().lower() in _TRUTHY


def forward(
    payload: Any, emitter: Optional[cemit.EmitterLike] = None
) -> Dict[str, Any]:
    """
    Redact and send one validation result.

    Redaction is on unless deliberately turned off: a GX result carries
    sample failing values, which are real rows from the customer's table,
    and the handbook promises we read "table shapes, run outcomes, row
    counts, lineage. Not the rows themselves."

    :param payload: whatever GX handed the action
    :param emitter: emitter to send through; built from the environment when
        not given
    :return: whether the observation was emitted, and whether it was redacted
    """
    redact = not send_samples()
    body = cemit.dump(payload)
    if redact:
        body = cemit.redact_samples(body)
    try:
        target = emitter or cemit.Emitter()
        target.emit(
            tool=TOOL,
            event="validation_result",
            payload=body,
            tool_version=cemit.version_of("great_expectations"),
        )
        target.flush()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A checkpoint must not fail because we could not report on it.
        _LOG.warning("convalesce: could not emit validation result: %s", exc)
        return {"convalesce_emitted": False, "error": str(exc)}
    return {"convalesce_emitted": True, "redacted": redact}

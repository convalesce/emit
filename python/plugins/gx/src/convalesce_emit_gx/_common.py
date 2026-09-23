"""
What both Great Expectations actions do once they have a payload.

Import as:

import convalesce_emit_gx._common as cegxcom
"""

import logging
import os
from typing import Any, Dict, List, Optional

import convalesce_emit as cemit

_LOG = logging.getLogger(__name__)

TOOL = "great_expectations"

_TRUTHY = frozenset({"1", "true", "yes", "on"})

# Arguments that are the Great Expectations runtime rather than the result of
# a validation. `data_asset` on 0.x is a live Validator, and through its data
# context it owns every store's configuration, the execution engine, the
# batch cache and the frame that was validated: about 10 KB of an 11 KB
# observation, none of it read by a receiver, and the shortest path to the
# customer's rows. What is worth having from the batch is already on
# `validation_result_suite.meta` as `batch_spec`, `batch_markers` and
# `active_batch_definition`, which is where the 1.x path reads it from too.
_SKIP_ARGS = frozenset({"data_asset"})


def send_samples() -> bool:
    """
    Whether the operator has opted into sending row values.

    :return: True when `CONVALESCE_GX_SEND_SAMPLES` is set truthy
    """
    raw = os.environ.get("CONVALESCE_GX_SEND_SAMPLES", "")
    return raw.strip().lower() in _TRUTHY


def is_runtime(value: Any) -> bool:
    """
    Tell the Great Expectations runtime from a validation result.

    Named for what it owns rather than by class, so a validator, a data
    context or a checkpoint arriving positionally is recognised on both
    majors without importing any of them.

    :param value: one argument the action was handed
    :return: whether it is the runtime rather than a result
    """
    return any(
        hasattr(value, name)
        for name in ("execution_engine", "data_context", "_data_context")
    )


def shape(payload: Any) -> Any:
    """
    Drop the runtime from what the action was handed.

    Only the shape both actions build, `args` and `kwargs`, is reshaped;
    anything else is a payload this does not recognise and crosses as it is.

    :param payload: the action's own arguments
    :return: the same, minus the runtime
    """
    if not isinstance(payload, dict):
        return payload
    if "args" not in payload and "kwargs" not in payload:
        return payload
    args: List[Any] = [
        value for value in payload.get("args") or [] if not is_runtime(value)
    ]
    kwargs = {
        name: value
        for name, value in (payload.get("kwargs") or {}).items()
        if name not in _SKIP_ARGS and not is_runtime(value)
    }
    return {"args": args, "kwargs": kwargs}


def runtime_platform(payload: Any) -> Dict[str, str]:
    """
    What the execution engine says about the platform, read before the
    runtime that carries it is left behind.

    A receiver has only the datasource's *name* to go on otherwise
    (`meta.active_batch_definition.datasource_name`), and falls back to
    using that as the platform, which produces a urn like
    `urn:li:dataset:(urn:li:dataPlatform:orders,orders,PROD)` for a
    datasource named `orders`. On 0.x the engine's own dialect is the real
    platform -- `engine.dialect.name`, never the engine's URL, so no
    credential travels.

    :param payload: the action's own arguments, before `shape()` reshapes
        them
    :return: `execution_engine_class` and, where the engine is backed by a
        live SQLAlchemy engine, `dialect_name`; empty when nothing runtime
        was handed to this action, or it named no engine
    """
    if not isinstance(payload, dict):
        return {}
    candidates: List[Any] = list(payload.get("args") or [])
    candidates.extend((payload.get("kwargs") or {}).values())
    for value in candidates:
        if not is_runtime(value):
            continue
        engine = getattr(value, "execution_engine", None)
        if engine is None:
            continue
        out: Dict[str, str] = {"execution_engine_class": type(engine).__name__}
        sa_engine = getattr(engine, "engine", None)
        dialect = getattr(sa_engine, "dialect", None)
        name = getattr(dialect, "name", None)
        if name:
            out["dialect_name"] = str(name)
        return out
    return {}


def forward(
    payload: Any, emitter: Optional[cemit.EmitterLike] = None
) -> Dict[str, Any]:
    """
    Redact and send one validation result.

    Redaction is on unless deliberately turned off: a GX result carries
    sample failing values, which are real rows from the customer's table,
    and the handbook promises we read "table shapes, run outcomes, row
    counts, lineage. Not the rows themselves."

    The Great Expectations runtime is left behind for the same reason and
    one more: a validator owns the frame it validated, and no receiver reads
    a word of the data context hanging off it.

    :param payload: whatever GX handed the action
    :param emitter: emitter to send through; built from the environment when
        not given
    :return: whether the observation was emitted, and whether it was redacted
    """
    redact = not send_samples()
    try:
        # Inside the guard, not before it: GX fails the whole checkpoint when
        # an action raises.
        platform = runtime_platform(payload)
        budget = cemit.new_budget()
        body = cemit.dump(shape(payload), budget=budget)
        if platform and isinstance(body, dict):
            body.update(platform)
        excluded = budget.excluded
        if redact:
            body, redacted = cemit.redact_samples(body)
            excluded = excluded + redacted
        target = emitter or cemit.Emitter()
        target.emit(
            tool=TOOL,
            event="validation_result",
            payload=body,
            tool_version=cemit.version_of("great_expectations"),
            excluded=excluded,
        )
        target.flush()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A checkpoint must not fail because we could not report on it.
        _LOG.warning("convalesce: could not emit validation result: %s", exc)
        return {"convalesce_emitted": False, "error": str(exc)}
    return {"convalesce_emitted": True, "redacted": redact}

"""
Validation action for Great Expectations Core 1.x.

In 1.x an action is a pydantic model discriminated on `type`, and GX calls
`run(checkpoint_result, action_context)`. `run` takes `*args` because we do
not read the result: a signature change within 1.x cannot break us, only a
rename of the method itself.

Import as:

import convalesce_emit_gx.action_v1 as cegxv1
"""

import logging
from typing import Any, Dict, Optional

from great_expectations.checkpoint.actions import ValidationAction

import convalesce_emit as cemit
import convalesce_emit_gx._common as cegxcom

_LOG = logging.getLogger(__name__)

# #############################################################################
# ConvalesceValidationAction
# #############################################################################


class ConvalesceValidationAction(ValidationAction):  # type: ignore[misc]
    """Forwards each validation result to Convalesce, on GX 1.x."""

    # GX discriminates actions on `type`, and its base requires a `name`;
    # both are defaulted so a checkpoint can list the action without
    # repeating them.
    type: str = "convalesce_emit"
    name: str = "convalesce_emit"

    class Config:
        """Let a non-pydantic emitter be held on the model."""

        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def run(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Forward whatever GX passed, without reading it.

        :param args: GX passes `checkpoint_result` and `action_context`
        :param kwargs: the same, where GX passes them by keyword
        :return: whether the observation was emitted, and whether it was
            redacted
        """
        payload = {"args": list(args), "kwargs": kwargs}
        return cegxcom.forward(payload, getattr(self, "_emitter", None))

    def set_emitter(self, emitter: Optional[cemit.EmitterLike]) -> None:
        """
        Point this action at a specific emitter.

        GX builds the action itself, so there is no constructor to pass one
        to; tests and embedders use this instead.

        :param emitter: what to send through
        :return: nothing
        """
        object.__setattr__(self, "_emitter", emitter)

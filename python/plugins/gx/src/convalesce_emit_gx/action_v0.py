"""
Validation action for Great Expectations 0.17 and 0.18.

The 0.x base is a plain class taking `(data_context, name)` and dispatching
to `_run`, where 1.x is a pydantic model taking `run(checkpoint_result,
action_context)`. Nothing satisfies both, which is why there are two
classes rather than one with a branch inside it.

`_run` takes `*args` because we do not read the result. GX 0.x passes six
positional arguments and has changed which ones across point releases; none
of that matters to a forwarder.

Import as:

import convalesce_emit_gx.action_v0 as cegxv0
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
    """Forwards each validation result to Convalesce, on GX 0.x."""

    def __init__(
        self,
        data_context: Any = None,
        name: str = "convalesce_emit",
        emitter: Optional[cemit.EmitterLike] = None,
        **kwargs: Any,
    ) -> None:
        # Swallowed rather than forwarded: GX 0.x point releases pass extra
        # keywords to some actions, and rejecting them would refuse to build.
        del kwargs
        # 0.18.14 added `name` to the base; earlier releases take only the
        # context, so the base is called with whatever it will accept.
        try:
            super().__init__(data_context=data_context, name=name)
        except TypeError:
            super().__init__(data_context=data_context)  # type: ignore[call-arg]
        self._emitter = emitter

    def _run(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Forward whatever GX passed, without reading it.

        :param args: the positional arguments GX 0.x passes
        :param kwargs: the keyword arguments GX 0.x passes
        :return: whether the observation was emitted, and whether it was
            redacted
        """
        payload = {"args": list(args), "kwargs": kwargs}
        return cegxcom.forward(payload, self._emitter)

    def set_emitter(self, emitter: Optional[cemit.EmitterLike]) -> None:
        """
        Point this action at a specific emitter.

        :param emitter: what to send through
        :return: nothing
        """
        self._emitter = emitter

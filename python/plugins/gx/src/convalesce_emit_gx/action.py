"""
Great Expectations action that forwards a validation result.

Great Expectations rebuilt its action API between 0.x and 1.x: 0.x is a
plain class taking `(data_context, name)` that dispatches to `_run` with six
positional arguments, 1.x is a pydantic model discriminated on `type` whose
`run` takes `(checkpoint_result, action_context)`. One class cannot be both,
so there are two, and this module picks the one matching the installed GX.

A checkpoint therefore lists `convalesce_emit_gx.action.ConvalesceValidationAction`
whichever major it is on.

Import as:

import convalesce_emit_gx.action as cegxact
"""

import logging
from typing import Any, Optional, Tuple

_LOG = logging.getLogger(__name__)


def gx_major() -> Optional[int]:
    """
    Read the installed Great Expectations major version.

    :return: the major version, or None when GX is absent or unreadable
    """
    try:
        import great_expectations  # pylint: disable=import-outside-toplevel

        raw: str = str(great_expectations.__version__)
        first: str = raw.split(".", 1)[0]
        return int(first)
    except Exception:  # pylint: disable=broad-exception-caught
        # No GX, or a version string we cannot parse.
        return None


def _select() -> Tuple[Any, Optional[int]]:
    """
    Choose the action class for the installed Great Expectations.

    :return: the class and the major version it was chosen for
    """
    major = gx_major()
    if major is None:
        return None, None
    if major >= 1:
        import convalesce_emit_gx.action_v1 as impl  # pylint: disable=import-outside-toplevel
    else:
        import convalesce_emit_gx.action_v0 as impl  # type: ignore[no-redef] # pylint: disable=import-outside-toplevel
    return impl.ConvalesceValidationAction, major


# A re-exported class, not a constant: pylint reads any module-level
# assignment as one, and the name has to stay the class's own so a checkpoint
# can list it by the same path on either major.
# pylint: disable=invalid-name
ConvalesceValidationAction, GX_MAJOR = _select()

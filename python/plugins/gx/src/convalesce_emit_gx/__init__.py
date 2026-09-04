"""
Forward Great Expectations validation results to Convalesce.

Import as:

import convalesce_emit_gx as cegx
"""

import logging

from convalesce_emit_gx._version import __version__
from convalesce_emit_gx.action import GX_MAJOR, ConvalesceValidationAction

_LOG = logging.getLogger(__name__)

__all__ = ["GX_MAJOR", "ConvalesceValidationAction", "__version__"]

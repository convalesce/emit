"""
Forward Prefect flow and task run state to Convalesce.

Import as:

import convalesce_emit_prefect as ceprefec
"""

import logging

from convalesce_emit_prefect._version import __version__
from convalesce_emit_prefect.hooks import emit_flow_run, emit_task_run

_LOG = logging.getLogger(__name__)

__all__ = ["__version__", "emit_flow_run", "emit_task_run"]

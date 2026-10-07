"""
Forward Dagster run events to Convalesce.

Import as:

import convalesce_emit_dagster as cedag
"""

import logging

import convalesce_emit_dagster.steps as cedsteps
from convalesce_emit_dagster._version import __version__
from convalesce_emit_dagster.sensor import (
    convalesce_sensor,
    emit_dagster_event,
    unwrap_context,
)

_LOG = logging.getLogger(__name__)

# A step's process imports this package with the definitions the sensor is
# declared in, and nothing of ours is called there afterwards: importing is
# the only moment there is to start watching what its steps run.
cedsteps.install()

__all__ = [
    "__version__",
    "convalesce_sensor",
    "emit_dagster_event",
    "unwrap_context",
]

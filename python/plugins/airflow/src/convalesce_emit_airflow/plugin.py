"""
Airflow plugin entry point.

Import as:

import convalesce_emit_airflow.plugin as cealplug
"""

import logging
from typing import Any, ClassVar, List

from airflow.plugins_manager import AirflowPlugin

import convalesce_emit_airflow.listener as cealist

_LOG = logging.getLogger(__name__)


# #############################################################################
# ConvalescePlugin
# #############################################################################


class ConvalescePlugin(AirflowPlugin):  # type: ignore[misc]
    """Registers the Convalesce listener with Airflow."""

    name = "convalesce_emit"
    # filter(None, ...) so a listener that could not be built registers
    # nothing rather than putting a None into Airflow's plugin manager.
    listeners: ClassVar[List[Any]] = list(filter(None, [cealist.get_listener()]))

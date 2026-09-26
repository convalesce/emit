"""
Airflow plugin entry point.

Import as:

import convalesce_emit_airflow.plugin as cealplug
"""

import logging
from typing import Any, ClassVar, List

from airflow.plugins_manager import AirflowPlugin

import convalesce_emit_airflow.listener as cealist
import convalesce_emit_airflow.openlineage as cealol

_LOG = logging.getLogger(__name__)


# #############################################################################
# ConvalescePlugin
# #############################################################################


class ConvalescePlugin(AirflowPlugin):  # type: ignore[misc]
    """
    Registers the Convalesce listener with Airflow, and the OpenLineage
    provider's listener when this plugin is what switched it on.
    """

    name = "convalesce_emit"
    _openlineage: ClassVar[List[Any]] = cealol.enable()
    # filter(None, ...) so a listener that could not be built registers
    # nothing rather than putting a None into Airflow's plugin manager.
    listeners: ClassVar[List[Any]] = (
        list(filter(None, [cealist.get_listener()])) + _openlineage
    )
    # Only where this plugin switched OpenLineage on; a user's own setup
    # leaves the provider's plugin enabled, and it registers its own.
    hook_lineage_readers: ClassVar[List[Any]] = (
        cealol.hook_lineage_readers() if _openlineage else []
    )

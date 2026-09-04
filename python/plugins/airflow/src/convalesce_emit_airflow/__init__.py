"""
Forward Airflow's own listener payloads to Convalesce.

Import as:

import convalesce_emit_airflow as ceair
"""

import logging

from convalesce_emit_airflow._version import __version__
from convalesce_emit_airflow.listener import build_listener_class, get_listener

_LOG = logging.getLogger(__name__)

__all__ = ["__version__", "build_listener_class", "get_listener"]

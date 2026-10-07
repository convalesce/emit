"""
Report a PySpark driver to Convalesce: what it ran, and whether it failed.

Import as:

import convalesce_emit_pyspark as cepyspar
"""

import logging

from convalesce_emit_pyspark._version import __version__
from convalesce_emit_pyspark.driver import install

_LOG = logging.getLogger(__name__)

__all__ = ["__version__", "install"]

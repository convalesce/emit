"""
Reads the version of the tool a plugin is running inside.

Every plugin needs the same three lines -- import the host package, read its
`__version__`, give up quietly if either fails -- and pylint was right to
call four copies of it duplicated.

Import as:

import convalesce_emit.toolinfo as cetoolin
"""

import importlib
import logging
from typing import Optional

_LOG = logging.getLogger(__name__)


def version_of(module_name: str) -> Optional[str]:
    """
    Read a host tool's version.

    :param module_name: the tool's top-level module, such as "airflow"
    :return: its version, or None where it could not be read
    """
    try:
        module = importlib.import_module(module_name)
        version = getattr(module, "__version__", None)
        return str(version) if version is not None else None
    except Exception:  # pylint: disable=broad-exception-caught
        # A version is a nicety, never a blocker: the tool may be absent, or
        # may not publish one.
        return None

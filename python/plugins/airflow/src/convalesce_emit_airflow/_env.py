"""
A setting's value, under either of the names it can be given by.

Import as:

import convalesce_emit_airflow._env as cealenv
"""

import os
from typing import Mapping, Optional

# What a platform may put in front of a setting's name, as AWS Glue does
# with the variables it hands a job. Read here rather than through
# `convalesce_emit.config`, which a `convalesce-emit` older than that
# reader does not have.
_PLATFORM_PREFIX = "CUSTOMER_"


def read(
    name: str, environ: Optional[Mapping[str, str]] = None
) -> Optional[str]:
    """
    Read one of this plugin's settings, under either of its names.

    :param name: the setting, such as `CONVALESCE_SEND_ARGUMENTS`
    :param environ: the variables to read; the process's own when not given
    :return: its value as set, or None when it is set under neither name
    """
    env = os.environ if environ is None else environ
    value = env.get(name)
    if value is None:
        value = env.get(_PLATFORM_PREFIX + name)
    return value

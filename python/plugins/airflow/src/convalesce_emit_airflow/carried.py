"""
Make a carried copy of Airflow's OpenLineage provider importable where the
provider itself is not installed.

Table lineage, the SQL a task ran and column lineage for SQL operators all
come from the provider, so the plugin cannot do without it. It also cannot
ask pip for it: the newest provider needs the newest Airflow, and pip meets
that by replacing the libraries an older Airflow runs on. So the plugin
carries, under `_vendored/`, the provider and OpenLineage client that each
Airflow line's own constraints file pins, fetched unchanged by
`scripts/vendor_openlineage.py`, and this module puts the pair for the
running Airflow on the import path.

A provider somebody installed is always the one used: nothing here runs
when `airflow.providers.openlineage` already resolves. The same goes for
the client, one package at a time. The one piece not carried is the SQL
parser, `openlineage-sql`: it is compiled, depends on nothing, and is an
ordinary requirement of this distribution.

Import as:

import convalesce_emit_airflow.carried as cealcar
"""

import importlib
import importlib.util
import logging
import os
import pathlib
import sys
from typing import List, Optional, Tuple

import convalesce_emit_airflow._env as cealenv

_LOG = logging.getLogger(__name__)

_ROOT = pathlib.Path(__file__).parent / "_vendored"
_PROVIDER = "airflow.providers.openlineage"
_CLIENT = "openlineage"


def lines() -> List[Tuple[int, int]]:
    """
    The Airflow lines a copy is carried for, oldest first.

    :return: `(major, minor)` for each carried line
    """
    found = []
    for path in _ROOT.glob("airflow_*_*"):
        major, minor = path.name[len("airflow_") :].split("_")
        found.append((int(major), int(minor)))
    return sorted(found)


def line_for(airflow_version: Optional[str]) -> Optional[Tuple[int, int]]:
    """
    Which carried line fits a running Airflow.

    Its own line where one is carried. A newer Airflow than any carried
    takes the newest, which is the closest thing to it; an older one than
    any carried takes none, since the provider never ran there.

    :param airflow_version: the running Airflow's version, as it reports it
    :return: the line, or None when there is none to use or the version
        cannot be read
    """
    if not airflow_version:
        return None
    try:
        major, minor = (int(part) for part in airflow_version.split(".")[:2])
    except ValueError:
        return None
    carried = lines()
    fitting = [line for line in carried if line <= (major, minor)]
    return fitting[-1] if fitting else None


def _resolves(name: str) -> bool:
    """Whether a module can be found without importing it."""
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def make_importable(airflow_version: Optional[str]) -> Optional[str]:
    """
    Put the carried provider for this Airflow on the import path.

    :param airflow_version: the running Airflow's version
    :return: the line used, such as `2.9`; None when the provider is
        already installed, no line fits, or the path could not be set
    """
    if _resolves(_PROVIDER):
        return None
    line = line_for(airflow_version)
    if line is None:
        return None
    root = _ROOT / f"airflow_{line[0]}_{line[1]}"
    try:
        if not _resolves(_CLIENT):
            sys.path.append(str(root / "libs"))
        # `airflow.providers` is a namespace package: Python works its path
        # out from `airflow`'s, one `providers` directory per entry, and
        # works it out again whenever the import path changes. So the copy
        # goes in through `airflow`'s path, where it stays found; appended
        # to `airflow.providers` alone it is dropped at the next
        # recalculation, which the line above is enough to cause.
        airflow = importlib.import_module("airflow")
        airflow.__path__.append(str(root))
        providers = importlib.import_module("airflow.providers")
        if str(root / "providers") not in list(providers.__path__):
            providers.__path__.append(str(root / "providers"))
        importlib.invalidate_caches()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("convalesce: could not carry OpenLineage: %s", exc)
        return None
    if not _resolves(_PROVIDER):
        return None
    _config_defaults()
    return f"{line[0]}.{line[1]}"


def _config_defaults() -> None:
    """
    Give the carried provider's settings the defaults an install would.

    Airflow learns a provider's settings and their defaults from the
    installed distribution. A carried copy is not one, and the older
    provider releases read some settings with no fallback of their own, so
    an unset one is an error there. Each default the provider declares is
    put in the environment, where Airflow reads settings from, unless it
    is already set.
    """
    try:
        info = importlib.import_module(
            _PROVIDER + ".get_provider_info"
        ).get_provider_info()
        options = info["config"]["openlineage"]["options"]
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: no OpenLineage defaults to set: %s", exc)
        return
    for name, option in options.items():
        default = option.get("default")
        # The transport is this plugin's to choose, where nobody else has.
        if name == "transport":
            continue
        os.environ.setdefault(
            f"AIRFLOW__OPENLINEAGE__{name.upper()}",
            "" if default is None else str(default),
        )


_OFF = frozenset({"0", "false", "no", "off"})


def prepare(airflow_version: Optional[str]) -> Optional[str]:
    """
    Make the carried provider importable, unless OpenLineage is turned off.

    Called before anything imports from OpenLineage: a class built on its
    client has to find the real one, and one built before the copy is on
    the path would be built on nothing.

    :param airflow_version: the running Airflow's version
    :return: the line used, or None
    """
    for name in ("CONVALESCE_OPENLINEAGE", "CONVALESCE_ENABLED"):
        if (cealenv.read(name) or "").strip().lower() in _OFF:
            return None
    return make_importable(airflow_version)

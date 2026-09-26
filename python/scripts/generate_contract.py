"""
Generate each plugin package's `contract.json` from a directory of captures.

A capture is one line of JSON per observation, named `<tool>_<version>.jsonl`,
each line the observation exactly as a plugin sent it. That is the same shape
`collect/scripts/observe-live/README.md` recaptures into collect's fixtures,
so pointing this at that directory is the normal way to run it; nothing here
reads collect's tree directly, so any directory of same-shaped captures works.

For each (tool, event), the contract lists the field paths present in some
captured version's observations of that event: not a promise that every
supported version sends every path (a major version can rename or drop a
field, and Great Expectations 0.x and 1.x hand the action differently
shaped results under the same event name), but the whole set a receiver may
see from any version it might be talking to. A path missing from here is
one no supported version has ever been observed to send.

Usage:

    python scripts/generate_contract.py <captures-dir>
"""

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Optional, Set

HERE = Path(__file__).resolve().parent
PYTHON_ROOT = HERE.parent

# Tool name, as an envelope carries it, to the plugin's directory under
# `plugins/` and its package name. Spark is not here: nearly everything under
# its tool name comes from the Java listener and OpenLineage, and a capture
# would put all of it in convalesce-emit-pyspark's contract. That package
# sends one event, `driver_failure`, and its contract.json is kept by hand.
_PLUGIN_DIRS = {
    "airflow": "airflow",
    "dagster": "dagster",
    "great_expectations": "gx",
    "prefect": "prefect",
}


def field_paths(
    value: Any, prefix: str = "", out: Optional[Set[str]] = None
) -> Set[str]:
    """
    Every field path a payload carries.

    :param value: the payload, or part of one
    :param prefix: the path of this part of the payload
    :param out: where the paths are collected
    :return: the paths, as a receiver would address them
    """
    if out is None:
        out = set()
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            out.add(path)
            field_paths(item, path, out)
    elif isinstance(value, list):
        for item in value:
            field_paths(item, f"{prefix}[]", out)
    return out


def load_captures(
    captures_dir: Path,
) -> Dict[str, Dict[str, Dict[str, Set[str]]]]:
    """
    Read every capture, indexed by tool, event and version.

    :param captures_dir: directory of `<tool>_<version>.jsonl` files
    :return: tool -> event -> version -> the field paths that version's
        observations of that event carried
    """
    per_version: Dict[str, Dict[str, Dict[str, Set[str]]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for path in sorted(captures_dir.glob("*.jsonl")):
        lines = [line for line in path.read_text().splitlines() if line.strip()]
        if not lines:
            continue
        rows = [json.loads(line) for line in lines]
        tool = rows[0]["tool"]
        version = rows[0]["tool_version"]
        by_event: Dict[str, Set[str]] = defaultdict(set)
        for row in rows:
            by_event[row["event"]] |= field_paths(row["payload"])
        for event, paths in by_event.items():
            # Several captures of one version (a DAG's own events beside a
            # probe's OpenLineage ones) add up rather than replace each other.
            per_version[tool][event].setdefault(version, set()).update(paths)
    return per_version


def write_contracts(
    per_version: Dict[str, Dict[str, Dict[str, Set[str]]]],
) -> None:
    """
    Write one `contract.json` per plugin package.

    :param per_version: as returned by `load_captures`
    :return: nothing
    """
    for tool, events in per_version.items():
        plugin_dir = _PLUGIN_DIRS.get(tool)
        if plugin_dir is None:
            print(f"skipping {tool}: no Python plugin package", file=sys.stderr)
            continue
        package = f"convalesce_emit_{plugin_dir}"
        contract = {
            "tool": tool,
            "events": {
                event: sorted(set.union(*versions.values()))
                for event, versions in sorted(events.items())
                if versions
            },
        }
        out_path = (
            PYTHON_ROOT
            / "plugins"
            / plugin_dir
            / "src"
            / package
            / "contract.json"
        )
        out_path.write_text(
            json.dumps(contract, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}")


def main() -> None:
    """
    Entry point: generate every package's contract from the given directory.

    :return: nothing
    """
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <captures-dir>", file=sys.stderr)
        raise SystemExit(2)
    captures_dir = Path(sys.argv[1])
    per_version = load_captures(captures_dir)
    write_contracts(per_version)


if __name__ == "__main__":
    main()

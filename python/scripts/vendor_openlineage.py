"""
Fetch the copies of Airflow's OpenLineage provider that the Airflow plugin
carries.

The plugin has to work where the provider is not installed, and it cannot
ask pip for it: the newest provider needs the newest Airflow, and pip
satisfies that by replacing the libraries an older Airflow runs on. So the
plugin carries, for each Airflow line, the provider release and the
OpenLineage client release that line's own constraints file pins -- the
pair Airflow's maintainers released together -- and makes the right one
importable when nothing else is.

Nothing fetched here is edited. Run from `emit/python`:

    python scripts/vendor_openlineage.py

It rewrites `plugins/airflow/src/convalesce_emit_airflow/_vendored/` and
its `manifest.json`, which records every wheel's version and sha256.
"""

import hashlib
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import zipfile
from typing import Any, Dict, List

DEST = pathlib.Path("plugins/airflow/src/convalesce_emit_airflow/_vendored")

# Airflow line -> the provider and client its constraints file pins, read
# from constraints-<version>/constraints-<python>.txt for the patch named.
LINES: Dict[str, Dict[str, str]] = {
    "2.7": {"airflow": "2.7.3", "provider": "1.2.0", "client": "1.4.1"},
    "2.8": {"airflow": "2.8.4", "provider": "1.6.0", "client": "1.10.2"},
    "2.9": {"airflow": "2.9.3", "provider": "1.9.1", "client": "1.18.0"},
    "2.10": {"airflow": "2.10.5", "provider": "2.0.0", "client": "1.27.0"},
    "2.11": {"airflow": "2.11.0", "provider": "2.3.0", "client": "1.33.0"},
    "3.0": {"airflow": "3.0.3", "provider": "2.5.0", "client": "1.35.0"},
    "3.1": {"airflow": "3.1.0", "provider": "2.7.1", "client": "1.37.0"},
    "3.2": {"airflow": "3.2.2", "provider": "2.17.0", "client": "1.47.1"},
}

_PROVIDER = "apache-airflow-providers-openlineage"
_CLIENT = ("openlineage-python", "openlineage-integration-common")


def _download(spec: str, into: pathlib.Path) -> pathlib.Path:
    """Download one wheel, with nothing it depends on."""
    before = set(into.glob("*.whl"))
    subprocess.run(
        [
            sys.executable, "-m", "pip", "download", "--quiet", "--no-deps",
            "--only-binary", ":all:", "--dest", str(into), spec,
        ],
        check=True,
    )
    (wheel,) = set(into.glob("*.whl")) - before
    return wheel


def _record(wheel: pathlib.Path) -> Dict[str, str]:
    """A wheel's file name and sha256."""
    return {
        "wheel": wheel.name,
        "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
    }


def _copy(wheel: pathlib.Path, prefix: str, dest: pathlib.Path) -> None:
    """Unpack everything under `prefix` in a wheel into `dest`."""
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if not name.startswith(prefix) or name.endswith("/"):
                continue
            target = dest / name[len(prefix):]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(name))


def main() -> None:
    """Rebuild the carried copies and their manifest."""
    if DEST.exists():
        shutil.rmtree(DEST)
    manifest: Dict[str, Any] = {}
    with tempfile.TemporaryDirectory() as scratch:
        for line, pins in LINES.items():
            into = pathlib.Path(scratch) / line
            into.mkdir()
            root = DEST / ("airflow_" + line.replace(".", "_"))
            wheels: List[Dict[str, str]] = []
            provider = _download(f"{_PROVIDER}=={pins['provider']}", into)
            _copy(provider, "airflow/providers/openlineage/", root / "providers" / "openlineage")
            wheels.append(_record(provider))
            for name in _CLIENT:
                wheel = _download(f"{name}=={pins['client']}", into)
                _copy(wheel, "openlineage/", root / "libs" / "openlineage")
                wheels.append(_record(wheel))
            manifest[line] = {**pins, "wheels": wheels}
    (DEST / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (DEST / "__init__.py").write_text(
        '"""Copies of Airflow\'s OpenLineage provider and client, fetched unchanged\n'
        'by `scripts/vendor_openlineage.py`. Apache-2.0; see NOTICE."""\n'
    )


if __name__ == "__main__":
    main()

"""
Tests for choosing and exposing the carried OpenLineage provider.

Import as:

import convalesce_emit_airflow.test.test_carried as ceatestcar
"""

# pylint: disable=protected-access

import hashlib
import json
import os
import pathlib
import sys
import types
import unittest
import unittest.mock
from typing import Any, List

import convalesce_emit_airflow.carried as cealcar

_VENDORED = pathlib.Path(cealcar.__file__).parent / "_vendored"


class Test_line_for1(unittest.TestCase):
    """
    Test which carried copy a running Airflow is given.
    """

    def test1(self) -> None:
        """Test that an Airflow with a copy of its own line gets that one."""
        for version, line in (
            ("2.7.3", (2, 7)),
            ("2.9.3", (2, 9)),
            ("2.10.5", (2, 10)),
            ("2.11.0", (2, 11)),
            ("3.0.3", (3, 0)),
            ("3.2.2", (3, 2)),
        ):
            self.assertEqual(cealcar.line_for(version), line, version)

    def test2(self) -> None:
        """
        Test that a newer Airflow than any carried takes the newest, and
        one the provider never ran on takes none.
        """
        self.assertEqual(cealcar.line_for("3.9.0"), cealcar.lines()[-1])
        self.assertIsNone(cealcar.line_for("2.6.3"))
        self.assertIsNone(cealcar.line_for("2.5.3"))

    def test3(self) -> None:
        """Test that a version that cannot be read picks nothing."""
        for version in (None, "", "main", "x.y"):
            self.assertIsNone(cealcar.line_for(version))

    def test4(self) -> None:
        """Test that 2.10 sorts after 2.9, as a number and not as text."""
        self.assertLess(
            cealcar.lines().index((2, 9)), cealcar.lines().index((2, 10))
        )


class Test_manifest1(unittest.TestCase):
    """
    Test that what is carried is what the manifest says was fetched.
    """

    def test1(self) -> None:
        """
        Test that every line has its provider and client, and a manifest
        entry naming the wheels they came from.
        """
        manifest = json.loads((_VENDORED / "manifest.json").read_text())
        self.assertEqual(
            sorted(manifest),
            sorted(f"{major}.{minor}" for major, minor in cealcar.lines()),
        )
        for line, entry in manifest.items():
            root = _VENDORED / ("airflow_" + line.replace(".", "_"))
            self.assertTrue(
                (root / "providers" / "openlineage" / "__init__.py").is_file(),
                line,
            )
            self.assertTrue(
                (root / "libs" / "openlineage" / "client").is_dir(), line
            )
            self.assertEqual(len(entry["wheels"]), 3, line)
            for wheel in entry["wheels"]:
                self.assertEqual(
                    len(wheel["sha256"]), hashlib.sha256().digest_size * 2
                )


class Test_make_importable1(unittest.TestCase):
    """
    Test putting a carried copy on the import path, with a stand-in for
    Airflow's own `airflow.providers` namespace.
    """

    def setUp(self) -> None:
        """Give the test an `airflow.providers` whose path can be read."""
        self.path: List[str] = []
        providers = types.ModuleType("airflow.providers")
        providers.__path__ = self.path  # type: ignore[attr-defined]
        airflow = types.ModuleType("airflow")
        self.airflow_path: List[str] = []
        airflow.__path__ = self.airflow_path  # type: ignore[attr-defined]
        airflow.providers = providers  # type: ignore[attr-defined]
        modules = unittest.mock.patch.dict(
            sys.modules, {"airflow": airflow, "airflow.providers": providers}
        )
        modules.start()
        self.addCleanup(modules.stop)
        syspath = unittest.mock.patch.object(sys, "path", list(sys.path))
        syspath.start()
        self.addCleanup(syspath.stop)

    def _run(self, version: Any, resolves: Any) -> Any:
        with unittest.mock.patch.object(
            cealcar, "_resolves", side_effect=resolves
        ):
            return cealcar.make_importable(version)

    def test1(self) -> None:
        """
        Test that with neither installed, the provider and the client of
        the running Airflow's line are both made importable.
        """
        seen: List[str] = []

        def resolves(name: str) -> bool:
            seen.append(name)
            # Absent until this module has put it on the path.
            return name == "airflow.providers.openlineage" and bool(self.path)

        self.assertEqual(self._run("2.9.3", resolves), "2.9")
        self.assertTrue(self.path[0].endswith("airflow_2_9/providers"))
        # Through `airflow`'s own path too, which is what keeps it found.
        self.assertTrue(self.airflow_path[0].endswith("_vendored/airflow_2_9"))
        self.assertTrue(sys.path[-1].endswith("airflow_2_9/libs"))

    def test2(self) -> None:
        """Test that an installed provider is left alone."""
        self.assertIsNone(self._run("2.9.3", lambda name: True))
        self.assertEqual(self.path, [])

    def test3(self) -> None:
        """
        Test that an installed client is the one used, and only the
        provider is added.
        """
        before = list(sys.path)

        def resolves(name: str) -> bool:
            return name == "openlineage" or bool(self.path)

        self.assertEqual(self._run("3.0.3", resolves), "3.0")
        self.assertEqual(sys.path, before)

    def test4(self) -> None:
        """Test that an Airflow no copy fits is given nothing."""
        self.assertIsNone(self._run("2.6.3", lambda name: False))
        self.assertEqual(self.path, [])

    def test5(self) -> None:
        """
        Test that a copy that still does not resolve once on the path is
        reported as not there, so nothing is switched on over it.
        """
        self.assertIsNone(self._run("2.9.3", lambda name: False))


class Test_prepare1(unittest.TestCase):
    """
    Test that the copy is only put on the path where OpenLineage is wanted.
    """

    def test1(self) -> None:
        """Test that either off switch keeps the copy off the path."""
        for name in ("CONVALESCE_OPENLINEAGE", "CONVALESCE_ENABLED"):
            with (
                unittest.mock.patch.dict(os.environ, {name: "false"}),
                unittest.mock.patch.object(cealcar, "make_importable") as make,
            ):
                self.assertIsNone(cealcar.prepare("2.9.3"))
            make.assert_not_called()

    def test2(self) -> None:
        """Test that otherwise the running Airflow's copy is asked for."""
        with (
            unittest.mock.patch.dict(os.environ, {}, clear=False) as env,
            unittest.mock.patch.object(
                cealcar, "make_importable", return_value="2.9"
            ) as make,
        ):
            env.pop("CONVALESCE_OPENLINEAGE", None)
            env.pop("CONVALESCE_ENABLED", None)
            self.assertEqual(cealcar.prepare("2.9.3"), "2.9")
        make.assert_called_once_with("2.9.3")


class Test_config_defaults1(unittest.TestCase):
    """
    Test giving the carried provider the defaults an install would.
    """

    def test1(self) -> None:
        """
        Test that each declared default is set where nothing is, an
        existing value is kept, and the transport is left to this plugin.
        """
        info = types.ModuleType("info")
        info.get_provider_info = lambda: {  # type: ignore[attr-defined]
            "config": {
                "openlineage": {
                    "options": {
                        "disabled_for_operators": {"default": ""},
                        "config_path": {"default": None},
                        "namespace": {"default": "default"},
                        "transport": {"default": ""},
                    }
                }
            }
        }
        env = {"AIRFLOW__OPENLINEAGE__NAMESPACE": "theirs"}
        with (
            unittest.mock.patch.object(
                cealcar.importlib, "import_module", return_value=info
            ),
            unittest.mock.patch.dict(os.environ, env, clear=True),
        ):
            cealcar._config_defaults()
            self.assertEqual(
                dict(os.environ),
                {
                    "AIRFLOW__OPENLINEAGE__NAMESPACE": "theirs",
                    "AIRFLOW__OPENLINEAGE__DISABLED_FOR_OPERATORS": "",
                    "AIRFLOW__OPENLINEAGE__CONFIG_PATH": "",
                },
            )

    def test2(self) -> None:
        """Test that a provider that declares no settings is not an error."""
        with unittest.mock.patch.object(
            cealcar.importlib, "import_module", side_effect=ImportError("none")
        ):
            cealcar._config_defaults()

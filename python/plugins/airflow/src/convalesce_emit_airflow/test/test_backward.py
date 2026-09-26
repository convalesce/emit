"""
Tests that everything the installed Airflow provides is forwarded or excused.

Collect's payload coverage test checks one direction: every field this
plugin sends is read or knowingly ignored. This checks the other. It reads
the Airflow in this process -- its listener hookspecs, the fields of the
objects those hooks hand over, and the OpenLineage provider's facet classes
-- and fails on any that is neither forwarded (implemented by the listener,
or present in `contract.json`) nor listed in `backward_coverage.yml` with a
reason. A new Airflow that adds a hook or a field fails here until somebody
decides what it is worth.

Skipped where Airflow is not installed, so `make test` passes without it.
Run against a real install, one venv per version:

    AIRFLOW_HOME=$(mktemp -d) pytest \
        plugins/airflow/src/convalesce_emit_airflow/test/test_backward.py
"""

import functools
import importlib
import importlib.util
import inspect
import json
import logging
import pathlib
import pkgutil
import re
import unittest
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import convalesce_emit_airflow.listener as cealist

_LOG = logging.getLogger(__name__)

_HERE = pathlib.Path(__file__).resolve().parent
_COVERAGE = _HERE / "backward_coverage.yml"
_CONTRACT = _HERE.parent / "contract.json"

# Walked whole rather than listed module by module, so a spec module a new
# Airflow adds is read too.
_SPEC_PACKAGES = (
    "airflow.listeners.spec",
    "airflow._shared.listeners.spec",
    "airflow.sdk._shared.listeners.spec",
)
# pluggy marks each hookspec with `<project>_spec`.
_SPEC_MARKER = "airflow_spec"
# Constructor parameters that configure the call rather than the object.
_NOT_FIELDS = frozenset({"self", "args", "kwargs"})


def _importable(name: str) -> bool:
    """
    Whether a module can be found here.

    :param name: the module
    :return: whether it is importable
    """
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:  # pylint: disable=broad-exception-caught
        # A parent package that is half there raises rather than answering.
        return False


_HAS_AIRFLOW = _importable("airflow")
_SKIP_REASON = "needs a real Airflow install"


# #############################################################################
# reading what is declared
# #############################################################################


@functools.lru_cache(maxsize=1)
def coverage() -> Dict[str, Any]:
    """
    The exclusion file.

    PyYAML comes with every Airflow, and this module only runs where one is
    installed, so it is imported here rather than made a dependency.

    :return: the parsed file
    """
    import yaml  # pylint: disable=import-outside-toplevel

    loaded: Dict[str, Any] = yaml.safe_load(_COVERAGE.read_text())
    return loaded


@functools.lru_cache(maxsize=1)
def contract() -> Dict[str, Set[str]]:
    """
    Every field path the plugin has been captured sending, per event.

    :return: event to its paths, with list markers dropped
    """
    loaded = json.loads(_CONTRACT.read_text())
    return {
        event: {path.replace("[]", "") for path in paths}
        for event, paths in loaded["events"].items()
    }


def all_paths() -> Set[str]:
    """
    Every captured path, whichever event carried it.

    :return: the paths
    """
    out: Set[str] = set()
    for paths in contract().values():
        out |= paths
    return out


def hookspecs() -> Dict[str, Tuple[str, ...]]:
    """
    Every listener hook this Airflow specifies, with its parameters.

    :return: hook name to parameter names
    """
    found: Dict[str, Tuple[str, ...]] = {}
    for package_name in _SPEC_PACKAGES:
        try:
            package = importlib.import_module(package_name)
        except Exception:  # pylint: disable=broad-exception-caught
            # A package this Airflow does not have; the others may.
            continue
        for info in pkgutil.iter_modules(package.__path__):
            module = importlib.import_module(f"{package_name}.{info.name}")
            for name, fn in vars(module).items():
                if callable(fn) and hasattr(fn, _SPEC_MARKER):
                    found[name] = tuple(inspect.signature(fn).parameters)
    return found


def resolve(path: str) -> Optional[type]:
    """
    A class by dotted path, or None where this Airflow has no such class.

    :param path: `module.Class`
    :return: the class
    """
    module_name, _, name = path.rpartition(".")
    if not _importable(module_name):
        return None
    try:
        module = importlib.import_module(module_name)
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    found = getattr(module, name, None)
    return found if isinstance(found, type) else None


def fields_of(cls: type) -> Set[str]:
    """
    The fields a class declares, however it declares them.

    An ORM model's mapped attributes, a pydantic model's fields, an attrs
    class's attributes; for a plain class (Airflow 2's DAG and operators)
    what it serialises plus what its constructor takes.

    :param cls: the class
    :return: the field names, as the dump would key them
    """
    names: Set[str] = set()
    mapper = getattr(cls, "__mapper__", None)
    if mapper is not None:
        names |= {attr.key for attr in mapper.attrs}
    model_fields = getattr(cls, "model_fields", None)
    if isinstance(model_fields, dict):
        names |= set(model_fields)
    declared = getattr(cls, "__attrs_attrs__", None)
    if isinstance(declared, tuple):
        names |= {attr.name for attr in declared}
    if names:
        return names
    getter = getattr(cls, "get_serialized_fields", None)
    if callable(getter):
        names |= set(getter())
    init = getattr(cls, "__init__")
    names |= set(inspect.signature(init).parameters) - _NOT_FIELDS
    return names


def forwarded_names(paths: Iterable[str], prefixes: Sequence[str]) -> Set[str]:
    """
    The field names captured directly under any of the prefixes.

    :param paths: captured paths
    :param prefixes: where the object lands, `*` matching one segment
    :return: the names found one segment below a prefix
    """
    patterns = [
        re.compile(
            "^"
            + r"\.".join(
                "[^.]+" if part == "*" else re.escape(part)
                for part in prefix.split(".")
            )
            + r"\.([^.]+)"
        )
        for prefix in prefixes
    ]
    out: Set[str] = set()
    for path in paths:
        for pattern in patterns:
            match = pattern.match(path)
            if match:
                out.add(match.group(1))
    return out


def unaccounted(
    provided: Set[str], forwarded: Set[str], entry: Dict[str, Any]
) -> Tuple[List[str], List[str]]:
    """
    What is neither forwarded nor excused, and what is excused needlessly.

    :param provided: the names the tool provides
    :param forwarded: the names captured
    :param entry: the object's catalogue entry
    :return: the missing names, and the listed names that are forwarded
    """
    listed = set(entry.get("excluded") or {}) | set(
        entry.get("uncaptured") or {}
    )
    if "*" in listed:
        return [], []
    # The dump sends `_x` as `x` where the class reads it back through a
    # property of that name; see `_dump_container` in the core.
    missing = sorted(
        name
        for name in provided - forwarded - listed
        if not (name.startswith("_") and name.lstrip("_") in forwarded)
    )
    stale = sorted(listed & forwarded)
    return missing, stale


def check_objects(
    catalogue: Dict[str, Dict[str, Any]], classes: Dict[str, List[type]]
) -> Tuple[List[str], List[str]]:
    """
    Check every catalogued object's fields against the contract.

    :param catalogue: object name to its entry
    :param classes: object name to the classes found for it here
    :return: missing `Object.field` and stale `Object.field` descriptions
    """
    paths = all_paths()
    missing: List[str] = []
    stale: List[str] = []
    for name, entry in catalogue.items():
        forwarded = forwarded_names(paths, entry["at"])
        for cls in classes.get(name, []):
            lost, needless = unaccounted(fields_of(cls), forwarded, entry)
            missing += [
                f"{name}.{field} ({cls.__module__}.{cls.__qualname__})"
                for field in lost
            ]
            stale += [f"{name}.{field}" for field in needless]
    return missing, sorted(set(stale))


# #############################################################################
# Test_hooks1
# #############################################################################


@unittest.skipUnless(_HAS_AIRFLOW, _SKIP_REASON)
class Test_hooks1(unittest.TestCase):
    """
    Test that every listener hook Airflow specifies is forwarded or excused.
    """

    def test1(self) -> None:
        """
        Test that each hook is implemented with Airflow's exact parameters,
        or excluded with a reason.
        """
        excluded = coverage()["hooks"]["excluded"]
        listener = cealist.build_listener_class()
        specs = hookspecs()
        self.assertTrue(specs, "no hookspecs found; the spec packages moved")
        problems = []
        for name, params in sorted(specs.items()):
            if name in excluded:
                continue
            hook = getattr(listener, name, None)
            if hook is None:
                problems.append(f"{name}{params}: not implemented")
                continue
            implemented = tuple(inspect.signature(hook).parameters)[1:]
            if implemented != params:
                problems.append(
                    f"{name}: implements {implemented}, spec {params}"
                )
        self.assertEqual(problems, [], "add to WANTED or to hooks.excluded")

    def test2(self) -> None:
        """
        Test that each implemented hook's parameters are in the contract, or
        the hook is listed as uncaptured.
        """
        uncaptured = coverage()["hooks"]["uncaptured"]
        listener = cealist.build_listener_class()
        problems = []
        for name, params in sorted(hookspecs().items()):
            if getattr(listener, name, None) is None or name in uncaptured:
                continue
            captured = contract().get(name)
            if captured is None:
                problems.append(f"{name}: never captured")
                continue
            problems += [
                f"{name}.{param}: never captured"
                for param in params
                if param
                not in cealist._SKIP_ARGS  # pylint: disable=protected-access
                and param not in captured
            ]
        self.assertEqual(
            problems, [], "recapture, or list under hooks.uncaptured"
        )

    def test3(self) -> None:
        """
        Test that no hook is excused needlessly.
        """
        hooks = coverage()["hooks"]
        listener = cealist.build_listener_class()
        stale = [
            f"excluded but implemented: {name}"
            for name in hooks["excluded"]
            if getattr(listener, name, None) is not None
        ] + [
            f"uncaptured but in the contract: {name}"
            for name in hooks["uncaptured"]
            if name in contract()
        ]
        self.assertEqual(stale, [])

    def test4(self) -> None:
        """
        Test that every entry carries a reason.
        """
        hooks = coverage()["hooks"]
        blank = [
            name
            for section in ("excluded", "uncaptured")
            for name, reason in hooks[section].items()
            if not str(reason or "").strip()
        ]
        self.assertEqual(blank, [])


# #############################################################################
# Test_objects1
# #############################################################################


@unittest.skipUnless(_HAS_AIRFLOW, _SKIP_REASON)
class Test_objects1(unittest.TestCase):
    """
    Test that every field of every object a hook hands over is forwarded or
    excused.
    """

    @staticmethod
    def _classes() -> Dict[str, List[type]]:
        return {
            name: [
                cls
                for cls in (resolve(path) for path in entry["classes"])
                if cls is not None
            ]
            for name, entry in coverage()["objects"].items()
        }

    def test1(self) -> None:
        """
        Test that each required object resolves to a class in this Airflow,
        so a class that moved cannot silently drop out of the check.
        """
        classes = self._classes()
        unresolved = [
            name
            for name, entry in coverage()["objects"].items()
            if entry.get("required") and not classes[name]
        ]
        self.assertEqual(unresolved, [], "a class moved; add its new path")

    def test2(self) -> None:
        """
        Test that each field is in the contract or listed with a reason.
        """
        missing, _ = check_objects(coverage()["objects"], self._classes())
        self.assertEqual(
            missing, [], "list under the object's excluded/uncaptured"
        )

    def test3(self) -> None:
        """
        Test that no field is excused needlessly.
        """
        _, stale = check_objects(coverage()["objects"], self._classes())
        self.assertEqual(stale, [], "forwarded now; remove the entry")


# #############################################################################
# Test_openlineage_facets1
# #############################################################################


_FACETS = "airflow.providers.openlineage.plugins.facets"


@unittest.skipUnless(
    _HAS_AIRFLOW and _importable(_FACETS),
    "needs Airflow with the OpenLineage provider",
)
class Test_openlineage_facets1(unittest.TestCase):
    """
    Test that the OpenLineage provider's facets are forwarded or excused.
    """

    @staticmethod
    def _defined() -> Dict[str, type]:
        module = importlib.import_module(_FACETS)
        return {
            name: value
            for name, value in vars(module).items()
            if isinstance(value, type)
            and value.__module__ == module.__name__
            and hasattr(value, "__attrs_attrs__")
        }

    def test1(self) -> None:
        """
        Test that every facet class the provider defines is catalogued.
        """
        catalogue = coverage()["openlineage_facets"]["objects"]
        new = sorted(set(self._defined()) - set(catalogue))
        self.assertEqual(new, [], "catalogue the new facet class")

    def test2(self) -> None:
        """
        Test that each facet field is in the contract or listed with a reason.
        """
        catalogue = coverage()["openlineage_facets"]["objects"]
        defined = self._defined()
        classes = {
            name: [defined[name]] for name in catalogue if name in defined
        }
        missing, stale = check_objects(catalogue, classes)
        self.assertEqual(
            missing, [], "list under the facet's excluded/uncaptured"
        )
        self.assertEqual(stale, [], "forwarded now; remove the entry")

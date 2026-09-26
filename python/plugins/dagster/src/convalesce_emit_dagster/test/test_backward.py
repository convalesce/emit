"""
Tests that everything the installed Dagster offers is forwarded or excused.

The forward check lives in collect: every field a decoder reads must be in
`contract.json`. This is the other direction. It enumerates what the
installed Dagster itself defines -- every event type, the fields of the
data each forwarded event carries, the run and its stats, every metadata
value type and every standard `dagster/` metadata key -- and asks that
each one either appears in `contract.json` or is listed in
`backward_exclusions.json` with the reason it is not sent. A Dagster
release that adds any of these fails here until somebody decides.

Skipped where Dagster is not installed; run it in a venv with the version
under test:

    pip install dagster==<version> -e python -e python/plugins/dagster
    pytest python/plugins/dagster/src/convalesce_emit_dagster/test/test_backward.py
"""

import dataclasses
import enum
import json
import pathlib
import sys
import unittest
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import convalesce_emit_dagster.sensor as cedsens

try:
    import dagster

    # Not loaded by `import dagster`, and where the step stats live.
    import dagster._core.execution.stats  # noqa: F401
    from dagster._core.definitions.metadata import MetadataValue
    from dagster._core.events import DagsterEventType

    _VERSION: Optional[str] = dagster.__version__
except ImportError:  # pragma: no cover - only where Dagster is absent
    _VERSION = None

_HERE = pathlib.Path(__file__).resolve().parent
_CONTRACT = _HERE.parent / "contract.json"
_EXCLUSIONS = _HERE / "backward_exclusions.json"

# The events a run-status sensor is fired with that a receiver reads: a run
# that finished, one way or another. The event itself crosses whole as
# `dagster_event`; the event log crosses for `cedsens.EVENT_LOG_TYPE_NAMES`.
_TRIGGER_TYPE_NAMES = ("RUN_SUCCESS", "RUN_FAILURE", "RUN_CANCELED")

_EVENT = "event_log[].event_log_entry.dagster_event"
_DATA = f"{_EVENT}.event_specific_data"
_TRIGGER_DATA = "dagster_event.event_specific_data"

# Each forwarded event type, to the class of the data it carries. Dagster
# keeps no such table: `_validate_event_specific_data` checks only some of
# them, so a new type is caught by the event-type test and mapped here.
_DATA_CLASS_BY_TYPE = {
    "ASSET_CHECK_EVALUATION": "AssetCheckEvaluation",
    "ASSET_MATERIALIZATION": "StepMaterializationData",
    "ASSET_OBSERVATION": "AssetObservationData",
    "HANDLED_OUTPUT": "HandledOutputData",
    "LOADED_INPUT": "LoadedInputData",
    "RESOURCE_INIT_SUCCESS": "EngineEventData",
    "STEP_FAILURE": "StepFailureData",
    "RUN_FAILURE": "JobFailureData",
    "RUN_CANCELED": "JobCanceledData",
    "RUN_SUCCESS": None,
}

# The objects the sensor forwards whole, by class name, to where each one's
# fields cross. Every supported version has all of them; one that goes
# missing fails, since its fields would otherwise go unchecked.
_CARRIERS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("DagsterRun", ("dagster_run",)),
    ("DagsterEvent", ("dagster_event", _EVENT)),
    ("EventLogEntry", ("event_log[].event_log_entry",)),
    ("EventLogRecord", ("event_log[]",)),
    ("DagsterRunStatsSnapshot", ("run_stats",)),
    ("RunStepKeyStatsSnapshot", ("step_stats[]",)),
    (
        "RunStepMarker",
        ("step_stats[].markers[]", "step_stats[].attempts_list[]"),
    ),
    ("AssetMaterialization", (f"{_DATA}.materialization",)),
    ("AssetObservation", (f"{_DATA}.asset_observation",)),
    (
        "AssetCheckEvaluationTargetMaterializationData",
        (f"{_DATA}.target_materialization_data",),
    ),
    ("UserFailureData", (f"{_DATA}.user_failure_data",)),
    ("SerializableErrorInfo", (f"{_DATA}.error", f"{_TRIGGER_DATA}.error")),
)


def _load(path: pathlib.Path) -> Any:
    """
    Read one JSON file beside this package.

    :param path: the file
    :return: its content
    """
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _contract_paths() -> Set[str]:
    """
    Every field path the published contract lists, for any event.

    :return: the paths
    """
    events = _load(_CONTRACT)["events"]
    return {path for paths in events.values() for path in paths}


def _dagster_class(name: str) -> Optional[type]:
    """
    A class by name from whichever Dagster module defines it on this version.

    Dagster moves classes between private modules from release to release;
    importing `dagster` loads them all, so the loaded modules are searched.

    :param name: the class name
    :return: the class, or None when this version has none by that name
    """
    for module_name, module in list(sys.modules.items()):
        if not module_name.startswith("dagster") or module is None:
            continue
        found = getattr(module, name, None)
        if isinstance(found, type) and found.__name__ == name:
            return found
    return None


def _fields(cls: type) -> List[str]:
    """
    The public fields of a Dagster value class, however it declares them.

    Dagster 1.7 declares them as a `NamedTuple`, 1.13 as a `@record` or a
    pydantic model; each has its own way to list them.

    :param cls: the class
    :return: its field names
    """
    names: Iterable[str]
    if hasattr(cls, "_fields"):
        names = getattr(cls, "_fields")
    elif dataclasses.is_dataclass(cls):
        names = [field.name for field in dataclasses.fields(cls)]
    elif hasattr(cls, "model_fields"):
        names = list(getattr(cls, "model_fields"))
    else:
        names = [
            name
            for klass in reversed(cls.__mro__)
            for name in getattr(klass, "__annotations__", {})
        ]
    return sorted({name for name in names if not name.startswith("_")})


def _subclasses(cls: type) -> Set[type]:
    """
    Every subclass of a class, however deep.

    :param cls: the class
    :return: its subclasses
    """
    out: Set[type] = set()
    for sub in cls.__subclasses__():
        out.add(sub)
        out |= _subclasses(sub)
    return out


def _missing_fields(
    cls: type, prefixes: Iterable[str], contract: Set[str], excused: Set[str]
) -> List[str]:
    """
    The fields of a class that are neither in the contract nor excused.

    :param cls: the class
    :param prefixes: where its fields cross, as contract paths
    :param contract: the contract's paths
    :param excused: `Class.field` names the exclusions account for
    :return: `Class.field` for each unaccounted field
    """
    missing = []
    for name in _fields(cls):
        label = f"{cls.__name__}.{name}"
        if label in excused:
            continue
        if not any(f"{prefix}.{name}" in contract for prefix in prefixes):
            missing.append(label)
    return missing


def _metadata_field_names(contract: Set[str]) -> Set[str]:
    """
    The value-class fields a forwarded metadata entry has crossed with.

    A metadata value crosses as `...metadata.<key>.<field>`: its class is
    not sent, only its fields, so a type is judged by those.

    :param contract: the contract's paths
    :return: the field names found one level under a metadata key
    """
    out: Set[str] = set()
    for path in contract:
        parts = path.split(".")
        for i, part in enumerate(parts[:-2]):
            if part == "metadata" and i + 3 == len(parts):
                out.add(parts[-1])
    return out


def _standard_metadata_keys() -> Set[str]:
    """
    Every `dagster/` metadata key the installed Dagster defines.

    Dagster declares its standard keys as the fields of its namespaced
    metadata sets -- `TableMetadataSet`, `UriMetadataSet` and the rest -- so
    a new standard key is a new field on one of them.

    :return: the keys, namespace included
    """
    base = _dagster_class("NamespacedMetadataSet")
    if base is None:
        return set()
    keys = set()
    for cls in _subclasses(base):
        if not cls.__module__.startswith("dagster."):
            continue
        namespace = getattr(cls, "namespace")()
        keys |= {f"{namespace}/{name}" for name in _fields(cls)}
    return keys


# #############################################################################
# Test_exclusions1
# #############################################################################


class Test_exclusions1(unittest.TestCase):
    """
    Test that the exclusion file is well formed.
    """

    def test1(self) -> None:
        """
        Test that every exclusion gives a reason.
        """
        for section, entries in _load(_EXCLUSIONS).items():
            if section.startswith("_"):
                continue
            for name, reason in entries.items():
                self.assertTrue(
                    isinstance(reason, str) and reason.strip(),
                    f"{section}.{name} has no reason",
                )

    def test2(self) -> None:
        """
        Test that no event type is both forwarded and excluded.
        """
        excluded = set(_load(_EXCLUSIONS)["event_types"])
        forwarded = set(cedsens.EVENT_LOG_TYPE_NAMES) | set(_TRIGGER_TYPE_NAMES)
        self.assertEqual(sorted(excluded & forwarded), [])

    def test3(self) -> None:
        """
        Test that no metadata key the sensor allows is also excluded.
        """
        excluded = set(_load(_EXCLUSIONS)["metadata_keys"])
        self.assertEqual(sorted(excluded & cedsens.METADATA_ALLOWED), [])


# #############################################################################
# Test_backward1
# #############################################################################


@unittest.skipIf(_VERSION is None, "Dagster is not installed")
class Test_backward1(unittest.TestCase):
    """
    Test that what the installed Dagster defines is forwarded or excused.
    """

    maxDiff = None

    def setUp(self) -> None:
        """
        Read the contract and the exclusions.
        """
        self.contract = _contract_paths()
        self.exclusions: Dict[str, Dict[str, str]] = _load(_EXCLUSIONS)

    def test1(self) -> None:
        """
        Test that every `DagsterEventType` is forwarded or excluded.
        """
        members = {
            member.name
            for member in DagsterEventType  # type: ignore[union-attr]
            if isinstance(member, enum.Enum)
        }
        forwarded = set(cedsens.EVENT_LOG_TYPE_NAMES) | set(_TRIGGER_TYPE_NAMES)
        unaccounted = sorted(
            members - forwarded - set(self.exclusions["event_types"])
        )
        self.assertEqual(
            unaccounted,
            [],
            f"Dagster {_VERSION} has event types neither forwarded "
            f"nor in {_EXCLUSIONS.name}: {unaccounted}",
        )

    def test2(self) -> None:
        """
        Test that each forwarded event type's data class is mapped, and each
        of its fields is in the contract or excluded.
        """
        excused = set(self.exclusions["fields"])
        forwarded = list(cedsens.EVENT_LOG_TYPE_NAMES) + list(
            _TRIGGER_TYPE_NAMES
        )
        missing: List[str] = []
        for type_name in forwarded:
            self.assertIn(
                type_name,
                _DATA_CLASS_BY_TYPE,
                f"{type_name} is forwarded but its data class is not mapped",
            )
            class_name = _DATA_CLASS_BY_TYPE[type_name]
            if class_name is None:
                continue
            cls = _dagster_class(class_name)
            self.assertIsNotNone(cls, f"no {class_name} in this Dagster")
            assert cls is not None
            prefixes = (
                (_TRIGGER_DATA,)
                if type_name in _TRIGGER_TYPE_NAMES
                else (_DATA,)
            )
            missing += _missing_fields(cls, prefixes, self.contract, excused)
        self.assertEqual(
            sorted(set(missing)),
            [],
            f"Dagster {_VERSION} event data fields neither in "
            f"contract.json nor in {_EXCLUSIONS.name}",
        )

    def test3(self) -> None:
        """
        Test that the fields of the run, its stats, its events and the
        objects inside them are in the contract or excluded.
        """
        excused = set(self.exclusions["fields"])
        missing: List[str] = []
        for class_name, prefixes in _CARRIERS:
            cls = _dagster_class(class_name)
            self.assertIsNotNone(cls, f"no {class_name} in this Dagster")
            assert cls is not None
            missing += _missing_fields(cls, prefixes, self.contract, excused)
        self.assertEqual(
            sorted(missing),
            [],
            f"Dagster {_VERSION} fields neither in contract.json "
            f"nor in {_EXCLUSIONS.name}",
        )

    def test4(self) -> None:
        """
        Test that every `MetadataValue` type has crossed or is excluded.
        """
        crossed = _metadata_field_names(self.contract)
        excused = set(self.exclusions["metadata_types"])
        missing = []
        for cls in _subclasses(MetadataValue):  # type: ignore[arg-type]
            fields = _fields(cls)
            if cls.__name__ in excused or not fields:
                continue
            if not set(fields) <= crossed:
                missing.append(cls.__name__)
        self.assertEqual(
            sorted(set(missing)),
            [],
            f"Dagster {_VERSION} metadata value types neither in "
            f"contract.json nor in {_EXCLUSIONS.name}",
        )

    def test5(self) -> None:
        """
        Test that every standard `dagster/` metadata key is allowed through
        and in the contract, or excluded.
        """
        excused = set(self.exclusions["metadata_keys"])
        missing = []
        for key in sorted(_standard_metadata_keys() - excused):
            in_contract = any(
                path.endswith(f".{key}") or f".{key}." in path
                for path in self.contract
            )
            if key not in cedsens.METADATA_ALLOWED or not in_contract:
                missing.append(key)
        self.assertEqual(
            missing,
            [],
            f"Dagster {_VERSION} standard metadata keys neither "
            f"forwarded nor in {_EXCLUSIONS.name}",
        )

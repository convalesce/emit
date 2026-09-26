"""
Tests that everything the installed Prefect offers is forwarded or excluded.

The forward check, collect's payload coverage, asks whether a receiver uses
what the plugin sends. This asks the other way round: whether the plugin
sends what Prefect has. It enumerates, from the installed Prefect itself,
the state hooks a flow and a task accept, the fields of every object the
hooks and the API reads forward, and the families of events Prefect emits.
Each must be forwarded -- a hook the plugin's functions are wired to, a field
in `contract.json`, an event family a hook carries -- or listed in
`backward_exclusions.json` with the reason it is not. A Prefect release that
adds a hook, a field or an event family fails here until somebody decides.

Skipped where Prefect is not installed, as in the plain unit-test run. Run it
in a virtualenv per supported version, e.g.
`pip install prefect==3.8.6 -e . -e plugins/prefect && pytest plugins/prefect`.

Import as:

import convalesce_emit_prefect.test.test_backward as ceptbac
"""

import importlib
import importlib.util
import inspect
import json
import logging
import pathlib
import re
import unittest
from typing import Any, Dict, Iterator, List, Set, Tuple
from unittest import mock

import convalesce_emit_prefect.hooks as cephooks

_LOG = logging.getLogger(__name__)

_HERE = pathlib.Path(__file__).resolve().parent
_CONTRACT = _HERE.parent / "contract.json"
_EXCLUSIONS = _HERE / "backward_exclusions.json"

_HAS_PREFECT = importlib.util.find_spec("prefect") is not None
_SKIP_REASON = "Prefect is not installed"

# The hooks `emit_flow_run` and `emit_task_run` are made for: each is handed
# the flow or task, its run and the state, which is every signature below.
_SUPPORTED_HOOKS = {
    "flow": frozenset(
        {
            "on_cancellation",
            "on_completion",
            "on_crashed",
            "on_failure",
            "on_running",
        }
    ),
    "task": frozenset({"on_completion", "on_failure", "on_running"}),
}

# A constructor argument is a hook when Prefect calls it back: `on_*`, or a
# `*_fn` a task consults as it runs.
_HOOK_ARGUMENT = re.compile(r"^on_|_fn$")

# Each object Prefect hands the hooks, or the API reads return, and where in
# the payload the plugin forwards it. A field is forwarded when the contract
# lists it under any of the object's places.
_MODELS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "FlowRun": (
        "prefect.client.schemas.objects",
        ("flow_run", "api_flow_run"),
    ),
    "TaskRun": (
        "prefect.client.schemas.objects",
        ("task_run", "api_task_runs[]"),
    ),
    "State": (
        "prefect.client.schemas.objects",
        ("state", "flow_run.state", "task_run.state", "api_flow_run.state"),
    ),
    "StateDetails": (
        "prefect.client.schemas.objects",
        ("state.state_details", "api_flow_run.state.state_details"),
    ),
    "FlowRunPolicy": (
        "prefect.client.schemas.objects",
        ("flow_run.empirical_policy", "api_flow_run.empirical_policy"),
    ),
    "TaskRunPolicy": (
        "prefect.client.schemas.objects",
        ("task_run.empirical_policy", "api_task_runs[].empirical_policy"),
    ),
    "Flow": ("prefect.client.schemas.objects", ("api_flow",)),
    "Deployment": ("prefect.client.schemas.objects", ()),
    "WorkPool": ("prefect.client.schemas.objects", ()),
    "Asset": ("prefect.assets", ("task.assets[]", "task.asset_deps[]")),
    "AssetProperties": (
        "prefect.assets",
        ("task.assets[].properties", "task.asset_deps[].properties"),
    ),
}

# The event families a hook carries: a flow run's and a task run's state
# changes, and the assets a task materializes, which its task names.
_CARRIED_EVENTS = frozenset({"asset", "flow-run", "task-run"})

# `prefect.<family>.` as an event name is written into Prefect's own source.
_EVENT_NAME = re.compile(
    r"""(?:event\s*=|emit_event\(|Event\()\s*f?["']prefect(?:-cloud)?"""
    r"""\.([a-z][a-z-]*)\."""
)


def _contract_paths() -> Set[str]:
    """
    Every field path the published contract lists, whatever the event.

    :return: the paths
    """
    contract = json.loads(_CONTRACT.read_text())
    return {path for paths in contract["events"].values() for path in paths}


def _exclusions() -> Dict[str, Any]:
    """
    The exclusion file, every entry with its reason.

    :return: hooks, fields and events not forwarded, and why
    """
    loaded: Dict[str, Any] = json.loads(_EXCLUSIONS.read_text())
    return loaded


def _field_names(model: Any) -> List[str]:
    """
    A Prefect model's declared fields, on Pydantic 1 or 2.

    :param model: the model class
    :return: its field names
    """
    fields = getattr(model, "model_fields", None) or getattr(
        model, "__fields__", {}
    )
    return sorted(fields)


def _flow_and_task_classes() -> Iterator[Tuple[str, Any]]:
    """
    The flow and task classes of the installed Prefect.

    :return: the kind and the class, for a flow and for every task class
    """
    flows = importlib.import_module("prefect.flows")
    tasks = importlib.import_module("prefect.tasks")
    yield "flow", flows.Flow
    yield "task", tasks.Task
    materializing = getattr(tasks, "MaterializingTask", None)
    if materializing is not None:
        yield "task", materializing


def _hook_arguments(cls: Any) -> Set[str]:
    """
    The constructor arguments a class calls back.

    :param cls: a flow or task class
    :return: their names
    """
    parameters = inspect.signature(cls.__init__).parameters
    return {name for name in parameters if _HOOK_ARGUMENT.search(name)}


def _event_families() -> Dict[str, Set[str]]:
    """
    Every family of event the installed Prefect's source names.

    :return: family to the modules naming it
    """
    prefect = importlib.import_module("prefect")
    root = pathlib.Path(inspect.getfile(prefect)).parent
    families: Dict[str, Set[str]] = {}
    for path in root.rglob("*.py"):
        relative = path.relative_to(root).as_posix()
        if relative.startswith(("testing/", "_vendor/")):
            continue
        text = path.read_text(errors="ignore")
        for match in _EVENT_NAME.finditer(text):
            families.setdefault(match.group(1), set()).add(relative)
    return families


def _unaccounted(
    names: Set[str], forwarded: Set[str], excluded: Dict[str, str]
) -> List[str]:
    """
    The names neither forwarded nor excluded with a reason.

    :param names: what the installed Prefect has
    :param forwarded: what the plugin forwards
    :param excluded: what it does not, and why
    :return: the rest, sorted
    """
    return sorted(
        name
        for name in names
        if name not in forwarded and not excluded.get(name, "").strip()
    )


# #############################################################################
# Test_hooks1
# #############################################################################


@unittest.skipUnless(_HAS_PREFECT, _SKIP_REASON)
class Test_hooks1(unittest.TestCase):
    """
    Test that every hook Prefect calls back is wired or excluded.
    """

    def test1(self) -> None:
        """
        Test that each flow and task hook argument is one the plugin's
        functions serve, or is excluded with a reason.
        """
        excluded = _exclusions()["hooks"]
        for kind, cls in _flow_and_task_classes():
            names = {f"{kind}.{name}" for name in _hook_arguments(cls)}
            forwarded = {f"{kind}.{name}" for name in _SUPPORTED_HOOKS[kind]}
            missing = _unaccounted(names, forwarded, excluded)
            self.assertEqual(missing, [], f"{cls.__name__}: hooks unhandled")

    def test2(self) -> None:
        """
        Test that the installed Prefect accepts the plugin's functions for
        every hook they serve that it has.
        """
        prefect = importlib.import_module("prefect")
        for kind, cls in _flow_and_task_classes():
            if cls.__name__ == "MaterializingTask":
                continue
            hook = (
                cephooks.emit_flow_run
                if kind == "flow"
                else cephooks.emit_task_run
            )
            available = _SUPPORTED_HOOKS[kind] & _hook_arguments(cls)
            decorate = prefect.flow if kind == "flow" else prefect.task
            wired = decorate(**{name: [hook] for name in available})(_noop)
            self.assertIsNotNone(wired, kind)


def _noop() -> None:
    """
    A flow or task body that does nothing.

    :return: nothing
    """


# #############################################################################
# Test_fields1
# #############################################################################


@unittest.skipUnless(_HAS_PREFECT, _SKIP_REASON)
class Test_fields1(unittest.TestCase):
    """
    Test that every field of every forwarded object is in the contract or
    excluded.
    """

    def test1(self) -> None:
        """
        Test that each Prefect model's fields are forwarded or excluded.
        """
        paths = _contract_paths()
        excluded = _exclusions()["fields"]
        checked = 0
        for name, (module_name, places) in _MODELS.items():
            try:
                model = getattr(importlib.import_module(module_name), name)
            except (ImportError, AttributeError):
                # Not in this version: assets are Prefect 3.x only.
                continue
            checked += 1
            reasons = excluded.get(name, {})
            if reasons.get("*", "").strip():
                continue
            fields = set(_field_names(model))
            forwarded = {
                field
                for field in fields
                if any(f"{place}.{field}" in paths for place in places)
            }
            missing = _unaccounted(fields, forwarded, reasons)
            self.assertEqual(missing, [], f"{name}: fields unhandled")
        self.assertGreaterEqual(checked, 9)

    def test2(self) -> None:
        """
        Test that each attribute of a live flow and task is forwarded or
        excluded -- they are plain objects, not models, so their fields are
        what an instance carries.
        """
        prefect = importlib.import_module("prefect")
        paths = _contract_paths()
        excluded = _exclusions()["fields"]
        instances: List[Tuple[str, Any]] = [
            ("flow", prefect.flow(_noop)),
            ("task", prefect.task(_noop)),
        ]
        try:
            assets = importlib.import_module("prefect.assets")
            instances.append(
                ("task", assets.materialize("s3://bucket/key")(_noop))
            )
        except ImportError:
            pass
        for kind, instance in instances:
            names = {name for name in vars(instance) if not name.startswith("_")}
            forwarded = {name for name in names if f"{kind}.{name}" in paths}
            reasons = excluded.get(kind, {})
            missing = _unaccounted(names, forwarded, reasons)
            self.assertEqual(missing, [], f"{kind}: attributes unhandled")


# #############################################################################
# Test_events1
# #############################################################################


@unittest.skipUnless(_HAS_PREFECT, _SKIP_REASON)
class Test_events1(unittest.TestCase):
    """
    Test that every family of event Prefect emits is carried or excluded.
    """

    def test1(self) -> None:
        """
        Test that each event family named in Prefect's source is one a hook
        carries, or is excluded with a reason.
        """
        families = _event_families()
        self.assertIn("flow-run", families)
        missing = _unaccounted(
            set(families), set(_CARRIED_EVENTS), _exclusions()["events"]
        )
        self.assertEqual(missing, [], {name: families[name] for name in missing})


# #############################################################################
# Test_live_run1
# #############################################################################


@unittest.skipUnless(_HAS_PREFECT, _SKIP_REASON)
class Test_live_run1(unittest.TestCase):
    """
    Test the plugin against a real run on a throwaway Prefect server.
    """

    def test1(self) -> None:
        """
        Test that a real run fires every wired hook, withholds parameter
        values, forwards the assets a task materializes, and emits no event
        family that is neither carried nor excluded.
        """
        sent, events = _live_run()
        by_event: Dict[str, List[Dict[str, Any]]] = {}
        for observation in sent:
            by_event.setdefault(observation["event"], []).append(observation)
        self.assertIn("flow_run", by_event)
        states = {
            (item["event"], item["payload"]["state"]["name"]) for item in sent
        }
        self.assertIn(("flow_run", "Completed"), states)
        self.assertIn(("task_run", "Failed"), states)
        text = json.dumps([item["payload"] for item in sent])
        self.assertNotIn("customer-value", text)
        flow_run = by_event["flow_run"][-1]["payload"]["flow_run"]
        self.assertEqual(flow_run["parameters"], {"customer": "<str>"})
        if importlib.util.find_spec("prefect.assets") is not None:
            keys = {
                asset["key"]
                for item in by_event["task_run"]
                for asset in item["payload"]["task"].get("assets") or []
            }
            self.assertEqual(keys, {"s3://bucket/report.parquet"})
        families = {name.split(".")[1] for name in events}
        missing = _unaccounted(
            families, set(_CARRIED_EVENTS), _exclusions()["events"]
        )
        self.assertEqual(missing, [], sorted(events))


def _live_run() -> Tuple[List[Dict[str, Any]], Set[str]]:
    """
    Run a flow with the plugin wired in, against a throwaway server.

    Built here rather than at import, so the plain unit-test run never needs
    Prefect. What the hooks send is caught at `cemit.send_one`, which is the
    one place every event leaves through.

    :return: the observations the hooks sent, and the names of the events
        Prefect emitted from this process
    """
    # pylint: disable=too-many-locals
    prefect = importlib.import_module("prefect")
    utilities = importlib.import_module("prefect.testing.utilities")
    sent: List[Dict[str, Any]] = []
    events: Set[str] = set()

    def _record(**kwargs: Any) -> None:
        sent.append(kwargs)

    @prefect.task(on_completion=[cephooks.emit_task_run])
    def extract() -> List[int]:
        return [1, 2]

    @prefect.task(on_failure=[cephooks.emit_task_run])
    def audit(rows: List[int]) -> None:
        raise ValueError(f"audit failed on purpose after {len(rows)} rows")

    materialized: List[Any] = []
    try:
        assets = importlib.import_module("prefect.assets")

        @assets.materialize(
            "s3://bucket/report.parquet",
            asset_deps=["postgres://shop/public/orders"],
            on_completion=[cephooks.emit_task_run],
        )
        def report(rows: List[int]) -> None:
            del rows

        materialized.append(report)
    except ImportError:
        pass

    @prefect.flow(
        on_running=[cephooks.emit_flow_run],
        on_completion=[cephooks.emit_flow_run],
        on_failure=[cephooks.emit_flow_run],
        on_crashed=[cephooks.emit_flow_run],
        on_cancellation=[cephooks.emit_flow_run],
    )
    def nightly(customer: str = "default-value") -> int:
        del customer
        rows = extract()
        for task in materialized:
            task(rows)
        audit(rows, return_state=True)
        # Returned, so the flow completes: on 2.x a flow returning nothing
        # takes its state from its tasks, and one of them fails on purpose.
        return 1

    with (
        utilities.prefect_test_harness(),
        _events_recorded(events),
        mock.patch("convalesce_emit.send_one", side_effect=_record),
    ):
        nightly(customer="customer-value")
    return sent, events


class _events_recorded:  # pylint: disable=invalid-name
    """
    Catch the events Prefect emits from this process, by name.

    The asserting client Prefect's own tests use, where this version has it;
    where it does not, nothing is caught and only the source scan above
    speaks for this version's events.
    """

    def __init__(self, names: Set[str]) -> None:
        self._names = names
        self._patch: Any = None
        self._worker: Any = None

    def __enter__(self) -> "_events_recorded":
        try:
            worker_module = importlib.import_module("prefect.events.worker")
            clients = importlib.import_module("prefect.events.clients")
            worker = worker_module.EventsWorker.instance(
                clients.AssertingEventsClient
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.debug("no asserting events client: %s", exc)
            return self
        self._worker = worker
        self._patch = mock.patch.object(
            worker_module.EventsWorker,
            "instance",
            classmethod(lambda cls, *args, **kwargs: worker),
        )
        self._patch.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        if self._worker is None:
            return
        self._patch.stop()
        self._worker.drain()
        client = getattr(self._worker, "_client", None)
        for event in getattr(client, "events", []) or []:
            self._names.add(str(event.event))

"""
Tests for the run-status sensor body.
"""

import os
import sys
import types
import unittest
import unittest.mock
from typing import Any, Dict, List

import convalesce_emit_dagster.sensor as cedsens


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


class _Context:
    """The shape of a run-status context: private state, public properties."""

    def __init__(self) -> None:
        self._run = {"job_name": "nightly", "run_id": "abc"}
        self._event = {"event_type_value": "PIPELINE_FAILURE"}

    @property
    def dagster_run(self) -> Dict[str, str]:
        """The run, as Dagster exposes it."""
        return self._run

    @property
    def dagster_event(self) -> Dict[str, str]:
        """The event, as Dagster exposes it."""
        return self._event

    @property
    def sensor_name(self) -> str:
        """The sensor that fired."""
        return "convalesce_on_failure"

    @property
    def partition_key(self) -> str:
        """Raises, as Dagster's does for an unpartitioned run."""
        raise RuntimeError("not partitioned")


# #############################################################################
# Test_convalesce_sensor1
# #############################################################################


class Test_convalesce_sensor1(unittest.TestCase):
    """
    Test that a run-status context crosses as its run and event.
    """

    def test1(self) -> None:
        """
        Test that the run and event are forwarded, not the wrapper's repr.

        The context keeps its state private, so dumped whole it is a bare
        object description with no job name in it.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor(_Context(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["dagster_run"]["job_name"], "nightly")
        self.assertEqual(
            payload["dagster_event"]["event_type_value"], "PIPELINE_FAILURE"
        )
        self.assertEqual(payload["sensor_name"], "convalesce_on_failure")
        self.assertNotIn("context", payload)

    def test2(self) -> None:
        """
        Test that something that is not a context still crosses whole.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor({"job_name": "nightly"}, emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["context"]["job_name"], "nightly")


class _Instance:
    """Stands in for the DagsterInstance the context carries."""

    def __init__(self) -> None:
        self.asked: List[str] = []

    def get_job_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """The job's ops and their inputs and outputs."""
        self.asked.append(snapshot_id)
        return {"name": "nightly", "node_defs": [{"name": "extract"}]}

    def get_execution_plan_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """Which step depends on which."""
        self.asked.append(snapshot_id)
        return {"steps": [{"key": "load", "inputs": ["extract"]}]}

    def get_run_stats(self, run_id: str) -> Dict[str, Any]:
        """When the run began and ended."""
        self.asked.append(run_id)
        return {"run_id": run_id, "start_time": 1.0, "end_time": 2.0}

    def get_run_step_stats(self, run_id: str) -> List[Dict[str, Any]]:
        """When each step began and ended, and how it went."""
        self.asked.append(run_id)
        return [{"step_key": "extract", "status": "SUCCESS"}]


class _Run:
    """Stands in for a DagsterRun, which names what it points at."""

    def __init__(self) -> None:
        self.job_name = "nightly"
        self.run_id = "abc"
        self.job_snapshot_id = "snap-1"
        self.execution_plan_snapshot_id = "plan-1"


class _ReachableContext:
    """A run-status context with an instance behind it."""

    def __init__(self, instance: Any = None) -> None:
        self.instance = _Instance() if instance is None else instance

    @property
    def dagster_run(self) -> Any:
        """The run, as Dagster exposes it."""
        return _Run()

    @property
    def dagster_event(self) -> Dict[str, str]:
        """The event, as Dagster exposes it."""
        return {"event_type_value": "PIPELINE_SUCCESS"}

    @property
    def sensor_name(self) -> str:
        """The sensor that fired."""
        return "convalesce_on_success"


# #############################################################################
# Test_reach_instance1
# #############################################################################


class Test_reach_instance1(unittest.TestCase):
    """
    Test that what the run points at is read and forwarded.
    """

    def test1(self) -> None:
        """
        Test that the snapshots and the stats arrive with the run.

        The run and the event say a job ran and how it ended. What it is
        made of, when each step ran and which step feeds which is on the
        instance, one call away, and was left behind.
        """
        recorder = _Recorder()
        context = _ReachableContext()
        cedsens.convalesce_sensor(context, emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["job_snapshot"]["name"], "nightly")
        self.assertEqual(
            payload["execution_plan_snapshot"]["steps"][0]["key"], "load"
        )
        self.assertEqual(payload["run_stats"]["end_time"], 2.0)
        self.assertEqual(payload["step_stats"][0]["step_key"], "extract")
        # Each was asked for by the id the run named, not by guesswork.
        self.assertEqual(
            context.instance.asked, ["snap-1", "plan-1", "abc", "abc"]
        )

    def test2(self) -> None:
        """
        Test that a storage that cannot answer loses only that one part.
        """

        class Broken:
            """An instance whose storage is unreachable."""

            def get_job_snapshot(self, snapshot_id: str) -> Any:
                """Fail, the way a storage outage does."""
                raise RuntimeError("no storage")

            def get_run_stats(self, run_id: str) -> Dict[str, Any]:
                """Answer anyway."""
                return {"run_id": run_id}

        recorder = _Recorder()
        cedsens.convalesce_sensor(_ReachableContext(Broken()), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("job_snapshot", payload)
        self.assertEqual(payload["run_stats"]["run_id"], "abc")
        self.assertEqual(payload["dagster_run"]["job_name"], "nightly")

    def test3(self) -> None:
        """
        Test that a context with no instance still sends the run.

        Which is what a sensor built by hand in a test looks like.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor(_Context(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["dagster_run"]["job_name"], "nightly")
        self.assertNotIn("job_snapshot", payload)


# #############################################################################
# Test_event_log1
# #############################################################################


# The same five names the plan asks `_FROM_INSTANCE`'s event log read to
# filter on -- restated here, not imported from the module under test, so
# this stays a test of the public contract rather than the private constant.
_EVENT_TYPE_NAMES = (
    "ASSET_MATERIALIZATION",
    "ASSET_OBSERVATION",
    "HANDLED_OUTPUT",
    "LOADED_INPUT",
    "RESOURCE_INIT_SUCCESS",
)


def _fake_dagster_event_type() -> types.SimpleNamespace:
    """A stand-in `DagsterEventType` carrying the five names this reads."""
    return types.SimpleNamespace(**{name: name for name in _EVENT_TYPE_NAMES})


class Test_event_log1(unittest.TestCase):
    """
    Test that the run's event log is read, filtered and forwarded, with its
    author-written metadata redacted.
    """

    def test1(self) -> None:
        """
        Test that matching records cross, and each one's `metadata` is
        redacted rather than sent verbatim.

        `all_logs`, the call the plan was written against, is gone on
        Dagster 1.13; this is read through `get_records_for_run` instead,
        which both supported versions carry.
        """

        class Instance(_Instance):
            """An instance whose storage also answers for the event log."""

            def get_records_for_run(
                self, run_id: str, of_type: Any = None
            ) -> types.SimpleNamespace:
                """The event log, the shape `EventLogConnection` has."""
                self.asked.append(run_id)
                self.of_type = of_type
                record = {
                    "event_log_entry": {
                        "dagster_event": {
                            "event_specific_data": {
                                "materialization": {
                                    "asset_key": "orders",
                                    "metadata": {
                                        "row_count": 1000,
                                        "sample": "alice@x.com",
                                    },
                                }
                            }
                        }
                    }
                }
                return types.SimpleNamespace(records=[record])

        fake_module = types.SimpleNamespace(
            DagsterEventType=_fake_dagster_event_type()
        )
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, {"dagster": fake_module}):
            cedsens.convalesce_sensor(
                _ReachableContext(Instance()), emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        entry = payload["event_log"][0]
        materialization = entry["event_log_entry"]["dagster_event"][
            "event_specific_data"
        ]["materialization"]
        self.assertEqual(materialization["asset_key"], "orders")
        self.assertEqual(
            materialization["metadata"], {"redacted": True, "count": 2}
        )
        self.assertNotIn("alice@x.com", str(payload))
        excluded = recorder.sent[0]["excluded"]
        self.assertTrue(
            any(entry["reason"] == "sample redacted" for entry in excluded)
        )

    def test2(self) -> None:
        """
        Test that an instance with no event-log storage is left out cleanly.
        """

        class Instance(_Instance):
            """An instance that never learned `get_records_for_run`."""

        fake_module = types.SimpleNamespace(
            DagsterEventType=_fake_dagster_event_type()
        )
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, {"dagster": fake_module}):
            cedsens.convalesce_sensor(
                _ReachableContext(Instance()), emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("event_log", payload)

    def test3(self) -> None:
        """
        Test that Dagster's absence -- no `DagsterEventType` to resolve --
        is the same as nothing matching, not a raise.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor(_ReachableContext(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("event_log", payload)


# #############################################################################
# Test_asset_group_names1
# #############################################################################


class Test_asset_group_names1(unittest.TestCase):
    """
    Test that asset group names are read from the sensor's repository, not
    from the run or its event log.
    """

    def test1(self) -> None:
        """
        Test that every key a multi-asset definition owns gets its group,
        read once per definition rather than once per key.
        """

        class Key:
            """Stands in for an `AssetKey`."""

            def __init__(self, *path: str) -> None:
                self.path = list(path)

        class AssetsDef:
            """Stands in for one `AssetsDefinition`, multi-asset or not."""

            def __init__(self, groups: Dict[Any, str]) -> None:
                self.group_names_by_key = groups

        raw_key = Key("orders")
        agg_key = Key("daily", "totals")
        combined = AssetsDef({raw_key: "core", agg_key: "core"})

        class AssetGraph:
            """Stands in for the repository's asset graph."""

            assets_defs_by_key = {raw_key: combined, agg_key: combined}

        class Repository:
            """Stands in for the sensor's `RepositoryDefinition`."""

            asset_graph = AssetGraph()

        class Context:
            """A context exposing only what this reads."""

            repository_def = Repository()

        groups = cedsens.asset_group_names(Context())
        self.assertEqual(groups, {"orders": "core", "daily.totals": "core"})

    def test2(self) -> None:
        """
        Test that a context with no repository yields nothing, not a raise.
        """
        self.assertEqual(cedsens.asset_group_names(_Context()), {})

    def test3(self) -> None:
        """
        Test that an asset graph that cannot be read loses only the groups.
        """

        class Repository:
            """A repository whose asset graph is unreadable."""

            @property
            def asset_graph(self) -> Any:
                """Fail, the way an unloaded repository does."""
                raise RuntimeError("not loaded")

        class Context:
            """A context exposing only what this reads."""

            repository_def = Repository()

        self.assertEqual(cedsens.asset_group_names(Context()), {})


# #############################################################################
# Test_cloud_environment1
# #############################################################################


class Test_cloud_environment1(unittest.TestCase):
    """
    Test that Dagster Cloud's own environment is forwarded, minus anything
    that looks like a credential.
    """

    def test1(self) -> None:
        """
        Test that a documented Cloud variable crosses, and an unrelated one
        does not.
        """
        env = {
            "DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod",
            "DAGSTER_CLOUD_GIT_SHA": "abc123",
            "OTHER_VAR": "ignored",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=False):
            out = cedsens.cloud_environment()
        self.assertEqual(out.get("DAGSTER_CLOUD_DEPLOYMENT_NAME"), "prod")
        self.assertEqual(out.get("DAGSTER_CLOUD_GIT_SHA"), "abc123")
        self.assertNotIn("OTHER_VAR", out)

    def test2(self) -> None:
        """
        Test that a variable whose name suggests a credential never crosses,
        even though none of Dagster's own documented variables are one.
        """
        env = {"DAGSTER_CLOUD_API_TOKEN": "s3cret"}
        with unittest.mock.patch.dict(os.environ, env, clear=False):
            out = cedsens.cloud_environment()
        self.assertNotIn("DAGSTER_CLOUD_API_TOKEN", out)

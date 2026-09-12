"""
Tests for the run-status sensor body.
"""

import unittest
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

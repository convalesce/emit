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

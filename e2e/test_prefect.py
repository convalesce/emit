"""
Real flows run against a real Prefect server, with the hooks attached.

The probe calls one hook by hand. This lets Prefect itself call them, by
keyword on 2.x and positionally on 3.x, for flows and tasks, on completion
and on failure.

Run with `EMIT_E2E_PREFECT=<version> pytest test_prefect.py`.
"""

import logging
from typing import Dict, List

import harness as e2eharn

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_prefect1
# #############################################################################


class Test_prefect1(e2eharn.StackCase):
    """
    Test that flow and task hooks fire for both flows.
    """

    TOOL = "prefect"

    @classmethod
    def compose_env(cls, version: str) -> Dict[str, str]:
        major, _ = e2eharn.find_version_parts(version)
        python = "3.12" if major >= 3 else "3.11"
        return {
            "EMIT_E2E_PREFECT_IMAGE": f"prefecthq/prefect:{version}-python{python}"
        }

    def test1(self) -> None:
        """
        Test that a completed and a failed flow both arrive with their tasks.
        """
        with self.logs_on_failure():
            self.stack.up("receiver", "server")
            self.stack.run("flows")
            observations = self.stack.wait_for(
                self._all_four,
                timeout=60,
                what="flow and task hooks for both flows",
            )
            self.assert_envelopes(observations, "prefect")
            states = self._flow_states(observations)
            _LOG.info("flow states: %s", states)
            self.assertTrue(any("COMPLETED" in s for s in states), states)
            self.assertTrue(any("FAILED" in s for s in states), states)
            self._assert_task_runs_name_their_flow(observations)

    def _assert_task_runs_name_their_flow(
        self, observations: List[e2eharn.Observation]
    ) -> None:
        """
        Check a task event says which flow and which run it belongs to.

        A task run names its flow run by id and nothing else, and the flow
        hook that carries the name fires last, after every task hook, so a
        receiver had to hold task runs back until the flow arrived.

        :param observations: what the receiver recorded
        :return: nothing
        """
        seen = 0
        for observation in observations:
            if observation["event"] != "task_run":
                continue
            payload = observation["payload"]
            flow = payload.get("flow")
            self.assertIsInstance(
                flow, dict, f"no flow on a task run: {sorted(payload)}"
            )
            self.assertTrue(flow.get("name"), flow)
            flow_run = payload.get("flow_run")
            self.assertIsInstance(
                flow_run, dict, f"no flow run on a task run: {sorted(payload)}"
            )
            self.assertTrue(flow_run.get("name"), flow_run)
            # The run it names must be the one the task run points at.
            self.assertEqual(
                flow_run.get("id"), payload["task_run"].get("flow_run_id")
            )
            seen += 1
        self.assertTrue(seen, "no task runs arrived")

    @staticmethod
    def _all_four(observations: List[e2eharn.Observation]) -> bool:
        """
        Whether both flows and both hook kinds have arrived.

        :param observations: what the receiver recorded
        :return: whether to stop waiting
        """
        text = e2eharn.wire(observations)
        return (
            {"flow_run", "task_run"} <= set(e2eharn.events(observations))
            and "nightly_ok" in text
            and "nightly_broken" in text
            and "load failed on purpose" in text
        )

    @staticmethod
    def _flow_states(observations: List[e2eharn.Observation]) -> List[str]:
        """
        The state each flow run ended in.

        :param observations: what the receiver recorded
        :return: upper-cased state names
        """
        states = []
        for obs in observations:
            if obs["event"] != "flow_run":
                continue
            state = obs["payload"]["state"]
            states.append(str(state.get("type") or state.get("name")).upper())
        return sorted(states)

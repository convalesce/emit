"""
A real Airflow runs the example DAG with the plugin installed.

The in-process probe in CI proves the hookspecs match. This proves the rest:
the plugin is discovered through the `airflow.plugins` entry point, fires in
the scheduler's own task processes, and what it sends reaches the endpoint
with the key.

Run with `EMIT_E2E_AIRFLOW=<version> pytest test_airflow.py`.
"""

import logging
import time
from typing import Any, Dict, List

import harness as e2eharn

_LOG = logging.getLogger(__name__)

DAG_ID = "convalesce_example"
# Where a DAG turns up, in the order it is preferred. Airflow 2 hangs it on
# the task and on the dag run; Airflow 3's task instance carries neither, so
# it comes off the task's own `dag` property. A task group holds a copy too,
# and that is the last resort rather than the first.
_DAG_PATHS = (
    ("task", "dag"),
    ("dag_run", "dag"),
    ("task", "task_group", "dag"),
)
# Proves the scheduler has parsed the DAG itself. On Airflow 2.9 and later
# the CLI's `unpause` parses the file and writes the row on its own, and a
# row the CLI wrote before the scheduler's first parse left every run it
# created unscheduled on a slow runner. Only the scheduler writes here.
SERIALIZED_CHECK = (
    "import sys; "
    "from airflow.models.serialized_dag import SerializedDagModel as S; "
    f"sys.exit(0 if S.has_dag({DAG_ID!r}) else 1)"
)
TASK_EVENTS = {
    "on_task_instance_running",
    "on_task_instance_success",
    "on_task_instance_failed",
}


def find_dag(task_instance: Dict[str, Any]) -> Any:
    """
    The DAG a task event names, wherever this Airflow put it.

    :param task_instance: the dumped task instance
    :return: the DAG as a mapping, or None when it named none
    """
    for path in _DAG_PATHS:
        found: Any = task_instance
        for name in path:
            found = found.get(name) if isinstance(found, dict) else None
        if isinstance(found, dict) and found.get("dag_id"):
            return found
    return None


# #############################################################################
# Test_airflow1
# #############################################################################


class Test_airflow1(e2eharn.StackCase):
    """
    Test that the example DAG's events reach the endpoint.
    """

    TOOL = "airflow"

    @classmethod
    def compose_env(cls, version: str) -> Dict[str, str]:
        # The bare tag's Python moves with the release, and 2.5's is 3.7,
        # below the floor the packages declare. Name the Python the CI
        # matrix uses.
        major_minor = e2eharn.find_version_parts(version)
        if major_minor >= (3, 0):
            python = "3.12"
        elif major_minor >= (2, 9):
            python = "3.11"
        else:
            python = "3.10"
        image = f"apache/airflow:{version}-python{python}"
        return {"EMIT_E2E_AIRFLOW_IMAGE": image}

    def test1(self) -> None:
        """
        Test that task and dag-run events arrive from a real scheduler.

        The task hooks fire in the process Airflow forks per task, the
        dag-run hooks in the scheduler itself; both must find their way
        out. The failure message is checked only from 2.10, which is when
        Airflow started passing it to listeners.
        """
        with self.logs_on_failure():
            self.stack.up()
            self._wait_until_scheduler_alive()
            self._wait_until_serialized()
            self.stack.exec("airflow", "airflow", "dags", "unpause", DAG_ID)
            self.stack.exec("airflow", "airflow", "dags", "trigger", DAG_ID)
            observations = self.stack.wait_for(
                lambda obs: TASK_EVENTS <= set(e2eharn.events(obs)),
                timeout=300,
                what=f"task events {sorted(TASK_EVENTS)}",
            )
            self.assert_envelopes(observations, "airflow")
            self.assertIn(DAG_ID, e2eharn.wire(observations))
            self._assert_run_is_named(observations)
            self._assert_session_absent(observations)
            if e2eharn.find_version_parts(self.version) >= (2, 10):
                self._assert_failure_forwarded(observations)
            observations = self.stack.wait_for(
                lambda obs: "on_dag_run_failed" in e2eharn.events(obs),
                timeout=120,
                what="on_dag_run_failed from the scheduler",
            )
            _LOG.info("events: %s", e2eharn.events(observations))

    def _wait_until_scheduler_alive(self, timeout: float = 300) -> None:
        """
        Wait for a scheduler heartbeat, the documented liveness check.

        :param timeout: seconds to wait
        :return: nothing
        :raises AssertionError: if no scheduler ever heartbeats
        """
        self._poll(
            ("airflow", "jobs", "check", "--job-type", "SchedulerJob"),
            timeout,
            "a scheduler heartbeat",
        )

    def _wait_until_serialized(self, timeout: float = 240) -> None:
        """
        Wait until the scheduler's own parse has serialized the DAG.

        :param timeout: seconds to wait
        :return: nothing
        :raises AssertionError: if the DAG is never serialized
        """
        self._poll(
            ("python", "-c", SERIALIZED_CHECK),
            timeout,
            f"{DAG_ID} in the serialized_dag table",
        )

    def _poll(
        self, command: "tuple[str, ...]", timeout: float, what: str
    ) -> None:
        """
        Run a command in the Airflow container until it exits zero.

        :param command: what to run
        :param timeout: seconds to keep trying
        :param what: named in the failure
        :return: nothing
        :raises AssertionError: if the timeout passes first
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.stack.exec("airflow", *command, check=False)
            if result.returncode == 0:
                return
            time.sleep(3)
        raise AssertionError(
            f"timed out after {timeout:.0f}s waiting for {what}"
        )

    def _assert_run_is_named(
        self, observations: List[e2eharn.Observation]
    ) -> None:
        """
        Check a task event names the run it belongs to, and the DAG.

        A receiver builds the job from the DAG and the run from the dag run.
        On Airflow 3 the task instance carries neither: the DAG sits behind
        a property on the task and the run on the context the API server
        sent.

        Airflow 2.5 hands the failed hook a task instance with no task on it
        at all, so there is no DAG to find on that one event. That is the
        tool's own doing, so the DAG is required of every event that carries
        a task, and of at least one event, rather than of every event.

        :param observations: what the receiver recorded
        :return: nothing
        """
        named = 0
        for observation in observations:
            if not observation["event"].startswith("on_task_instance"):
                continue
            task_instance = observation["payload"]["task_instance"]
            self.assertIsInstance(task_instance, dict, observation["payload"])
            dag_run = task_instance.get("dag_run")
            self.assertIsInstance(
                dag_run,
                dict,
                f"no dag run on {observation['event']}; the task instance "
                f"carried {sorted(task_instance)}",
            )
            self.assertTrue(dag_run.get("run_id"), dag_run)
            task = task_instance.get("task")
            if not isinstance(task, dict):
                _LOG.info(
                    "%s carried no task, so no dag either: %s",
                    observation["event"],
                    sorted(task_instance),
                )
                continue
            dag = find_dag(task_instance)
            self.assertIsInstance(
                dag,
                dict,
                f"no dag on {observation['event']}; its task carried "
                f"{sorted(task)}",
            )
            self.assertEqual(dag.get("dag_id"), DAG_ID, dag)
            named += 1
        self.assertTrue(named, "no task event named the DAG")

    def _assert_session_absent(
        self, observations: List[e2eharn.Observation]
    ) -> None:
        """
        Check the ORM session is not on the wire.

        Airflow 2 passes the scheduler's SQLAlchemy session to every task
        hook. It says nothing about the run and cost a few hundred values of
        machinery per event.

        :param observations: what the receiver recorded
        :return: nothing
        """
        for observation in observations:
            self.assertNotIn("session", observation["payload"])

    def _assert_failure_forwarded(
        self, observations: List[e2eharn.Observation]
    ) -> None:
        """
        Check the failed task's error reached the wire.

        :param observations: what the receiver recorded
        :return: nothing
        """
        failed = [
            obs
            for obs in observations
            if obs["event"] == "on_task_instance_failed"
        ]
        self.assertTrue(
            any(obs["payload"].get("error") for obs in failed),
            "error was declared but arrived empty",
        )
        self.assertIn("load failed on purpose", e2eharn.wire(observations))

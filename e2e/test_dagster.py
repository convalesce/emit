"""
A real Dagster daemon ticks the run-status sensors that call the plugin.

The probe calls `convalesce_sensor` by hand. This launches two jobs through
the web server, the way the UI does, and lets the daemon's sensor
evaluation, in its own code-server process, find the runs and forward them.

Run with `EMIT_E2E_DAGSTER=<version> pytest test_dagster.py`.
"""

import json
import logging
import time
import urllib.request
from typing import Any, Dict

import harness as e2eharn

_LOG = logging.getLogger(__name__)

REPOSITORIES_QUERY = """
  { repositoriesOrError { ... on RepositoryConnection {
      nodes { name location { name } } } } }
"""
LAUNCH_MUTATION = """
  mutation($params: ExecutionParams!) {
    launchRun(executionParams: $params) {
      __typename
      ... on LaunchRunSuccess { run { runId } }
      ... on PythonError { message }
      ... on RunConfigValidationInvalid { errors { message } }
    }
  }
"""


# #############################################################################
# Test_dagster1
# #############################################################################


class Test_dagster1(e2eharn.StackCase):
    """
    Test that run-status sensors forward both jobs' runs.
    """

    TOOL = "dagster"

    @classmethod
    def compose_env(cls, version: str) -> Dict[str, str]:
        # Dagster before 1.9 does not resolve on Python 3.12.
        python = (
            "3.12" if e2eharn.find_version_parts(version) >= (1, 9) else "3.11"
        )
        return {
            "EMIT_E2E_DAGSTER": version,
            "EMIT_E2E_PYTHON_IMAGE": f"python:{python}-slim",
        }

    def test1(self) -> None:
        """
        Test that a success and a failure each arrive as run and event.

        The context wrapper must not cross as its repr: the run has to
        carry the job name and the event the failure.
        """
        with self.logs_on_failure():
            self.stack.up()
            selector = self._wait_for_code_location()
            self._launch(selector, "nightly_ok")
            self._launch(selector, "nightly_broken")
            observations = self.stack.wait_for(
                lambda obs: "nightly_ok" in e2eharn.wire(obs)
                and "nightly_broken" in e2eharn.wire(obs),
                timeout=240,
                what="run_status for both jobs",
            )
            self.assert_envelopes(observations, "dagster")
            for observation in observations:
                self.assertEqual(observation["event"], "run_status")
                run = observation["payload"]["dagster_run"]
                self.assertIsInstance(run, dict, observation["payload"])
                self.assertTrue(run.get("job_name"), observation["payload"])
                self.assertIsInstance(
                    observation["payload"]["dagster_event"], dict
                )
            self.assertIn("load failed on purpose", e2eharn.wire(observations))

    def _graphql(self, query: str, variables: Dict[str, Any]) -> Any:
        """
        Post one query to the web server.

        :param query: the GraphQL document
        :param variables: its variables
        :return: the `data` of the response
        """
        port = e2eharn.optional("EMIT_E2E_DAGSTER_PORT", "13000")
        body = json.dumps({"query": query, "variables": variables})
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/graphql",
            data=body.encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)["data"]

    def _wait_for_code_location(self, timeout: float = 180) -> Dict[str, str]:
        """
        Wait until the code location has loaded.

        :param timeout: seconds to wait
        :return: the repository selector to launch runs with
        :raises AssertionError: if it never loads
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = self._graphql(REPOSITORIES_QUERY, {})
                nodes = data["repositoriesOrError"]["nodes"]
            except Exception:  # pylint: disable=broad-exception-caught
                # The web server is not answering yet.
                nodes = []
            if nodes:
                return {
                    "repositoryLocationName": nodes[0]["location"]["name"],
                    "repositoryName": nodes[0]["name"],
                }
            time.sleep(3)
        raise AssertionError("the code location never loaded")

    def _launch(self, selector: Dict[str, str], job: str) -> str:
        """
        Launch one run through the web server.

        :param selector: which repository the job lives in
        :param job: the job name
        :return: the run id
        """
        params = {"selector": {**selector, "jobName": job}, "runConfigData": {}}
        result = self._graphql(LAUNCH_MUTATION, {"params": params})["launchRun"]
        self.assertEqual(result["__typename"], "LaunchRunSuccess", result)
        run_id = str(result["run"]["runId"])
        _LOG.info("launched %s as %s", job, run_id)
        return run_id

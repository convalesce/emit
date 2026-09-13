"""
Tests for the Dagster retry executor.

Every test below reaches into a private helper on purpose: the module
under test is built around small, individually testable seams, and
exercising each directly is how the fail-closed behaviour on every one of
them gets proven rather than only asserted through the one public
entrypoint.

Run with `make test`.
"""

# pylint: disable=protected-access

import http.server
import json
import logging
import threading
import unittest
import unittest.mock
from typing import Any, Dict, List

import convalesce_emit as cemit
import convalesce_emit.retry as ceretry
import convalesce_emit_dagster.retry as cedagretry

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_normalize_host1
# #############################################################################


class Test_normalize_host1(unittest.TestCase):
    """
    Test turning a configured host into a base URL.
    """

    def test1(self) -> None:
        """Test that a host already carrying a scheme keeps it."""
        self.assertEqual(
            cedagretry._normalize_host("https://dagster.internal:3000/"),
            "https://dagster.internal:3000",
        )

    def test2(self) -> None:
        """Test that a bare host is assumed to be plaintext http."""
        self.assertEqual(
            cedagretry._normalize_host("dagster-webserver:3000"),
            "http://dagster-webserver:3000",
        )


# #############################################################################
# Test_read_target1
# #############################################################################


class Test_read_target1(unittest.TestCase):
    """
    Test resolving the retry credential from its two env vars.
    """

    def test1(self) -> None:
        """Test that both variables set resolves to a target."""
        with unittest.mock.patch.dict(
            "os.environ",
            {
                "CONVALESCE_DAGSTER_RETRY_HOST": "http://dagster:3000",
                "CONVALESCE_DAGSTER_RETRY_TOKEN": "s3cret",
            },
        ):
            target = cedagretry._read_target()
        assert target is not None
        self.assertEqual(target.base_url, "http://dagster:3000")
        self.assertEqual(target.token, "s3cret")

    def test2(self) -> None:
        """Test that a missing host fails closed."""
        with unittest.mock.patch.dict(
            "os.environ",
            {"CONVALESCE_DAGSTER_RETRY_TOKEN": "s3cret"},
            clear=True,
        ):
            self.assertIsNone(cedagretry._read_target())

    def test3(self) -> None:
        """Test that a missing token fails closed."""
        with unittest.mock.patch.dict(
            "os.environ",
            {"CONVALESCE_DAGSTER_RETRY_HOST": "http://dagster:3000"},
            clear=True,
        ):
            self.assertIsNone(cedagretry._read_target())


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder(http.server.BaseHTTPRequestHandler):
    """Answers a configured response and records what it was asked."""

    requests: List[Dict[str, Any]] = []
    status = 200
    body = b"{}"

    def do_POST(self) -> None:  # pylint: disable=invalid-name
        """Record one POST and answer with the configured response."""
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b""
        type(self).requests.append(
            {
                "path": self.path,
                "headers": dict(self.headers),
                "body": json.loads(raw) if raw else None,
            }
        )
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(type(self).body)

    def log_message(self, *_args: Any) -> None:
        """Silence the default stderr logging."""


class _ServerCase(unittest.TestCase):
    """A local HTTP server for the duration of one test."""

    def setUp(self) -> None:
        _Recorder.requests = []
        _Recorder.status = 200
        _Recorder.body = b"{}"
        self._httpd = http.server.HTTPServer(("127.0.0.1", 0), _Recorder)
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()
        self._target = cedagretry._RetryTarget(
            base_url=f"http://127.0.0.1:{self._httpd.server_address[1]}",
            token="dg-token",
        )

    def tearDown(self) -> None:
        self._httpd.shutdown()


# #############################################################################
# Test_call_graphql1
# #############################################################################


class Test_call_graphql1(_ServerCase):
    """
    Test the real HTTP call every GraphQL request makes.
    """

    def test1(self) -> None:
        """
        Test a well-formed response: the right URL, the bearer token, and
        the query/variables sent verbatim.
        """
        _Recorder.body = json.dumps({"data": {"ok": True}}).encode("utf-8")
        data = cedagretry._call_graphql(
            self._target, "query { x }", {"a": 1}, timeout=2.0
        )
        self.assertEqual(data, {"ok": True})
        request = _Recorder.requests[0]
        self.assertEqual(request["path"], "/graphql")
        self.assertEqual(request["headers"]["Authorization"], "Bearer dg-token")
        self.assertEqual(
            request["body"], {"query": "query { x }", "variables": {"a": 1}}
        )

    def test2(self) -> None:
        """
        Test that a top-level `errors` array raises, not returns partial
        data silently.
        """
        _Recorder.body = json.dumps({"errors": [{"message": "boom"}]}).encode(
            "utf-8"
        )
        with self.assertRaises(RuntimeError):
            cedagretry._call_graphql(
                self._target, "query { x }", {}, timeout=2.0
            )

    def test3(self) -> None:
        """
        Test that a response with no `data` object raises rather than
        letting a caller read None fields silently.
        """
        _Recorder.body = b"{}"
        with self.assertRaises(RuntimeError):
            cedagretry._call_graphql(
                self._target, "query { x }", {}, timeout=2.0
            )


# #############################################################################
# Test_run_exists_locally1
# #############################################################################


class Test_run_exists_locally1(unittest.TestCase):
    """
    Test the local-scope check against the webserver's own runOrError.
    """

    def _target(self) -> cedagretry._RetryTarget:
        return cedagretry._RetryTarget(base_url="http://dg", token="t")

    def test1(self) -> None:
        """Test that a real Run resolves to True."""
        with unittest.mock.patch.object(
            cedagretry,
            "_call_graphql",
            return_value={"runOrError": {"__typename": "Run", "runId": "r1"}},
        ):
            self.assertTrue(
                cedagretry.run_exists_locally(self._target(), "r1", timeout=1.0)
            )

    def test2(self) -> None:
        """Test that RunNotFoundError is False, not an error."""
        with unittest.mock.patch.object(
            cedagretry,
            "_call_graphql",
            return_value={
                "runOrError": {
                    "__typename": "RunNotFoundError",
                    "message": "no such run",
                }
            },
        ):
            self.assertFalse(
                cedagretry.run_exists_locally(
                    self._target(), "missing", timeout=1.0
                )
            )

    def test3(self) -> None:
        """Test that any exception fails closed to False, not raises."""
        with unittest.mock.patch.object(
            cedagretry, "_call_graphql", side_effect=Exception("unreachable")
        ):
            self.assertFalse(
                cedagretry.run_exists_locally(self._target(), "r1", timeout=1.0)
            )


# #############################################################################
# Test_reexecute_run1
# #############################################################################


class Test_reexecute_run1(unittest.TestCase):
    """
    Test parsing the reexecution mutation's result.
    """

    def _target(self) -> cedagretry._RetryTarget:
        return cedagretry._RetryTarget(base_url="http://dg", token="t")

    def test1(self) -> None:
        """Test that LaunchRunSuccess returns the new run's id."""
        with unittest.mock.patch.object(
            cedagretry,
            "_call_graphql",
            return_value={
                "launchPipelineReexecution": {
                    "__typename": "LaunchRunSuccess",
                    "run": {"runId": "r2", "status": "STARTED"},
                }
            },
        ) as call:
            new_run_id = cedagretry._reexecute_run(
                self._target(), "r1", timeout=1.0
            )
        self.assertEqual(new_run_id, "r2")
        variables = call.call_args[0][2]
        self.assertEqual(
            variables,
            {
                "reexecutionParams": {
                    "parentRunId": "r1",
                    "strategy": "FROM_FAILURE",
                }
            },
        )

    def test2(self) -> None:
        """Test that anything but LaunchRunSuccess raises."""
        with unittest.mock.patch.object(
            cedagretry,
            "_call_graphql",
            return_value={
                "launchPipelineReexecution": {
                    "__typename": "RunNotFoundError",
                    "message": "no such run",
                }
            },
        ):
            with self.assertRaises(RuntimeError):
                cedagretry._reexecute_run(self._target(), "r1", timeout=1.0)


# #############################################################################
# Test_run_pending_retries1
# #############################################################################


def _candidate(**coords: str) -> ceretry.RetryCandidate:
    """One dagster-tool candidate, coordinates as given."""
    return ceretry.RetryCandidate(
        remedy_id="r1",
        incident_urn="urn:li:incident:1",
        tool="dagster",
        coordinates=coords,
        created_at_millis=1,
    )


class Test_run_pending_retries1(unittest.TestCase):
    """
    Test the orchestration in `run_pending_retries`, with collect and the
    webserver's own GraphQL API both mocked out.
    """

    def test1(self) -> None:
        """
        Test that a run_id this webserver does not recognize is skipped
        without ever calling claim().
        """
        candidate = _candidate(run_id="not-mine")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cedagretry,
                "_read_target",
                return_value=cedagretry._RetryTarget(
                    base_url="http://dg", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cedagretry, "run_exists_locally", return_value=False
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = cedagretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual(summary.considered, 1)
        self.assertEqual(summary.skipped, 1)

    def test2(self) -> None:
        """
        Test the full happy path: local scope passes, the claim
        succeeds, re-execution succeeds, and TRIGGERED is reported.
        """
        candidate = _candidate(run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cedagretry,
                "_read_target",
                return_value=cedagretry._RetryTarget(
                    base_url="http://dg", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cedagretry, "run_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(
                cedagretry, "_reexecute_run", return_value="r2"
            ),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = cedagretry.run_pending_retries(config=cemit.Config())
        report.assert_called_once_with(
            "r1",
            outcome=ceretry.TRIGGERED,
            native_run_ref="r2",
            config=unittest.mock.ANY,
        )
        self.assertEqual(
            (summary.claimed, summary.triggered, summary.failed), (1, 1, 0)
        )

    def test3(self) -> None:
        """
        Test that a re-execution call that raises is reported as
        FAILED_TO_TRIGGER.
        """
        candidate = _candidate(run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cedagretry,
                "_read_target",
                return_value=cedagretry._RetryTarget(
                    base_url="http://dg", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cedagretry, "run_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(
                cedagretry,
                "_reexecute_run",
                side_effect=RuntimeError("launchPipelineReexecution failed"),
            ),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = cedagretry.run_pending_retries(config=cemit.Config())
        report.assert_called_once_with(
            "r1",
            outcome=ceretry.FAILED_TO_TRIGGER,
            error=unittest.mock.ANY,
            config=unittest.mock.ANY,
        )
        self.assertEqual(
            (summary.claimed, summary.triggered, summary.failed), (1, 0, 1)
        )

    def test4(self) -> None:
        """Test that no retry credential skips every candidate."""
        candidate = _candidate(run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cedagretry, "_read_target", return_value=None
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = cedagretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual((summary.considered, summary.skipped), (1, 1))

    def test5(self) -> None:
        """Test that a lost claim race is skipped, not reported."""
        candidate = _candidate(run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cedagretry,
                "_read_target",
                return_value=cedagretry._RetryTarget(
                    base_url="http://dg", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cedagretry, "run_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=False),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = cedagretry.run_pending_retries(config=cemit.Config())
        report.assert_not_called()
        self.assertEqual((summary.claimed, summary.skipped), (0, 1))

    def test6(self) -> None:
        """Test that a tool-mismatched candidate is never considered."""
        candidate = ceretry.RetryCandidate(
            remedy_id="r2",
            incident_urn="urn:li:incident:2",
            tool="airflow",
            coordinates={"dag_id": "x"},
            created_at_millis=1,
        )
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = cedagretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual(summary.considered, 0)

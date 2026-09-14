"""
Tests for the Prefect retry executor.

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
import convalesce_emit_prefect.retry as ceprefretry

_LOG = logging.getLogger(__name__)


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
                "CONVALESCE_PREFECT_RETRY_API_URL": "http://prefect:4200/api/",
                "CONVALESCE_PREFECT_RETRY_API_KEY": "s3cret",
            },
        ):
            target = ceprefretry._read_target()
        assert target is not None
        self.assertEqual(target.base_url, "http://prefect:4200/api")
        self.assertEqual(target.api_key, "s3cret")

    def test2(self) -> None:
        """Test that a missing url fails closed."""
        with unittest.mock.patch.dict(
            "os.environ",
            {"CONVALESCE_PREFECT_RETRY_API_KEY": "s3cret"},
            clear=True,
        ):
            self.assertIsNone(ceprefretry._read_target())

    def test3(self) -> None:
        """Test that a missing key fails closed."""
        with unittest.mock.patch.dict(
            "os.environ",
            {"CONVALESCE_PREFECT_RETRY_API_URL": "http://prefect:4200/api"},
            clear=True,
        ):
            self.assertIsNone(ceprefretry._read_target())


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder(http.server.BaseHTTPRequestHandler):
    """Answers a configured response and records what it was asked."""

    requests: List[Dict[str, Any]] = []
    status = 200
    body = b"{}"

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        """Record and answer a GET."""
        self._handle()

    def do_POST(self) -> None:  # pylint: disable=invalid-name
        """Record and answer a POST."""
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b""
        type(self).requests.append(
            {
                "method": self.command,
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
        self._target = ceprefretry._RetryTarget(
            base_url=f"http://127.0.0.1:{self._httpd.server_address[1]}",
            api_key="pf-token",
        )

    def tearDown(self) -> None:
        self._httpd.shutdown()


# #############################################################################
# Test_flow_run_is_reschedulable1
# #############################################################################


class Test_flow_run_is_reschedulable1(_ServerCase):
    """
    Test the local-scope check against a real flow run read.
    """

    def test1(self) -> None:
        """
        Test that a flow run with a deployment_id is reschedulable, and
        that the bearer token and path both cross correctly.
        """
        _Recorder.body = json.dumps(
            {"id": "r1", "deployment_id": "d1", "state": {"type": "FAILED"}}
        ).encode("utf-8")
        self.assertTrue(
            ceprefretry.flow_run_is_reschedulable(
                self._target, "r1", timeout=2.0
            )
        )
        request = _Recorder.requests[0]
        self.assertEqual(request["method"], "GET")
        self.assertEqual(request["path"], "/flow_runs/r1")
        self.assertEqual(request["headers"]["Authorization"], "Bearer pf-token")

    def test2(self) -> None:
        """
        Test that a flow run with no deployment_id -- nothing would ever
        pick up a SCHEDULED state -- is not reschedulable.
        """
        _Recorder.body = json.dumps({"id": "r1", "deployment_id": None}).encode(
            "utf-8"
        )
        with self.assertLogs("convalesce_emit_prefect.retry", level="INFO"):
            self.assertFalse(
                ceprefretry.flow_run_is_reschedulable(
                    self._target, "r1", timeout=2.0
                )
            )

    def test3(self) -> None:
        """
        Test that a 404 fails closed to False, not raises.
        """
        _Recorder.status = 404
        self.assertFalse(
            ceprefretry.flow_run_is_reschedulable(
                self._target, "missing", timeout=2.0
            )
        )

    def test4(self) -> None:
        """
        Test that an unreachable endpoint fails closed to False.
        """
        target = ceprefretry._RetryTarget(
            base_url="http://127.0.0.1:1", api_key="k"
        )
        with self.assertLogs("convalesce_emit_prefect.retry", level="WARNING"):
            self.assertFalse(
                ceprefretry.flow_run_is_reschedulable(target, "r1", timeout=1.0)
            )


# #############################################################################
# Test_reschedule_flow_run1
# #############################################################################


class Test_reschedule_flow_run1(_ServerCase):
    """
    Test the real HTTP call that reschedules a flow run.
    """

    def test1(self) -> None:
        """
        Test an ACCEPTed transition: the right path, a SCHEDULED body
        naming this exact flow run, and force=False.

        A real Prefect 3.8.5 server answers an ACCEPTed `set_state` with
        HTTP 201, not 200 -- confirmed live against a real server (see
        `plan/04-retry-remedy-kind.md`'s closing state section). The fake
        server here is set to 201 on purpose, not the class default of
        200, so this test would have caught the real bug that shipped
        (every real success misreported as `FAILED_TO_TRIGGER`) instead
        of passing against a response shape no real server sends.
        """
        _Recorder.status = 201
        _Recorder.body = json.dumps(
            {"status": "ACCEPT", "state": {"type": "SCHEDULED"}}
        ).encode("utf-8")
        ref = ceprefretry._reschedule_flow_run(self._target, "r1", timeout=2.0)
        self.assertEqual(ref, "r1")
        request = _Recorder.requests[0]
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["path"], "/flow_runs/r1/set_state")
        self.assertEqual(request["body"]["force"], False)
        self.assertEqual(request["body"]["state"]["type"], "SCHEDULED")
        self.assertEqual(
            request["body"]["state"]["state_details"]["flow_run_id"], "r1"
        )

    def test2(self) -> None:
        """
        Test that a REJECTed transition raises rather than being treated
        as success.

        A real server answers a REJECT/ABORT with HTTP 200 (confirmed
        live against a real Prefect 3.8.5 server calling `set_state` on a
        terminal, no-deployment flow run), so this is set explicitly
        rather than left at the class default to document that fact
        rather than merely benefit from it.
        """
        _Recorder.status = 200
        _Recorder.body = json.dumps(
            {"status": "REJECT", "details": {"reason": "not schedulable"}}
        ).encode("utf-8")
        with self.assertRaises(RuntimeError):
            ceprefretry._reschedule_flow_run(self._target, "r1", timeout=2.0)

    def test3(self) -> None:
        """
        Test that a 404 raises -- the caller turns this into
        FAILED_TO_TRIGGER, this function itself never swallows it.
        """
        _Recorder.status = 404
        with self.assertRaises(RuntimeError):
            ceprefretry._reschedule_flow_run(self._target, "r1", timeout=2.0)


# #############################################################################
# Test_run_pending_retries1
# #############################################################################


def _candidate(**coords: str) -> ceretry.RetryCandidate:
    """One prefect-tool candidate, coordinates as given."""
    return ceretry.RetryCandidate(
        remedy_id="r1",
        incident_urn="urn:li:incident:1",
        tool="prefect",
        coordinates=coords,
        created_at_millis=1,
    )


class Test_run_pending_retries1(unittest.TestCase):
    """
    Test the orchestration in `run_pending_retries`, with collect and the
    Prefect API both mocked out.
    """

    def test1(self) -> None:
        """
        Test that a flow run with no deployment is skipped without ever
        calling claim().
        """
        candidate = _candidate(flow_run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                ceprefretry,
                "_read_target",
                return_value=ceprefretry._RetryTarget(
                    base_url="http://pf", api_key="k"
                ),
            ),
            unittest.mock.patch.object(
                ceprefretry, "flow_run_is_reschedulable", return_value=False
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = ceprefretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual(summary.considered, 1)
        self.assertEqual(summary.skipped, 1)

    def test2(self) -> None:
        """
        Test the full happy path: reschedulable, claimed, accepted, and
        TRIGGERED is reported with the same flow_run_id as the ref.
        """
        candidate = _candidate(flow_run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                ceprefretry,
                "_read_target",
                return_value=ceprefretry._RetryTarget(
                    base_url="http://pf", api_key="k"
                ),
            ),
            unittest.mock.patch.object(
                ceprefretry, "flow_run_is_reschedulable", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(
                ceprefretry, "_reschedule_flow_run", return_value="r1"
            ),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = ceprefretry.run_pending_retries(config=cemit.Config())
        report.assert_called_once_with(
            "r1",
            outcome=ceretry.TRIGGERED,
            native_run_ref="r1",
            config=unittest.mock.ANY,
        )
        self.assertEqual(
            (summary.claimed, summary.triggered, summary.failed), (1, 1, 0)
        )

    def test3(self) -> None:
        """
        Test that a reschedule call that raises is reported as
        FAILED_TO_TRIGGER.
        """
        candidate = _candidate(flow_run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                ceprefretry,
                "_read_target",
                return_value=ceprefretry._RetryTarget(
                    base_url="http://pf", api_key="k"
                ),
            ),
            unittest.mock.patch.object(
                ceprefretry, "flow_run_is_reschedulable", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(
                ceprefretry,
                "_reschedule_flow_run",
                side_effect=RuntimeError("set_state returned REJECT"),
            ),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = ceprefretry.run_pending_retries(config=cemit.Config())
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
        candidate = _candidate(flow_run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                ceprefretry, "_read_target", return_value=None
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = ceprefretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual((summary.considered, summary.skipped), (1, 1))

    def test5(self) -> None:
        """Test that a lost claim race is skipped, not reported."""
        candidate = _candidate(flow_run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                ceprefretry,
                "_read_target",
                return_value=ceprefretry._RetryTarget(
                    base_url="http://pf", api_key="k"
                ),
            ),
            unittest.mock.patch.object(
                ceprefretry, "flow_run_is_reschedulable", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=False),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = ceprefretry.run_pending_retries(config=cemit.Config())
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
            summary = ceprefretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual(summary.considered, 0)

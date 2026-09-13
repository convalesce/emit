"""
Tests for the Airflow retry executor.

Every test below reaches into a private helper on purpose: the module
under test is built around small, individually testable seams
(`_read_target`, `_dag_exists_locally`, `_clear_task_instance`, ...), and
exercising each directly is how the fail-closed behaviour on every one of
them gets proven rather than only asserted through the one public
entrypoint.

Run with `make test`.
"""

# pylint: disable=protected-access

import http.server
import json
import logging
import sys
import threading
import types
import unittest
import unittest.mock
from typing import Any, Dict, List

import convalesce_emit as cemit
import convalesce_emit.retry as ceretry
import convalesce_emit_airflow.retry as cealretry

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_webserver_base_url1
# #############################################################################


class _Connection:
    """Stands in for Airflow's own Connection model."""

    host: Any = None
    schema: Any = None
    port: Any = None
    password: Any = None


class Test_webserver_base_url1(unittest.TestCase):
    """
    Test building the webserver's base URL from a resolved connection.
    """

    def test1(self) -> None:
        """
        Test that a host already carrying a scheme is used as-is, minus
        a trailing slash.
        """
        connection = _Connection()
        connection.host = "http://airflow-webserver:8080/"
        self.assertEqual(
            cealretry._webserver_base_url(connection),
            "http://airflow-webserver:8080",
        )

    def test2(self) -> None:
        """
        Test that a bare host is composed with its schema and port.
        """
        connection = _Connection()
        connection.host = "airflow-webserver"
        connection.schema = "https"
        connection.port = 8443
        self.assertEqual(
            cealretry._webserver_base_url(connection),
            "https://airflow-webserver:8443",
        )

    def test3(self) -> None:
        """
        Test that no host at all yields None rather than a guess.
        """
        self.assertIsNone(cealretry._webserver_base_url(_Connection()))


# #############################################################################
# Test_read_target1
# #############################################################################


class Test_read_target1(unittest.TestCase):
    """
    Test resolving the retry credential.
    """

    def test1(self) -> None:
        """
        Test that a fully-configured connection resolves to a target.
        """
        connection = _Connection()
        connection.host = "http://airflow-webserver:8080"
        connection.password = "s3cret-token"

        with (
            unittest.mock.patch.dict(
                "os.environ", {"CONVALESCE_AIRFLOW_RETRY_TOKEN": "retry_conn"}
            ),
            unittest.mock.patch.object(
                cealretry, "_get_connection", return_value=connection
            ) as get,
        ):
            target = cealretry._read_target()
        get.assert_called_once_with("retry_conn")
        assert target is not None
        self.assertEqual(target.base_url, "http://airflow-webserver:8080")
        self.assertEqual(target.token, "s3cret-token")

    def test2(self) -> None:
        """
        Test that no env var set fails closed without resolving anything.
        """
        with unittest.mock.patch.dict("os.environ", {}, clear=False) as environ:
            environ.pop("CONVALESCE_AIRFLOW_RETRY_TOKEN", None)
            with unittest.mock.patch.object(cealretry, "_get_connection") as get:
                target = cealretry._read_target()
        get.assert_not_called()
        self.assertIsNone(target)

    def test3(self) -> None:
        """
        Test that a connection with no password fails closed.
        """
        connection = _Connection()
        connection.host = "http://airflow-webserver:8080"

        with (
            unittest.mock.patch.dict(
                "os.environ", {"CONVALESCE_AIRFLOW_RETRY_TOKEN": "retry_conn"}
            ),
            unittest.mock.patch.object(
                cealretry, "_get_connection", return_value=connection
            ),
        ):
            self.assertIsNone(cealretry._read_target())

    def test4(self) -> None:
        """
        Test that a connection id that does not resolve fails closed.
        """
        with (
            unittest.mock.patch.dict(
                "os.environ", {"CONVALESCE_AIRFLOW_RETRY_TOKEN": "retry_conn"}
            ),
            unittest.mock.patch.object(
                cealretry,
                "_get_connection",
                side_effect=Exception("no such conn"),
            ),
        ):
            self.assertIsNone(cealretry._read_target())


# #############################################################################
# Test_parse_map_index1
# #############################################################################


class Test_parse_map_index1(unittest.TestCase):
    """
    Test reading a coordinate's map_index.
    """

    def test1(self) -> None:
        """Test that no map_index at all is fine -- an unmapped task."""
        self.assertEqual(cealretry._parse_map_index(None), (True, None))

    def test2(self) -> None:
        """Test that a numeric string parses."""
        self.assertEqual(cealretry._parse_map_index("3"), (True, 3))

    def test3(self) -> None:
        """Test that a non-numeric value fails closed, not silently 0."""
        self.assertEqual(cealretry._parse_map_index("abc"), (False, None))


# #############################################################################
# Test_clear_url1
# #############################################################################


class Test_clear_url1(unittest.TestCase):
    """
    Test picking the version-appropriate clearTaskInstances URL.
    """

    def test1(self) -> None:
        """Test Airflow 2's path -- confirmed against the real API."""
        target = cealretry._RetryTarget(base_url="http://af", token="t")
        self.assertEqual(
            cealretry._clear_url(target, 2, "orders"),
            "http://af/api/v1/dags/orders/clearTaskInstances",
        )

    def test2(self) -> None:
        """Test Airflow 3's path -- confirmed against the real API."""
        target = cealretry._RetryTarget(base_url="http://af", token="t")
        self.assertEqual(
            cealretry._clear_url(target, 3, "orders"),
            "http://af/api/v2/dags/orders/clearTaskInstances",
        )

    def test3(self) -> None:
        """
        Test that an unreadable version raises rather than guessing which
        API shape to call.
        """
        target = cealretry._RetryTarget(base_url="http://af", token="t")
        with self.assertRaises(RuntimeError):
            cealretry._clear_url(target, None, "orders")


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder(http.server.BaseHTTPRequestHandler):
    """Answers a configured response and records what it was asked."""

    requests: List[Dict[str, Any]] = []
    status = 200
    body = b"[]"

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
        _Recorder.body = b"[]"
        self._httpd = http.server.HTTPServer(("127.0.0.1", 0), _Recorder)
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()
        self._target = cealretry._RetryTarget(
            base_url=f"http://127.0.0.1:{self._httpd.server_address[1]}",
            token="af-token",
        )

    def tearDown(self) -> None:
        self._httpd.shutdown()


# #############################################################################
# Test_clear_task_instance1
# #############################################################################


class Test_clear_task_instance1(_ServerCase):
    """
    Test the real HTTP call clearTaskInstances makes.
    """

    def test1(self) -> None:
        """
        Test a successful clear of an unmapped task: the right URL, the
        bearer token, and a body that clears exactly this run's task
        without touching the dag run's own state.
        """
        ref = cealretry._clear_task_instance(
            self._target,
            2,
            dag_id="orders",
            task_id="load",
            run_id="manual__1",
            map_index=None,
            timeout=2.0,
        )
        self.assertEqual(ref, "orders/manual__1/load")
        request = _Recorder.requests[0]
        self.assertEqual(
            request["path"], "/api/v1/dags/orders/clearTaskInstances"
        )
        self.assertEqual(request["headers"]["Authorization"], "Bearer af-token")
        self.assertEqual(
            request["body"],
            {
                "dry_run": False,
                "only_failed": False,
                "reset_dag_runs": False,
                "dag_run_id": "manual__1",
                "task_ids": ["load"],
            },
        )

    def test2(self) -> None:
        """
        Test a mapped task's clear names its map_index in both the body
        and the returned reference.
        """
        ref = cealretry._clear_task_instance(
            self._target,
            3,
            dag_id="orders",
            task_id="load",
            run_id="manual__1",
            map_index=2,
            timeout=2.0,
        )
        self.assertEqual(ref, "orders/manual__1/load[2]")
        request = _Recorder.requests[0]
        self.assertEqual(
            request["path"], "/api/v2/dags/orders/clearTaskInstances"
        )
        self.assertEqual(request["body"]["task_ids"], [["load", 2]])

    def test3(self) -> None:
        """
        Test that a non-2xx response raises, not swallows -- the caller
        (`run_pending_retries`) is what turns this into FAILED_TO_TRIGGER.
        """
        _Recorder.status = 404
        _Recorder.body = b'{"detail": "dag not found"}'
        with self.assertRaises(RuntimeError):
            cealretry._clear_task_instance(
                self._target,
                2,
                dag_id="orders",
                task_id="load",
                run_id="manual__1",
                map_index=None,
                timeout=2.0,
            )


# #############################################################################
# Test_dag_exists_locally1
# #############################################################################


class _FakeDagBag:
    """Stands in for Airflow's own DagBag, resolving one fixed dag id."""

    known_dag_id: Any = None

    def __init__(self, **_kwargs: Any) -> None:
        pass

    def get_dag(self, dag_id: str) -> Any:
        """Resolve only the dag id this test seeded, if any."""
        return object() if dag_id == type(self).known_dag_id else None


class Test_dag_exists_locally1(unittest.TestCase):
    """
    Test the local-scope check against a DagBag.
    """

    def test1(self) -> None:
        """
        Test that Airflow being entirely absent -- true in this test
        environment, not simulated -- fails closed to False.
        """
        self.assertFalse(cealretry._dag_exists_locally("orders"))

    def test2(self) -> None:
        """
        Test that a dag the local DagBag resolves returns True.
        """
        _FakeDagBag.known_dag_id = "orders"
        module = types.SimpleNamespace(DagBag=_FakeDagBag)
        with unittest.mock.patch.dict(sys.modules, {"airflow.models": module}):
            self.assertTrue(cealretry._dag_exists_locally("orders"))

    def test3(self) -> None:
        """
        Test that a dag id the local DagBag does not know is False -- the
        exact case that must stop `run_pending_retries` from claiming it.
        """
        _FakeDagBag.known_dag_id = "orders"
        module = types.SimpleNamespace(DagBag=_FakeDagBag)
        with unittest.mock.patch.dict(sys.modules, {"airflow.models": module}):
            self.assertFalse(cealretry._dag_exists_locally("someone-elses-dag"))


# #############################################################################
# Test_run_pending_retries1
# #############################################################################


def _candidate(**coords: str) -> ceretry.RetryCandidate:
    """One airflow-tool candidate, coordinates as given."""
    return ceretry.RetryCandidate(
        remedy_id="r1",
        incident_urn="urn:li:incident:1",
        tool="airflow",
        coordinates=coords,
        created_at_millis=1,
    )


class Test_run_pending_retries1(unittest.TestCase):
    """
    Test the orchestration in `run_pending_retries`, with collect and
    Airflow's own API both mocked out -- each already has its own real
    tests above and in `convalesce_emit`'s own shared-client suite.
    """

    def test1(self) -> None:
        """
        Test that a dag_id this process does not recognize is skipped
        without ever calling claim() -- the single-shot cap's whole point.
        """
        candidate = _candidate(dag_id="not-mine", task_id="load", run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cealretry,
                "_read_target",
                return_value=cealretry._RetryTarget(
                    base_url="http://af", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_locally", return_value=False
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual(summary.considered, 1)
        self.assertEqual(summary.claimed, 0)
        self.assertEqual(summary.skipped, 1)

    def test2(self) -> None:
        """
        Test the full happy path: local scope passes, the claim
        succeeds, the clear call succeeds, and TRIGGERED is reported.
        """
        candidate = _candidate(dag_id="orders", task_id="load", run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cealretry,
                "_read_target",
                return_value=cealretry._RetryTarget(
                    base_url="http://af", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(
                cealretry, "_clear_task_instance", return_value="orders/r1/load"
            ),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        report.assert_called_once_with(
            "r1",
            outcome=ceretry.TRIGGERED,
            native_run_ref="orders/r1/load",
            config=unittest.mock.ANY,
        )
        self.assertEqual(
            (summary.claimed, summary.triggered, summary.failed), (1, 1, 0)
        )

    def test3(self) -> None:
        """
        Test that a clear call that raises is reported as
        FAILED_TO_TRIGGER, never left silently unreported and never
        retried in the same pass.
        """
        candidate = _candidate(dag_id="orders", task_id="load", run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cealretry,
                "_read_target",
                return_value=cealretry._RetryTarget(
                    base_url="http://af", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(
                cealretry,
                "_clear_task_instance",
                side_effect=RuntimeError("clearTaskInstances returned HTTP 404"),
            ),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
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
        """
        Test that no retry credential configured skips every candidate
        without claiming any of them.
        """
        candidate = _candidate(dag_id="orders", task_id="load", run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cealretry, "_read_target", return_value=None
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual((summary.considered, summary.skipped), (1, 1))

    def test5(self) -> None:
        """
        Test that a lost claim race (claim() returns False) is skipped,
        not treated as a failure to report.
        """
        candidate = _candidate(dag_id="orders", task_id="load", run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cealretry,
                "_read_target",
                return_value=cealretry._RetryTarget(
                    base_url="http://af", token="t"
                ),
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=False),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        report.assert_not_called()
        self.assertEqual((summary.claimed, summary.skipped), (0, 1))

    def test6(self) -> None:
        """
        Test that a tool-mismatched candidate (not this plugin's problem)
        is never considered at all.
        """
        candidate = ceretry.RetryCandidate(
            remedy_id="r2",
            incident_urn="urn:li:incident:2",
            tool="dagster",
            coordinates={"run_id": "abc"},
            created_at_millis=1,
        )
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual(summary.considered, 0)

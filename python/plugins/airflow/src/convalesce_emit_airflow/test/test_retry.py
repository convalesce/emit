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

# Airflow, Dagster and Prefect each test their retry executor against the
# same local HTTP server / _Recorder harness shape. Each plugin is
# independently installable with zero cross-plugin dependency, so sharing a
# test harness module between them would add one for no real benefit; the
# similarity stays and the check is turned off here rather than everywhere.
# pylint: disable=duplicate-code
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
    login: Any = None
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
        Test that with no name set the connection looked for is
        `convalesce_retry`, and that its absence is not a sign-in.
        """
        with unittest.mock.patch.dict("os.environ", {}, clear=False) as environ:
            environ.pop("CONVALESCE_AIRFLOW_RETRY_TOKEN", None)
            environ.pop("CONVALESCE_AIRFLOW_RETRY_CONNECTION", None)
            with unittest.mock.patch.object(
                cealretry, "_get_connection", side_effect=RuntimeError("none")
            ) as get:
                target = cealretry._read_target()
        get.assert_called_once_with("convalesce_retry")
        self.assertIsNone(target)

    def test5(self) -> None:
        """
        Test that the connection can be named, by the current variable
        first and still by the one it replaced.
        """
        for env, expected in (
            ({"CONVALESCE_AIRFLOW_RETRY_CONNECTION": "mine"}, "mine"),
            ({"CONVALESCE_AIRFLOW_RETRY_TOKEN": "older"}, "older"),
            (
                {
                    "CONVALESCE_AIRFLOW_RETRY_CONNECTION": "mine",
                    "CONVALESCE_AIRFLOW_RETRY_TOKEN": "older",
                },
                "mine",
            ),
        ):
            with unittest.mock.patch.dict(
                "os.environ", {}, clear=False
            ) as environ:
                environ.pop("CONVALESCE_AIRFLOW_RETRY_TOKEN", None)
                environ.pop("CONVALESCE_AIRFLOW_RETRY_CONNECTION", None)
                environ.update(env)
                with unittest.mock.patch.object(
                    cealretry,
                    "_get_connection",
                    side_effect=RuntimeError("none"),
                ) as get:
                    cealretry._read_target()
            get.assert_called_once_with(expected)

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

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        """Record one GET and answer with the configured response."""
        type(self).requests.append(
            {"path": self.path, "headers": dict(self.headers), "body": None}
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
                "reset_dag_runs": True,
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


# #############################################################################
# Test_authorization1
# #############################################################################


class Test_authorization1(_ServerCase):
    """
    Test how a call signs in, which follows what the connection carries
    and which Airflow it is.
    """

    def _user(self) -> Any:
        return cealretry._RetryTarget(
            base_url=self._target.base_url, token="pw", login="convalesce"
        )

    def test1(self) -> None:
        """
        Test that a password with no login is a ready-made token, sent as
        a bearer token whatever the Airflow version, with nothing fetched.
        """
        for major in (2, 3, None):
            self.assertEqual(
                cealretry._authorization(self._target, major, 5.0),
                "Bearer af-token",
            )
        self.assertEqual(_Recorder.requests, [])

    def test2(self) -> None:
        """
        Test that a user is signed in to Airflow 2 with basic auth, which
        is what its API accepts, with nothing fetched.
        """
        header = cealretry._authorization(self._user(), 2, 5.0)
        self.assertEqual(header, "Basic Y29udmFsZXNjZTpwdw==")
        self.assertEqual(_Recorder.requests, [])

    def test3(self) -> None:
        """
        Test that a user is signed in to Airflow 3 by exchanging the login
        and password for a token, so nothing stored can expire.
        """
        _Recorder.status = 201
        _Recorder.body = b'{"access_token": "fresh-jwt"}'
        header = cealretry._authorization(self._user(), 3, 5.0)
        self.assertEqual(header, "Bearer fresh-jwt")
        request = _Recorder.requests[0]
        self.assertEqual(request["path"], "/auth/token")
        self.assertEqual(
            request["body"], {"username": "convalesce", "password": "pw"}
        )

    def test4(self) -> None:
        """
        Test that a refused sign-in, or a reply with no token in it, is an
        error and never a header.
        """
        _Recorder.status = 401
        with self.assertRaises(RuntimeError):
            cealretry._authorization(self._user(), 3, 5.0)
        _Recorder.status = 201
        _Recorder.body = b"{}"
        with self.assertRaises(RuntimeError):
            cealretry._authorization(self._user(), 3, 5.0)

    def test5(self) -> None:
        """
        Test that a user on an Airflow whose version cannot be read is not
        signed in by guessing which of the two ways applies.
        """
        with self.assertRaises(RuntimeError):
            cealretry._authorization(self._user(), None, 5.0)


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
        incident_urn="urn:cvl:incident:1",
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
            incident_urn="urn:cvl:incident:2",
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

    def test7(self) -> None:
        """
        Test that a sign-in Airflow refuses stops the run before anything
        is claimed: a claim is a single shot, and none is spent on it.
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
                    base_url="http://af", token="pw", login="convalesce"
                ),
            ),
            unittest.mock.patch.object(
                cealretry, "_airflow_major_version", return_value=3
            ),
            unittest.mock.patch.object(
                cealretry, "_token_for", side_effect=RuntimeError("401")
            ),
            unittest.mock.patch.object(ceretry, "claim") as claim,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        claim.assert_not_called()
        self.assertEqual(summary.skipped, 1)
        self.assertEqual(summary.claimed, 0)


class Test_run_pending_retries_in_place1(unittest.TestCase):
    """
    Test which way a task is cleared: in place on an Airflow 2 with no
    sign-in set up, through the API wherever a sign-in exists, and not at
    all where neither is possible.
    """

    def _run(self, major: Any, target: Any, named: Any = None) -> Any:
        candidate = _candidate(dag_id="orders", task_id="load", run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cealretry, "_read_target", return_value=target
            ),
            unittest.mock.patch.object(
                cealretry, "_named_connection", return_value=named
            ),
            unittest.mock.patch.object(
                cealretry, "_airflow_major_version", return_value=major
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_over_api", return_value=True
            ),
            unittest.mock.patch.object(
                cealretry, "_authorization", return_value="Bearer t"
            ),
            unittest.mock.patch.object(
                ceretry, "claim", return_value=True
            ) as claim,
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
            unittest.mock.patch.object(
                cealretry, "_clear_in_place", return_value="orders/r1/load"
            ) as in_place,
            unittest.mock.patch.object(
                cealretry, "_clear_task_instance", return_value="orders/r1/load"
            ) as through_api,
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        return summary, claim, report, in_place, through_api

    def test1(self) -> None:
        """
        Test that Airflow 2 with no connection clears in place and
        reports the retry as triggered.
        """
        summary, _, report, in_place, through_api = self._run(2, None)
        in_place.assert_called_once_with(
            dag_id="orders", task_id="load", run_id="r1", map_index=None
        )
        through_api.assert_not_called()
        self.assertEqual((summary.claimed, summary.triggered), (1, 1))
        self.assertEqual(report.call_args.kwargs["outcome"], ceretry.TRIGGERED)

    def test2(self) -> None:
        """
        Test that a connection, where there is one, is what is used, on
        either major version.
        """
        target = cealretry._RetryTarget(base_url="http://af", token="t")
        for major in (2, 3):
            _, _, _, in_place, through_api = self._run(major, target)
            in_place.assert_not_called()
            through_api.assert_called_once()

    def test3(self) -> None:
        """
        Test that Airflow 3 with no connection, and an Airflow whose
        version cannot be read, claim nothing.
        """
        for major in (3, None):
            summary, claim, _, in_place, _ = self._run(major, None)
            claim.assert_not_called()
            in_place.assert_not_called()
            self.assertEqual((summary.considered, summary.skipped), (1, 1))

    def test4(self) -> None:
        """
        Test that a connection somebody named and that does not resolve is
        not quietly replaced by clearing in place.
        """
        summary, claim, _, in_place, _ = self._run(2, None, named="mine")
        claim.assert_not_called()
        in_place.assert_not_called()
        self.assertEqual(summary.skipped, 1)

    def test5(self) -> None:
        """
        Test that a failure clearing in place is reported, not retried.
        """
        candidate = _candidate(dag_id="orders", task_id="load", run_id="r1")
        with (
            unittest.mock.patch.object(
                ceretry, "list_pending", return_value=[candidate]
            ),
            unittest.mock.patch.object(
                cealretry, "_read_target", return_value=None
            ),
            unittest.mock.patch.object(
                cealretry, "_named_connection", return_value=None
            ),
            unittest.mock.patch.object(
                cealretry, "_airflow_major_version", return_value=2
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_locally", return_value=True
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_over_api", return_value=True
            ),
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(ceretry, "report_outcome") as report,
            unittest.mock.patch.object(
                cealretry, "_clear_in_place", side_effect=RuntimeError("gone")
            ),
        ):
            summary = cealretry.run_pending_retries(config=cemit.Config())
        self.assertEqual((summary.claimed, summary.failed), (1, 1))
        self.assertEqual(
            report.call_args.kwargs["outcome"], ceretry.FAILED_TO_TRIGGER
        )


class Test_dag_exists_over_api1(_ServerCase):
    """
    Test the question Airflow 3 is asked instead of its database.
    """

    def test1(self) -> None:
        """
        Test that a dag the API knows is ours, asked for by its own path
        with the sign-in already worked out.
        """
        _Recorder.status = 200
        self.assertTrue(
            cealretry._dag_exists_over_api(
                self._target, "orders", "Bearer abc", 2.0
            )
        )
        request = _Recorder.requests[0]
        self.assertEqual(request["path"], "/api/v2/dags/orders")
        self.assertEqual(request["headers"]["Authorization"], "Bearer abc")

    def test2(self) -> None:
        """
        Test that a dag the API does not know, and an API that cannot be
        asked, are both not ours.
        """
        _Recorder.status = 404
        with self.assertLogs("convalesce_emit_airflow.retry", level="WARNING"):
            self.assertFalse(
                cealretry._dag_exists_over_api(
                    self._target, "nope", "Bearer abc", 2.0
                )
            )
        gone = cealretry._RetryTarget(base_url="http://127.0.0.1:1", token="t")
        with self.assertLogs("convalesce_emit_airflow.retry", level="WARNING"):
            self.assertFalse(
                cealretry._dag_exists_over_api(gone, "orders", "Bearer abc", 1.0)
            )


class Test_run_pending_retries_on_three1(unittest.TestCase):
    """
    Test that Airflow 3 is asked through its API whether a dag is ours,
    and Airflow 2 through its own dag bag.
    """

    def _run(self, major: int) -> Any:
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
                cealretry, "_airflow_major_version", return_value=major
            ),
            unittest.mock.patch.object(
                cealretry, "_authorization", return_value="Bearer t"
            ),
            unittest.mock.patch.object(
                cealretry, "_dag_exists_over_api", return_value=True
            ) as over_api,
            unittest.mock.patch.object(
                cealretry, "_dag_exists_locally", return_value=True
            ) as locally,
            unittest.mock.patch.object(ceretry, "claim", return_value=True),
            unittest.mock.patch.object(ceretry, "report_outcome"),
            unittest.mock.patch.object(
                cealretry, "_clear_task_instance", return_value="orders/r1/load"
            ),
        ):
            cealretry.run_pending_retries(config=cemit.Config())
        return over_api, locally

    def test1(self) -> None:
        """Test that Airflow 3 is asked through its API."""
        over_api, locally = self._run(3)
        over_api.assert_called_once()
        locally.assert_not_called()

    def test2(self) -> None:
        """Test that Airflow 2 is asked through its dag bag."""
        over_api, locally = self._run(2)
        locally.assert_called_once()
        over_api.assert_not_called()

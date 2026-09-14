"""
Tests for the retry client.

Driven against a real HTTP server, the same discipline `test_gate.py` and
`test_client.py` use: a stubbed transport cannot prove what actually
crosses the wire, or what a response this module was not built to expect
does to the caller -- which is the entire point of a fail-closed module.

Run with `make test`.
"""

# Every test file in this suite keeps its own private local-HTTP-server /
# _Recorder harness rather than sharing one, `test_gate.py` and
# `test_client.py` included, so each test file stays self-contained and
# readable on its own; the check is turned off here rather than there too.
# pylint: disable=duplicate-code

import http.server
import json
import logging
import threading
import unittest
import urllib.parse
from typing import Any, Dict, List

import convalesce_emit.config as ceconfig
import convalesce_emit.retry as ceretry

_LOG = logging.getLogger(__name__)


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder(http.server.BaseHTTPRequestHandler):
    """
    Answers a configured response and records what it was asked.
    """

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
        parsed = urllib.parse.urlparse(self.path)
        type(self).requests.append(
            {
                "method": self.command,
                "path": parsed.path,
                "headers": dict(self.headers),
                "body": raw.decode("utf-8") if raw else None,
            }
        )
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(type(self).body)

    def log_message(self, *_args: Any) -> None:
        """Silence the default stderr logging."""


# #############################################################################
# _ServerCase
# #############################################################################


class _ServerCase(unittest.TestCase):
    """
    A local HTTP server for the duration of one test.
    """

    def setUp(self) -> None:
        _Recorder.requests = []
        _Recorder.status = 200
        _Recorder.body = b"{}"
        self._httpd = http.server.HTTPServer(("127.0.0.1", 0), _Recorder)
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()
        self._url = f"http://127.0.0.1:{self._httpd.server_address[1]}"

    def tearDown(self) -> None:
        self._httpd.shutdown()

    def _config(self, **kwargs: Any) -> ceconfig.Config:
        kwargs.setdefault("endpoint", self._url)
        kwargs.setdefault("api_key", "api-secret")
        kwargs.setdefault("timeout", 2.0)
        return ceconfig.Config(**kwargs)


# #############################################################################
# Test_list_pending1
# #############################################################################


class Test_list_pending1(_ServerCase):
    """
    Test listing pending retries.
    """

    def test1(self) -> None:
        """
        Test that a well-formed list round-trips into candidates.
        """
        _Recorder.body = json.dumps(
            {
                "retries": [
                    {
                        "remedy_id": "r1",
                        "incident_urn": "urn:li:incident:1",
                        "tool": "airflow",
                        "coordinates": {"dag_id": "orders", "task_id": "load"},
                        "created_at_millis": 123,
                    }
                ]
            }
        ).encode("utf-8")
        candidates = ceretry.list_pending(config=self._config())
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].remedy_id, "r1")
        self.assertEqual(candidates[0].tool, "airflow")
        self.assertEqual(
            candidates[0].coordinates, {"dag_id": "orders", "task_id": "load"}
        )
        self.assertEqual(_Recorder.requests[0]["path"], "/v1/retries")
        self.assertEqual(
            _Recorder.requests[0]["headers"]["Authorization"],
            "Bearer api-secret",
        )

    def test2(self) -> None:
        """
        Test that one malformed row is skipped without dropping the rest.
        """
        _Recorder.body = json.dumps(
            {
                "retries": [
                    {"remedy_id": "r1"},
                    {
                        "remedy_id": "r2",
                        "incident_urn": "urn:li:incident:2",
                        "tool": "dagster",
                        "coordinates": {"run_id": "abc"},
                        "created_at_millis": 456,
                    },
                ]
            }
        ).encode("utf-8")
        with self.assertLogs("convalesce_emit.retry", level="WARNING"):
            candidates = ceretry.list_pending(config=self._config())
        self.assertEqual([c.remedy_id for c in candidates], ["r2"])

    def test3(self) -> None:
        """
        Test that a non-2xx status fails closed to an empty list.
        """
        _Recorder.status = 500
        with self.assertLogs("convalesce_emit.retry", level="WARNING"):
            self.assertEqual(ceretry.list_pending(config=self._config()), [])

    def test4(self) -> None:
        """
        Test that an unreachable endpoint fails closed, not raises.
        """
        config = ceconfig.Config(
            endpoint="http://127.0.0.1:1", api_key="k", timeout=1.0
        )
        with self.assertLogs("convalesce_emit.retry", level="WARNING") as logs:
            self.assertEqual(ceretry.list_pending(config=config), [])
        self.assertIn("could not list pending retries", "\n".join(logs.output))

    def test5(self) -> None:
        """
        Test that a body missing the 'retries' array fails closed.
        """
        _Recorder.body = b'{"unexpected": true}'
        with self.assertLogs("convalesce_emit.retry", level="WARNING"):
            self.assertEqual(ceretry.list_pending(config=self._config()), [])

    def test6(self) -> None:
        """
        Test that no api key configured fails closed without a request.
        """
        with self.assertLogs("convalesce_emit.retry", level="WARNING"):
            self.assertEqual(
                ceretry.list_pending(config=self._config(api_key=None)), []
            )
        self.assertEqual(_Recorder.requests, [])

    def test7(self) -> None:
        """
        Test that dry-run and disabled both skip the network entirely.
        """
        self.assertEqual(
            ceretry.list_pending(config=self._config(dry_run=True)), []
        )
        self.assertEqual(
            ceretry.list_pending(config=self._config(enabled=False)), []
        )
        self.assertEqual(_Recorder.requests, [])


# #############################################################################
# Test_claim1
# #############################################################################


class Test_claim1(_ServerCase):
    """
    Test claiming one pending retry.
    """

    def test1(self) -> None:
        """
        Test that a real 201 with claimed=true returns True, and that the
        owner and api key -- never the ingest key -- travel as sent.
        """
        _Recorder.status = 201
        _Recorder.body = b'{"remedy_id": "r1", "claimed": true}'
        result = ceretry.claim(
            "r1",
            owner="worker-7",
            config=self._config(
                api_key="api-secret", ingest_key="ingest-secret"
            ),
        )
        self.assertTrue(result)
        request = _Recorder.requests[0]
        self.assertEqual(request["path"], "/v1/retries/r1/claim")
        self.assertEqual(request["method"], "POST")
        self.assertEqual(json.loads(request["body"]), {"owner": "worker-7"})
        self.assertEqual(
            request["headers"]["Authorization"], "Bearer api-secret"
        )
        self.assertNotIn("ingest-secret", json.dumps(request["headers"]))

    def test2(self) -> None:
        """
        Test that a 409 -- the race loser's correct answer -- returns
        False, not an exception, and is not treated as an error.
        """
        _Recorder.status = 409
        _Recorder.body = b'{"error": "already claimed"}'
        with self.assertLogs("convalesce_emit.retry", level="INFO"):
            self.assertFalse(
                ceretry.claim("r1", owner="worker-7", config=self._config())
            )

    def test3(self) -> None:
        """
        Test that a 404 for an unknown remedy id returns False.
        """
        _Recorder.status = 404
        _Recorder.body = b'{"error": "no such remedy"}'
        self.assertFalse(
            ceretry.claim("missing", owner="worker-7", config=self._config())
        )

    def test4(self) -> None:
        """
        Test that a 201 with no claimed=true confirmation still fails
        closed rather than assuming success.
        """
        _Recorder.status = 201
        _Recorder.body = b'{"remedy_id": "r1"}'
        with self.assertLogs("convalesce_emit.retry", level="WARNING"):
            self.assertFalse(
                ceretry.claim("r1", owner="worker-7", config=self._config())
            )

    def test5(self) -> None:
        """
        Test that an unreachable endpoint fails closed, not raises.
        """
        config = ceconfig.Config(
            endpoint="http://127.0.0.1:1", api_key="k", timeout=1.0
        )
        with self.assertLogs("convalesce_emit.retry", level="WARNING"):
            self.assertFalse(
                ceretry.claim("r1", owner="worker-7", config=config)
            )

    def test6(self) -> None:
        """
        Test that no api key configured fails closed without a request.
        """
        self.assertFalse(
            ceretry.claim(
                "r1", owner="worker-7", config=self._config(api_key=None)
            )
        )
        self.assertEqual(_Recorder.requests, [])


# #############################################################################
# Test_report_outcome1
# #############################################################################


class Test_report_outcome1(_ServerCase):
    """
    Test reporting a claimed retry's outcome.
    """

    def test1(self) -> None:
        """
        Test that a real 200 first report returns True, and the full
        payload -- outcome, native_run_ref -- travels as sent.
        """
        _Recorder.body = b'{"remedy_id": "r1", "outcome": "TRIGGERED"}'
        result = ceretry.report_outcome(
            "r1",
            outcome=ceretry.TRIGGERED,
            native_run_ref="dag_run_123",
            config=self._config(),
        )
        self.assertTrue(result)
        request = _Recorder.requests[0]
        self.assertEqual(request["path"], "/v1/retries/r1/outcome")
        self.assertEqual(
            json.loads(request["body"]),
            {"outcome": "TRIGGERED", "native_run_ref": "dag_run_123"},
        )

    def test2(self) -> None:
        """
        Test that FAILED_TO_TRIGGER with an error message reports the
        same way as TRIGGERED, field for field.
        """
        result = ceretry.report_outcome(
            "r1",
            outcome=ceretry.FAILED_TO_TRIGGER,
            error="dag not found locally",
            config=self._config(),
        )
        self.assertTrue(result)
        self.assertEqual(
            json.loads(_Recorder.requests[0]["body"]),
            {"outcome": "FAILED_TO_TRIGGER", "error": "dag not found locally"},
        )

    def test3(self) -> None:
        """
        Test that a 409 -- already reported, agreeing or not -- returns
        False and is never retried by this function itself.
        """
        _Recorder.status = 409
        with self.assertLogs("convalesce_emit.retry", level="INFO"):
            self.assertFalse(
                ceretry.report_outcome(
                    "r1", outcome=ceretry.TRIGGERED, config=self._config()
                )
            )

    def test4(self) -> None:
        """
        Test that an unrecognized outcome string raises rather than being
        swallowed -- a caller bug, not a transport failure.
        """
        with self.assertRaises(ValueError):
            ceretry.report_outcome(
                "r1", outcome="SUCCESS", config=self._config()
            )
        self.assertEqual(_Recorder.requests, [])

    def test5(self) -> None:
        """
        Test that an unreachable endpoint fails closed, not raises.
        """
        config = ceconfig.Config(
            endpoint="http://127.0.0.1:1", api_key="k", timeout=1.0
        )
        with self.assertLogs("convalesce_emit.retry", level="WARNING"):
            self.assertFalse(
                ceretry.report_outcome(
                    "r1", outcome=ceretry.TRIGGERED, config=config
                )
            )

    def test6(self) -> None:
        """
        Test that no api key configured fails closed without a request.
        """
        self.assertFalse(
            ceretry.report_outcome(
                "r1",
                outcome=ceretry.TRIGGERED,
                config=self._config(api_key=None),
            )
        )
        self.assertEqual(_Recorder.requests, [])

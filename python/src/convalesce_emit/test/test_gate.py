"""
Tests for the blocking operator.

Driven against a real HTTP server, the same discipline `test_client.py`
uses: a stubbed transport cannot prove what actually crosses the wire, or
what a request that never gets an answer at all does to the caller -- which
is the entire point of this module.

Run with `make test`.
"""

# Every test file in this suite keeps its own private local-HTTP-server /
# _Recorder harness rather than sharing one, `test_retry.py` and
# `test_client.py` included, so each test file stays self-contained and
# readable on its own; the check is turned off here rather than there too.
# pylint: disable=duplicate-code

import http.server
import json
import logging
import threading
import time
import unittest
import urllib.parse
from typing import Any, Dict, List

import convalesce_emit.config as ceconfig
import convalesce_emit.gate as cegate

_LOG = logging.getLogger(__name__)


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder(http.server.BaseHTTPRequestHandler):
    """
    Answers a configured verdict and records what it was asked.
    """

    requests: List[Dict[str, Any]] = []
    status = 200
    body = b'{"open": true, "reason": "ok"}'
    delay_seconds = 0.0

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        """Record one request and answer with the configured verdict."""
        if type(self).delay_seconds:
            time.sleep(type(self).delay_seconds)
        parsed = urllib.parse.urlparse(self.path)
        type(self).requests.append(
            {
                "path": parsed.path,
                "query": dict(urllib.parse.parse_qsl(parsed.query)),
                "headers": dict(self.headers),
            }
        )
        self.send_response(type(self).status)
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
        _Recorder.body = b'{"open": true, "reason": "ok"}'
        _Recorder.delay_seconds = 0.0
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
# Test_gate_wire1
# #############################################################################


class Test_gate_wire1(_ServerCase):
    """
    Test what actually reaches the network, and what comes back.
    """

    def test1(self) -> None:
        """
        Test that an explicit open verdict returns True.
        """
        _Recorder.body = b'{"open": true, "reason": "passing"}'
        self.assertTrue(cegate.is_open("orders", config=self._config()))

    def test2(self) -> None:
        """
        Test that an explicit closed verdict, and only that, returns False.
        """
        _Recorder.body = b'{"open": false, "reason": "failing assertion"}'
        self.assertFalse(cegate.is_open("orders", config=self._config()))

    def test3(self) -> None:
        """
        Test that the dataset is sent as a query parameter, unmodified.

        The operator builds no urns: whatever the caller passes crosses as
        given, the same as the recording operator.
        """
        cegate.is_open(
            "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)",
            config=self._config(),
        )
        self.assertEqual(
            _Recorder.requests[0]["query"]["dataset"],
            "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)",
        )
        self.assertEqual(_Recorder.requests[0]["path"], "/v1/gate")

    def test4(self) -> None:
        """
        Test that the api key travels as a bearer token, never as the
        ingest key, and never anywhere else in the request.
        """
        cegate.is_open(
            "orders",
            config=self._config(
                api_key="api-secret", ingest_key="ingest-secret"
            ),
        )
        headers = _Recorder.requests[0]["headers"]
        self.assertEqual(headers["Authorization"], "Bearer api-secret")
        self.assertNotIn("ingest-secret", json.dumps(headers))


# #############################################################################
# Test_gate_failsopen1
# #############################################################################


class Test_gate_failsopen1(_ServerCase):
    """
    Test the hard requirement: anything short of an explicit closed verdict
    proceeds. A metadata service having a bad minute must not stop a
    customer's pipeline.
    """

    def test1(self) -> None:
        """
        Test that an unreachable endpoint fails open.

        Nothing is listening on this port, so the connection is refused
        immediately -- a real failure to reach the gate, not a simulation
        of one.
        """
        config = ceconfig.Config(
            endpoint="http://127.0.0.1:1", api_key="k", timeout=1.0
        )
        with self.assertLogs("convalesce_emit.gate", level="WARNING") as logs:
            self.assertTrue(cegate.is_open("orders", config=config))
        self.assertIn("could not reach", "\n".join(logs.output))

    def test2(self) -> None:
        """
        Test that a slow endpoint -- a real timeout, not a mocked one --
        fails open.

        The server genuinely sleeps past the client's timeout; this proves
        the caller gets an answer and gets it back, rather than hanging or
        raising, when the gate is merely slow rather than actually down.
        """
        _Recorder.delay_seconds = 0.5
        config = self._config(timeout=0.05)
        started = time.monotonic()
        with self.assertLogs("convalesce_emit.gate", level="WARNING") as logs:
            self.assertTrue(cegate.is_open("orders", config=config))
        elapsed = time.monotonic() - started
        self.assertLess(
            elapsed, 0.5, "is_open should not wait for the slow response"
        )
        self.assertIn("could not reach", "\n".join(logs.output))

    def test3(self) -> None:
        """
        Test that a non-2xx status fails open rather than blocking.
        """
        _Recorder.status = 500
        with self.assertLogs("convalesce_emit.gate", level="WARNING"):
            self.assertTrue(cegate.is_open("orders", config=self._config()))

    def test4(self) -> None:
        """
        Test that a body that is not valid JSON fails open.
        """
        _Recorder.body = b"not json"
        with self.assertLogs("convalesce_emit.gate", level="WARNING") as logs:
            self.assertTrue(cegate.is_open("orders", config=self._config()))
        self.assertIn("not valid json", "\n".join(logs.output))

    def test5(self) -> None:
        """
        Test that a body with no "open" field fails open.

        A future, richer response shape must not be misread as a block.
        """
        _Recorder.body = b'{"reason": "missing the field that matters"}'
        with self.assertLogs("convalesce_emit.gate", level="WARNING"):
            self.assertTrue(cegate.is_open("orders", config=self._config()))

    def test6(self) -> None:
        """
        Test that "open" being anything other than exactly False proceeds.

        Only a definite closed verdict blocks; a truthy-but-not-boolean
        value must not be read as "closed" by accident.
        """
        _Recorder.body = b'{"open": "no", "reason": "not a real boolean"}'
        self.assertTrue(cegate.is_open("orders", config=self._config()))

    def test7(self) -> None:
        """
        Test that no api key configured fails open without a request.
        """
        config = self._config(api_key=None)
        with self.assertLogs("convalesce_emit.gate", level="WARNING") as logs:
            self.assertTrue(cegate.is_open("orders", config=config))
        self.assertEqual(_Recorder.requests, [])
        self.assertIn("no api key", "\n".join(logs.output))


# #############################################################################
# Test_gate_modes1
# #############################################################################


class Test_gate_modes1(_ServerCase):
    """
    Test the two ways of not asking at all.
    """

    def test1(self) -> None:
        """
        Test that a disabled configuration proceeds without a request.
        """
        self.assertTrue(
            cegate.is_open("orders", config=self._config(enabled=False))
        )
        self.assertEqual(_Recorder.requests, [])

    def test2(self) -> None:
        """
        Test that dry-run proceeds without a request.
        """
        self.assertTrue(
            cegate.is_open("orders", config=self._config(dry_run=True))
        )
        self.assertEqual(_Recorder.requests, [])

"""
Tests for batching and delivering observations.

Driven against a real HTTP server rather than a stubbed transport: what
leaves the process is the claim being tested, so a fake at that boundary
would test nothing.

Run with `make test`.
"""

import http.server
import json
import logging
import threading
import unittest
import unittest.mock
from typing import Any, Dict, List

import convalesce_emit.client as ceclient
import convalesce_emit.config as ceconfig

_LOG = logging.getLogger(__name__)


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder(http.server.BaseHTTPRequestHandler):
    """
    Records what was posted, so a test can read it back.

    Held on the handler class because `HTTPServer` builds a fresh instance
    per request; each test resets it in `setUp` rather than sharing state,
    since a fake that remembers across tests passes in isolation and fails in
    a suite.
    """

    received: List[Dict[str, Any]] = []
    headers_seen: List[Dict[str, str]] = []
    status = 200

    def do_POST(self) -> None:  # pylint: disable=invalid-name
        """Record one request and answer with the configured status."""
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        type(self).received.append(json.loads(body))
        type(self).headers_seen.append(dict(self.headers))
        self.send_response(type(self).status)
        self.end_headers()

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
        _Recorder.received = []
        _Recorder.headers_seen = []
        _Recorder.status = 200
        self._httpd = http.server.HTTPServer(("127.0.0.1", 0), _Recorder)
        threading.Thread(target=self._httpd.serve_forever, daemon=True).start()
        self._url = f"http://127.0.0.1:{self._httpd.server_address[1]}"

    def tearDown(self) -> None:
        self._httpd.shutdown()

    def _emitter(self, **kwargs: Any) -> ceclient.Emitter:
        """
        Build an emitter pointed at this test's server.

        :param kwargs: configuration overrides
        :return: the emitter
        """
        kwargs.setdefault("batch_size", 1)
        config = ceconfig.Config(
            endpoint=self._url,
            ingest_key="secret-key",
            **kwargs,
        )
        return ceclient.Emitter(config)

    def _sent(self) -> List[Dict[str, Any]]:
        """
        :return: the observations the server received, flattened
        """
        out: List[Dict[str, Any]] = []
        for request in _Recorder.received:
            out.extend(request["observations"])
        return out


# #############################################################################
# Test_emitter_wire1
# #############################################################################


class Test_emitter_wire1(_ServerCase):
    """
    Test what actually reaches the network.
    """

    def test1(self) -> None:
        """
        Test that a payload arrives byte for byte.

        Nested structures, nulls and non-ASCII all survive, because the
        receiver's mapping is written against the tool's own shape.
        """
        payload = {
            "dag_id": "orders",
            "state": "failed",
            "nested": {"list": [1, 2, {"deep": True}], "null": None},
            "unicode": "cafe ☕",
        }
        with self._emitter() as emitter:
            emitter.emit(
                tool="airflow", event="task_instance_failed", payload=payload
            )
        observation = self._sent()[0]
        self.assertEqual(observation["payload"], payload)
        self.assertEqual(observation["tool"], "airflow")

    def test2(self) -> None:
        """
        Test that the key travels in the header and never in the body.
        """
        with self._emitter() as emitter:
            emitter.emit(tool="airflow", event="e", payload={})
        headers = _Recorder.headers_seen[0]
        self.assertEqual(headers["Authorization"], "Bearer secret-key")
        # No header may name an account: one that did would be an
        # unauthenticated claim beside the credential that proves it.
        self.assertNotIn("X-Convalesce-Workspace", headers)
        self.assertTrue(headers["User-Agent"].startswith("convalesce-emit/"))
        self.assertNotIn("secret-key", json.dumps(_Recorder.received))

    def test3(self) -> None:
        """
        Test that a full batch is one request, not one per observation.
        """
        emitter = self._emitter(batch_size=5)
        for i in range(5):
            emitter.emit(tool="dagster", event="run_status", payload={"i": i})
        self.assertEqual(len(_Recorder.received), 1)
        self.assertEqual(len(self._sent()), 5)

    def test4(self) -> None:
        """
        Test that closing flushes a partial batch.
        """
        emitter = self._emitter(batch_size=100)
        emitter.emit(tool="airflow", event="e", payload={})
        emitter.close()
        self.assertEqual(len(self._sent()), 1)


# #############################################################################
# Test_emitter_failure1
# #############################################################################


class Test_emitter_failure1(_ServerCase):
    """
    Test that our failures stay ours.
    """

    def test1(self) -> None:
        """
        Test that a server error never reaches the caller.

        A customer's DAG must not go red because our endpoint had a bad
        minute: we are watching their pipeline, not standing in it.
        """
        _Recorder.status = 500
        emitter = self._emitter(max_retries=0)
        emitter.emit(tool="airflow", event="e", payload={})

    def test2(self) -> None:
        """
        Test that an unreachable endpoint never reaches the caller.
        """
        config = ceconfig.Config(
            endpoint="http://127.0.0.1:1",
            ingest_key="k",
            batch_size=1,
            max_retries=0,
        )
        ceclient.Emitter(config).emit(tool="airflow", event="e", payload={})


# #############################################################################
# Test_emitter_retry1
# #############################################################################


class Test_emitter_retry1(_ServerCase):
    """
    Test that only what a retry can fix is retried.
    """

    def test1(self) -> None:
        """
        Test that a rejected request is sent exactly once.

        A 400 means the receiver understood us and said no; sending it again
        only spends the pipeline's time.
        """
        _Recorder.status = 400
        emitter = self._emitter(max_retries=3)
        with unittest.mock.patch("time.sleep", lambda _s: None):
            emitter.emit(tool="airflow", event="e", payload={})
        self.assertEqual(len(_Recorder.received), 1)

    def test2(self) -> None:
        """
        Test that a transient failure is retried up to the limit.
        """
        _Recorder.status = 503
        emitter = self._emitter(max_retries=2)
        with unittest.mock.patch("time.sleep", lambda _s: None):
            emitter.emit(tool="airflow", event="e", payload={})
        # The first attempt plus two retries.
        self.assertEqual(len(_Recorder.received), 3)


# #############################################################################
# Test_emitter_modes1
# #############################################################################


class Test_emitter_modes1(_ServerCase):
    """
    Test the two ways of not sending.
    """

    def test1(self) -> None:
        """
        Test that a disabled emitter sends nothing.
        """
        emitter = self._emitter(enabled=False)
        emitter.emit(tool="airflow", event="e", payload={})
        self.assertEqual(_Recorder.received, [])

    def test2(self) -> None:
        """
        Test that dry-run logs the envelope and sends nothing.
        """
        emitter = self._emitter(dry_run=True)
        with self.assertLogs("convalesce_emit.client", level="INFO") as logs:
            emitter.emit(tool="airflow", event="e", payload={"x": 1})
        self.assertEqual(_Recorder.received, [])
        self.assertIn("dry-run", "\n".join(logs.output))
        self.assertIn('"x": 1', "\n".join(logs.output))

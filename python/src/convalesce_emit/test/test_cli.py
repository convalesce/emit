"""
Tests for `convalesce-emit check`.

Driven against a real HTTP server, like the transport's own tests: what the
check sends, and what it makes of each answer, is the claim being tested.

Run with `make test`.
"""

import dataclasses
import gzip
import http.server
import io
import logging
import os
import socket
import threading
import unittest
import unittest.mock
from typing import Any, Dict, List, Tuple

import convalesce_emit.cli as cecli
import convalesce_emit.config as ceconfig

_LOG = logging.getLogger(__name__)

_KEY = "cvl_ingest_abc123_s3cr3t-value"


# #############################################################################
# _Receiver
# #############################################################################


class _Receiver(http.server.BaseHTTPRequestHandler):
    """
    Answers every request with the configured status, and records it.
    """

    status = 202
    bodies: List[bytes] = []
    headers_seen: List[Dict[str, str]] = []
    paths: List[str] = []

    def do_POST(self) -> None:  # pylint: disable=invalid-name
        """Record one request and answer with the configured status."""
        length = int(self.headers.get("Content-Length", 0))
        type(self).bodies.append(gzip.decompress(self.rfile.read(length)))
        type(self).headers_seen.append(dict(self.headers))
        type(self).paths.append(self.path)
        self.send_response(type(self).status)
        self.end_headers()

    def log_message(self, *_args: Any) -> None:
        """Silence the default stderr logging."""


def _free_port() -> int:
    """
    A port nothing listens on.

    :return: the port
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# #############################################################################
# Test_check1
# #############################################################################


class Test_check1(unittest.TestCase):
    """
    Test what the check sends and what it says about each answer.
    """

    def setUp(self) -> None:
        _Receiver.status = 202
        _Receiver.bodies = []
        _Receiver.headers_seen = []
        _Receiver.paths = []
        self.server = http.server.HTTPServer(("127.0.0.1", 0), _Receiver)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.endpoint = (
            f"http://127.0.0.1:{self.server.server_address[1]}/openapi"
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def _check(self, config: ceconfig.Config) -> Tuple[int, str]:
        out = io.StringIO()
        code = cecli.check(config, out)
        return code, out.getvalue()

    def test1(self) -> None:
        """
        Test that an accepted key is connected, checked with one empty batch
        sent the way a real one is.
        """
        code, text = self._check(
            ceconfig.Config(endpoint=self.endpoint, ingest_key=_KEY)
        )
        self.assertEqual(code, cecli.EXIT_CONNECTED)
        self.assertIn("Connected", text)
        self.assertEqual(_Receiver.paths, ["/openapi/v1/observations"])
        self.assertEqual(_Receiver.bodies, [b'{"observations":[]}'])
        self.assertEqual(
            _Receiver.headers_seen[0]["Authorization"], f"Bearer {_KEY}"
        )

    def test2(self) -> None:
        """
        Test that each refusal names what to fix, and the secret is never
        printed.
        """
        config = ceconfig.Config(endpoint=self.endpoint, ingest_key=_KEY)
        for status, expected in (
            (401, "refused"),
            (403, "ingest key"),
            (404, "/openapi"),
            (500, "HTTP 500"),
        ):
            _Receiver.status = status
            code, text = self._check(config)
            self.assertEqual(code, cecli.EXIT_NOT_CONNECTED, status)
            self.assertIn(expected, text)
            self.assertIn("cvl_ingest_abc123_", text)
            self.assertNotIn("s3cr3t", text)

    def test3(self) -> None:
        """
        Test that an endpoint nothing listens on is reported as unreachable.
        """
        code, text = self._check(
            ceconfig.Config(
                endpoint=f"http://127.0.0.1:{_free_port()}", ingest_key=_KEY
            )
        )
        self.assertEqual(code, cecli.EXIT_NOT_CONNECTED)
        self.assertIn("could not reach", text)

    def test4(self) -> None:
        """
        Test that a configuration that would send nothing is told why,
        without making a request.
        """
        base = ceconfig.Config(endpoint=self.endpoint, ingest_key=_KEY)
        for fields, expected in (
            ({"ingest_key": None}, "CONVALESCE_INGEST_KEY"),
            ({"dry_run": True}, "CONVALESCE_DRY_RUN"),
            ({"enabled": False}, "CONVALESCE_ENABLED"),
        ):
            code, text = self._check(dataclasses.replace(base, **fields))
            self.assertEqual(code, cecli.EXIT_MISCONFIGURED, fields)
            self.assertIn(expected, text)
        self.assertEqual(_Receiver.paths, [])

    def test5(self) -> None:
        """
        Test that the command reads the environment, and its flags win.
        """
        environ = {
            "CONVALESCE_ENDPOINT": "http://example.invalid",
            "CONVALESCE_INGEST_KEY": "cvl_ingest_fromenv_x",
        }
        with unittest.mock.patch.dict(os.environ, environ):
            with unittest.mock.patch("sys.stdout", new_callable=io.StringIO):
                code = cecli.main(
                    ["check", "--endpoint", self.endpoint, "--ingest-key", _KEY]
                )
        self.assertEqual(code, cecli.EXIT_CONNECTED)
        self.assertEqual(
            _Receiver.headers_seen[0]["Authorization"], f"Bearer {_KEY}"
        )

    def test6(self) -> None:
        """
        Test that an unreadable numeric setting is reported, not raised.
        """
        with unittest.mock.patch.dict(
            os.environ, {"CONVALESCE_TIMEOUT": "soon"}
        ):
            with unittest.mock.patch(
                "sys.stdout", new_callable=io.StringIO
            ) as out:
                code = cecli.main(["check"])
        self.assertEqual(code, cecli.EXIT_MISCONFIGURED)
        self.assertIn("Not checked", out.getvalue())

    def test7(self) -> None:
        """
        Test that only a key's public part is ever shown.
        """
        self.assertEqual(cecli.key_id(_KEY), "cvl_ingest_abc123_...")
        self.assertEqual(cecli.key_id("not-a-key"), "(not a Convalesce key)")
        self.assertEqual(cecli.key_id(None), "(not a Convalesce key)")

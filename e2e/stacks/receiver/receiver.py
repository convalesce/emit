"""
A stand-in for the Convalesce ingest endpoint.

Records every observation a plugin posts so a test can read it back. It
checks the ingest key the way the real endpoint does, because a plugin that
forgot the Authorization header would otherwise look like it worked.

Standard library only, on purpose: the container is `python:*-alpine` and
this one file.

Run as `python receiver.py` with `RECEIVER_KEY` set.
"""

import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

_LOG = logging.getLogger(__name__)

KEY = os.environ.get("RECEIVER_KEY", "")
PORT = int(os.environ.get("RECEIVER_PORT", "8080"))
OBSERVATIONS_PATH = "/v1/observations"

_LOCK = threading.Lock()
_RECEIVED: List[Dict[str, Any]] = []
_REJECTED: List[Dict[str, Any]] = []


# #############################################################################
# Handler
# #############################################################################


class Handler(BaseHTTPRequestHandler):
    """Accepts observation batches and hands back what it has accepted."""

    def log_message(self, *args: Any) -> None:
        """Write the request log to stderr, prefixed."""
        sys.stderr.write("receiver: " + args[0] % args[1:] + "\n")

    def do_GET(self) -> None:  # pylint: disable=invalid-name
        """Answer the health check and the read-back."""
        if self.path == "/health":
            self._json(200, {"ok": True})
        elif self.path == "/observations":
            with _LOCK:
                body = {
                    "observations": list(_RECEIVED),
                    "rejected": list(_REJECTED),
                }
            self._json(200, body)
        else:
            self._json(404, {"error": "no such path"})

    def do_POST(self) -> None:  # pylint: disable=invalid-name
        """Accept one batch, or refuse it the way the real endpoint would."""
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        if self.path != OBSERVATIONS_PATH:
            self._json(404, {"error": "no such path"})
            return
        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {KEY}":
            with _LOCK:
                _REJECTED.append({"reason": "bad key", "header": auth[:12]})
            self._json(401, {"error": "bad key"})
            return
        try:
            body = json.loads(raw.decode("utf-8"))
        except ValueError:
            self._json(400, {"error": "not json"})
            return
        batch = body.get("observations")
        if not isinstance(batch, list):
            self._json(400, {"error": "no observations list"})
            return
        agent = self.headers.get("User-Agent", "")
        with _LOCK:
            for observation in batch:
                _RECEIVED.append(
                    {
                        "observation": observation,
                        "user_agent": agent,
                        "batch_size": len(batch),
                    }
                )
        self.log_message("accepted %d observation(s) from %s", len(batch), agent)
        self._json(202, {"accepted": len(batch)})

    def _json(self, status: int, body: Dict[str, Any]) -> None:
        """
        Send one JSON response.

        :param status: the HTTP status
        :param body: what to encode
        :return: nothing
        """
        raw = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def main() -> None:
    """
    Serve until killed.

    :return: nothing
    """
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)  # nosec B104
    sys.stderr.write(f"receiver: listening on {PORT}\n")
    server.serve_forever()


if __name__ == "__main__":
    main()

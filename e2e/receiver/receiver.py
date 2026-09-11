"""
A stand-in for the Convalesce ingest endpoint.

Records every observation a plugin posts so a test can read it back. It
checks the ingest key the way the real endpoint does, because a plugin that
forgets the Authorization header would otherwise look like it worked.

Runs on the stdlib alone so the container is `python:*-alpine` and a script.
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

KEY = os.environ.get("RECEIVER_KEY", "")
PORT = int(os.environ.get("RECEIVER_PORT", "8080"))

_LOCK = threading.Lock()
_RECEIVED = []
_REJECTED = []


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write("receiver: " + fmt % args + "\n")

    def _json(self, status, body):
        raw = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"ok": True})
        elif self.path == "/observations":
            with _LOCK:
                self._json(200, {"observations": list(_RECEIVED), "rejected": list(_REJECTED)})
        else:
            self._json(404, {"error": "no such path"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        if self.path != "/v1/observations":
            self._json(404, {"error": "no such path"})
            return
        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {KEY}":
            with _LOCK:
                _REJECTED.append({"reason": "bad key", "authorization": auth[:12]})
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
                _RECEIVED.append({"observation": observation, "user_agent": agent, "batch_size": len(batch)})
        self.log_message("accepted %d observation(s) from %s", len(batch), agent)
        self._json(202, {"accepted": len(batch)})


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    sys.stderr.write(f"receiver: listening on {PORT}\n")
    server.serve_forever()

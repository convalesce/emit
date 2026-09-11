"""
Drives one tool's compose stack and reads what reached the receiver.

Every test does the same three things: bring the stack up, make the tool run
an example workflow, and wait for the observations that workflow should have
produced. This holds the first and the last.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

DEFAULT_KEY = "e2e-ingest-key-not-a-secret"


class Stack:
    """One docker compose project: a tool plus the receiver."""

    def __init__(self, tool: str, env: Dict[str, str]) -> None:
        self.tool = tool
        self.project = f"emit-e2e-{tool}"
        self.env = {
            **os.environ,
            "EMIT_E2E_KEY": DEFAULT_KEY,
            "EMIT_E2E_PORT": os.environ.get("EMIT_E2E_PORT", "18080"),
            **env,
        }
        self.compose_file = HERE / tool / "compose.yml"

    def _compose(self, *args: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
        cmd = ["docker", "compose", "-p", self.project, "-f", str(self.compose_file), *args]
        print("+", " ".join(cmd), flush=True)
        result = subprocess.run(
            cmd, cwd=HERE, env=self.env, check=False, text=True,
            capture_output=capture,
        )
        if check and result.returncode != 0:
            if capture:
                print(result.stdout, flush=True)
                print(result.stderr, file=sys.stderr, flush=True)
            raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
        return result

    def up(self, *services: str) -> None:
        self._compose("up", "--build", "--detach", "--wait", *services)

    def run(self, service: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return self._compose("run", "--rm", "--build", service, *args, check=check)

    def exec(self, service: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return self._compose("exec", "-T", service, *args, check=check, capture=True)

    def logs(self, *services: str) -> str:
        return self._compose("logs", "--no-color", *services, check=False, capture=True).stdout

    def down(self) -> None:
        self._compose("down", "--volumes", "--remove-orphans", "--timeout", "10", check=False)

    @property
    def receiver_url(self) -> str:
        return f"http://127.0.0.1:{self.env['EMIT_E2E_PORT']}"

    def received(self) -> Dict[str, Any]:
        with urllib.request.urlopen(self.receiver_url + "/observations", timeout=5) as resp:
            return json.load(resp)

    def observations(self) -> List[Dict[str, Any]]:
        return [row["observation"] for row in self.received()["observations"]]

    def wait_for(
        self,
        ready: Callable[[List[Dict[str, Any]]], bool],
        timeout: float,
        what: str,
        also_poll: Optional[Callable[[], None]] = None,
    ) -> List[Dict[str, Any]]:
        """Poll the receiver until `ready` holds or `timeout` seconds pass."""
        deadline = time.monotonic() + timeout
        last: List[Dict[str, Any]] = []
        while time.monotonic() < deadline:
            try:
                last = self.observations()
            except (urllib.error.URLError, OSError):
                last = []
            if ready(last):
                return last
            if also_poll is not None:
                also_poll()
            time.sleep(2)
        raise AssertionError(
            f"timed out after {timeout:.0f}s waiting for {what}; "
            f"received {len(last)} observation(s): {sorted({o.get('event') for o in last})}"
        )


def events(observations: List[Dict[str, Any]]) -> List[str]:
    return sorted({str(o.get("event")) for o in observations})


def dump(observations: List[Dict[str, Any]]) -> str:
    return json.dumps(observations, default=str)

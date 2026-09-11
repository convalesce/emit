"""
Drives one tool's compose stack and reads what reached the receiver.

Every stack test does the same three things: bring the stack up, make the
tool run its example workflow, and wait for the observations that workflow
should have produced. `Stack` holds the first and the last; `StackCase` is
the test base that owns a stack for the life of one test and prints the
containers' logs when the test fails, because a timeout with no logs is
undiagnosable on a CI runner.

Import as:

import harness as e2eharn
"""

import contextlib
import json
import logging
import os
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

_LOG = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

# Not a secret: the receiver checks that a plugin sends *this* key, which is
# what proves the Authorization header is built.
DEFAULT_KEY = "e2e-ingest-key-not-a-secret"
DEFAULT_PORT = "18080"
# How long between polls of the receiver.
POLL_SECONDS = 2.0

Observation = Dict[str, Any]
Ready = Callable[[List[Observation]], bool]


# #############################################################################
# Stack
# #############################################################################


class Stack:
    """
    One docker compose project: a tool plus the receiver.

    :param tool: the directory under `e2e/stacks/` holding the compose file
    :param env: variables the compose file interpolates, on top of the
        process environment
    """

    def __init__(self, tool: str, env: Dict[str, str]) -> None:
        self.tool = tool
        self.project = f"emit-e2e-{tool}"
        self.env: Dict[str, str] = {
            **os.environ,
            "EMIT_E2E_KEY": DEFAULT_KEY,
            "EMIT_E2E_PORT": os.environ.get("EMIT_E2E_PORT", DEFAULT_PORT),
            **env,
        }
        self.compose_file = HERE / "stacks" / tool / "compose.yml"

    def up(self, *services: str) -> None:
        """
        Build and start services, waiting for their health checks.

        :param services: which to start; all when empty
        :return: nothing
        """
        self._compose("up", "--build", "--detach", "--wait", *services)

    def run(self, service: str, *args: str) -> None:
        """
        Run a one-shot service to completion, rebuilding its image first.

        Rebuilt every time: `compose run` on its own reuses whatever image
        the project last built, which silently ran one version's job on the
        previous version's image.

        :param service: the service to run
        :param args: arguments for its command
        :return: nothing
        :raises subprocess.CalledProcessError: if it exits non-zero
        """
        self._compose("run", "--rm", "--build", service, *args)

    def exec(
        self, service: str, *args: str, check: bool = True
    ) -> "subprocess.CompletedProcess[str]":
        """
        Run a command inside a running service.

        :param service: the service to run it in
        :param args: the command
        :param check: whether a non-zero exit raises
        :return: the completed process, output captured
        """
        return self._compose(
            "exec", "-T", service, *args, check=check, capture=True
        )

    def logs(self, *services: str) -> str:
        """
        Read the containers' logs so far.

        :param services: which to read; all when empty
        :return: the combined log text
        """
        result = self._compose(
            "logs", "--no-color", *services, check=False, capture=True
        )
        return result.stdout

    def down(self) -> None:
        """
        Stop and remove everything the project created.

        :return: nothing
        """
        self._compose(
            "down",
            "--volumes",
            "--remove-orphans",
            "--timeout",
            "10",
            check=False,
        )

    @property
    def receiver_url(self) -> str:
        """
        Where the receiver is published on this host.

        :return: the base URL
        """
        return f"http://127.0.0.1:{self.env['EMIT_E2E_PORT']}"

    def received(self) -> Dict[str, Any]:
        """
        Read everything the receiver recorded.

        :return: the receiver's `observations` and `rejected` lists
        """
        with urllib.request.urlopen(
            self.receiver_url + "/observations", timeout=5
        ) as response:
            data: Dict[str, Any] = json.load(response)
        return data

    def observations(self) -> List[Observation]:
        """
        The observations received so far, without receiver metadata.

        :return: the envelopes as the plugins sent them
        """
        return [row["observation"] for row in self.received()["observations"]]

    def wait_for(
        self, ready: Ready, timeout: float, what: str
    ) -> List[Observation]:
        """
        Poll the receiver until `ready` holds.

        :param ready: judged on the observations received so far
        :param timeout: seconds to keep polling
        :param what: named in the failure, so a timeout says what it
            waited for
        :return: the observations that satisfied `ready`
        :raises AssertionError: if the timeout passes first
        """
        deadline = time.monotonic() + timeout
        last: List[Observation] = []
        while time.monotonic() < deadline:
            try:
                last = self.observations()
            except (urllib.error.URLError, OSError):
                # The receiver is not up yet, or is restarting.
                last = []
            if ready(last):
                return last
            time.sleep(POLL_SECONDS)
        raise AssertionError(
            f"timed out after {timeout:.0f}s waiting for {what}; received "
            f"{len(last)} observation(s): {events(last)}"
        )

    def _compose(
        self, *args: str, check: bool = True, capture: bool = False
    ) -> "subprocess.CompletedProcess[str]":
        """
        Run one docker compose command against this project.

        :param args: the compose subcommand and its arguments
        :param check: whether a non-zero exit raises
        :param capture: whether to capture output instead of streaming it
        :return: the completed process
        :raises subprocess.CalledProcessError: on a non-zero exit when
            `check` is set, after printing any captured output
        """
        cmd = [
            "docker",
            "compose",
            "-p",
            self.project,
            "-f",
            str(self.compose_file),
            *args,
        ]
        print("+", " ".join(cmd), flush=True)
        result = subprocess.run(
            cmd,
            cwd=HERE,
            env=self.env,
            check=False,
            text=True,
            capture_output=capture,
        )
        if check and result.returncode != 0:
            if capture:
                print(result.stdout, flush=True)
                print(result.stderr, file=sys.stderr, flush=True)
            raise subprocess.CalledProcessError(
                result.returncode, cmd, result.stdout, result.stderr
            )
        return result


# #############################################################################
# StackCase
# #############################################################################


class StackCase(unittest.TestCase):
    """
    A test that owns one stack.

    A subclass names its `TOOL` and says how a version becomes compose
    variables. The version comes from `EMIT_E2E_<TOOL>`; when that is unset
    the test is skipped, so `pytest e2e` with nothing set runs nothing and
    CI sets exactly one.
    """

    TOOL = ""

    def setUp(self) -> None:
        variable = f"EMIT_E2E_{self.TOOL.upper()}"
        version = os.environ.get(variable)
        if not version:
            self.skipTest(f"{variable} is not set")
        self.version = version
        self.stack = Stack(self.TOOL, self.compose_env(version))
        self.stack.down()
        if os.environ.get("EMIT_E2E_KEEP") != "1":
            self.addCleanup(self.stack.down)

    @classmethod
    def compose_env(cls, version: str) -> Dict[str, str]:
        """
        Turn a tool version into the variables its compose file reads.

        :param version: the tool version under test
        :return: compose variables
        """
        raise NotImplementedError

    @contextlib.contextmanager
    def logs_on_failure(self) -> Iterator[None]:
        """
        Print the containers' logs if the body raises.

        :return: a context to run the test body in
        """
        try:
            yield
        except BaseException:
            print("\n===== container logs =====", file=sys.stderr)
            print(self.stack.logs(), file=sys.stderr, flush=True)
            raise

    def assert_envelopes(
        self, observations: List[Observation], tool: str
    ) -> None:
        """
        Check what every stack must send, whatever the tool.

        :param observations: what the receiver recorded
        :param tool: the tool name each envelope must carry
        :return: nothing
        """
        self.assertTrue(observations, "nothing was received")
        for observation in observations:
            self.assertEqual(observation["tool"], tool, observation)
            self.assertEqual(
                observation["tool_version"], self.version, observation
            )
            self.assertTrue(observation["client_version"], observation)
            self.assertNotIn("workspace", observation)
        rejected = self.stack.received()["rejected"]
        self.assertEqual(rejected, [], "the receiver refused a key")


def events(observations: List[Observation]) -> List[str]:
    """
    The distinct event names in a batch of observations.

    :param observations: what the receiver recorded
    :return: sorted event names
    """
    return sorted({str(obs.get("event")) for obs in observations})


def wire(observations: List[Observation]) -> str:
    """
    The observations as the JSON that crossed the wire.

    :param observations: what the receiver recorded
    :return: one JSON document, for substring checks
    """
    return json.dumps(observations, default=str)


def find_version_parts(version: str) -> "tuple[int, int]":
    """
    The major and minor of a version string.

    :param version: such as "2.10.5"
    :return: (major, minor)
    """
    major, minor = (int(part) for part in version.split(".")[:2])
    return major, minor


def optional(name: str, fallback: Optional[str] = None) -> Optional[str]:
    """
    Read an environment variable that may be absent.

    :param name: the variable
    :param fallback: what to return when it is unset
    :return: its value, or the fallback
    """
    return os.environ.get(name, fallback)

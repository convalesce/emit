"""
Dagster retry executor: re-executes a claimed run via the webserver's own
GraphQL API.

Not wired into `sensor.py`, and never fired just because a run-status
sensor is registered: this is a separate, opt-in entrypoint --
`run_pending_retries()` -- the customer schedules themselves, per
`plan/04-retry-remedy-kind.md`'s Phase 4.

**A real blocker the plan's own text did not survive contact with, found
before writing anything against it and adapted rather than guessed
around:** the plan named `DagsterGraphQLClient.reexecute_run(run_id)` as
the call to make. That method does not exist -- confirmed by reading the
real `dagster-graphql` package source
(`dagster_graphql/client/client.py`), which exposes exactly
`submit_job_execution`, `get_run_status`, `reload_repository_location`,
`shutdown_repository_location` and `terminate_run(s)`, nothing named
`reexecute_run`. The GraphQL mutation itself is real -- `dagster_graphql/
schema/roots/mutation.py`'s `launchPipelineReexecution` (an alias of
`GrapheneLaunchRunReexecutionMutation`, also exposed as
`launchRunReexecution`), taking a `ReexecutionParams` input
(`dagster_graphql/schema/inputs.py`: `parentRunId`, a required `strategy`
of `FROM_ASSET_FAILURE`/`FROM_FAILURE`/`ALL_STEPS`) -- it is simply not
wrapped by the typed client's own convenience methods. This module calls
that mutation directly over HTTP, using the exact query text the official
client itself issues internally (`dagster_graphql/client/query.py`'s
`REEXECUTE_PIPELINE_MUTATION`), rather than depending on the
`dagster-graphql` package at all -- which also keeps this plugin at zero
new runtime dependencies, the same discipline every other module in this
distribution already holds to. `FROM_FAILURE` is the strategy used here:
it re-executes only the failed step and its downstream steps, the same
scope Dagster's own UI "Re-execute from failure" action offers, and the
closest real match to what "retry" means for a single failed run.

Fail-closed at every step, the deliberate inverse of the sensor's own
"never let a read stop the pipeline" stance: a `run_id` this webserver
does not recognize -- checked with a `runOrError` query before `claim()`
is ever called -- is skipped, never claimed, because claiming burns the
tenant's single shot even when this process cannot act on it. Any failure
calling the webserver's GraphQL API after a claim is reported back as
`FAILED_TO_TRIGGER`, never silently retried.

The credentials used to reach the webserver are `CONVALESCE_DAGSTER_
RETRY_HOST` and `CONVALESCE_DAGSTER_RETRY_TOKEN` -- a separate, narrowly-
scoped, network-reached credential, never the sensor's in-process
`DagsterInstance`, and never `CONVALESCE_API_KEY`/`CONVALESCE_INGEST_KEY`
(those talk to collect; this talks to Dagster's webserver). Sent as a
bearer token, matching this distribution's own convention elsewhere --
documented as an assumption, not verified against a live Dagster+ or
reverse-proxied deployment: Dagster OSS's webserver GraphQL API carries
no authentication of its own by default, so what actually enforces this
token is whatever the customer puts in front of it.

Import as:

import convalesce_emit_dagster.retry as cedagretry
"""

import dataclasses
import json
import logging
import os
import socket
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

import convalesce_emit as cemit
import convalesce_emit.retry as ceretry
import convalesce_emit_dagster._version as cedagsterver

TOOL = "dagster"

_LOG = logging.getLogger(__name__)

_HOST_ENV = "CONVALESCE_DAGSTER_RETRY_HOST"
_TOKEN_ENV = "CONVALESCE_DAGSTER_RETRY_TOKEN"

# FROM_FAILURE re-executes only the failed step and everything downstream
# of it -- Dagster's own "Re-execute from failure" scope, and the closest
# real match to a single failed run's retry.
_STRATEGY = "FROM_FAILURE"

_RUN_EXISTS_QUERY = """
query($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run {
      runId
      status
    }
    ... on RunNotFoundError {
      message
    }
  }
}
"""

# The same query text the real `dagster-graphql` client issues internally
# for this mutation (`dagster_graphql/client/query.py`'s
# REEXECUTE_PIPELINE_MUTATION), confirmed against the real source rather
# than assumed -- see the module docstring's blocker note.
_REEXECUTE_MUTATION = """
mutation($reexecutionParams: ReexecutionParams) {
  launchPipelineReexecution(reexecutionParams: $reexecutionParams) {
    __typename
    ... on LaunchRunSuccess {
      run {
        runId
        status
      }
    }
    ... on RunNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""


# #############################################################################
# _RetryTarget
# #############################################################################


@dataclasses.dataclass(frozen=True)
class _RetryTarget:
    """
    Where and how to call this webserver's GraphQL API.

    :param base_url: the webserver's base URL, no trailing slash
    :param token: sent as a bearer token; never logged
    """

    base_url: str
    token: str


def _read_target() -> Optional[_RetryTarget]:
    """
    Resolve the retry credential, failing closed on anything missing.

    :return: the target to call, or None when the host or token is unset
    """
    host = os.environ.get(_HOST_ENV)
    token = os.environ.get(_TOKEN_ENV)
    if not host or not host.strip():
        _LOG.warning(
            "convalesce: %s is not set; not executing dagster retries",
            _HOST_ENV,
        )
        return None
    if not token or not token.strip():
        _LOG.warning(
            "convalesce: %s is not set; not executing dagster retries",
            _TOKEN_ENV,
        )
        return None
    return _RetryTarget(
        base_url=_normalize_host(host.strip()), token=token.strip()
    )


def _normalize_host(host: str) -> str:
    """
    Turn a configured host into a base URL, no trailing slash.

    :param host: whatever `CONVALESCE_DAGSTER_RETRY_HOST` names; a bare
        host is assumed to be plaintext http, since this credential is
        meant to reach a private, already-authenticated endpoint, not a
        public one
    :return: the base URL
    """
    if host.startswith("http://") or host.startswith("https://"):
        return host.rstrip("/")
    return f"http://{host.rstrip('/')}"


# #############################################################################
# GraphQL transport
# #############################################################################


def _call_graphql(
    target: _RetryTarget,
    query: str,
    variables: Dict[str, Any],
    *,
    timeout: float,
) -> Dict[str, Any]:
    """
    Post one GraphQL request to the webserver.

    :param target: where and how to call it
    :param query: the query or mutation text
    :param variables: its variables
    :param timeout: seconds to wait on the request
    :return: the response's `data` object
    :raises RuntimeError: if the response carries a top-level `errors`
        array, or `data` is missing
    :raises Exception: on anything that means the request never got a
        response at all -- unreachable, timed out, refused, or a non-2xx
        status
    """
    url = target.base_url + "/graphql"
    body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {target.token}",
        "Content-Type": "application/json",
        "User-Agent": f"convalesce-emit-dagster/{cedagsterver.__version__}",
    }
    request = urllib.request.Request(  # nosec B310
        url, data=body, headers=headers, method="POST"
    )
    with urllib.request.urlopen(
        request, timeout=timeout
    ) as response:  # nosec B310
        parsed = json.loads(response.read())
    errors = parsed.get("errors")
    if errors:
        raise RuntimeError(f"dagster graphql returned errors: {errors!r}")
    data = parsed.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("dagster graphql response had no 'data' object")
    return data


# #############################################################################
# local scope
# #############################################################################


def run_exists_locally(
    target: _RetryTarget, run_id: str, *, timeout: float
) -> bool:
    """
    Whether this webserver's own instance recognizes a run id.

    The local-scope check the plan requires before ever calling `claim()`:
    a shared collect endpoint serves every deployment in the tenant, and
    only this webserver's own instance can say a run is actually its own.

    :param target: where and how to call this webserver
    :param run_id: the run id a candidate names
    :return: True only when `runOrError` resolves a real `Run`; False on
        a `RunNotFoundError`, or on any exception, timeout or malformed
        response -- "could not tell" and "yes, it is mine" are not the
        same answer
    """
    try:
        data = _call_graphql(
            target, _RUN_EXISTS_QUERY, {"runId": run_id}, timeout=timeout
        )
        run_or_error = data.get("runOrError", {})
        return bool(run_or_error.get("__typename") == "Run")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not check run %s locally: %s", run_id, exc
        )
        return False


# #############################################################################
# the native call
# #############################################################################


def _reexecute_run(target: _RetryTarget, run_id: str, *, timeout: float) -> str:
    """
    Re-execute one run from its point of failure.

    :param target: where and how to call this webserver
    :param run_id: the run to re-execute
    :param timeout: seconds to wait on the request
    :return: the new run's id
    :raises RuntimeError: on anything short of a `LaunchRunSuccess`
    :raises Exception: on anything that means the request never got a
        response at all
    """
    data = _call_graphql(
        target,
        _REEXECUTE_MUTATION,
        {"reexecutionParams": {"parentRunId": run_id, "strategy": _STRATEGY}},
        timeout=timeout,
    )
    result = data.get("launchPipelineReexecution", {})
    if result.get("__typename") == "LaunchRunSuccess":
        new_run_id = result.get("run", {}).get("runId")
        if isinstance(new_run_id, str) and new_run_id:
            return new_run_id
        raise RuntimeError("launchPipelineReexecution succeeded with no runId")
    raise RuntimeError(
        f"launchPipelineReexecution returned {result.get('__typename')}: "
        f"{result.get('message')}"
    )


# #############################################################################
# run_pending_retries
# #############################################################################


@dataclasses.dataclass(frozen=True)
class RetryRunSummary:
    """
    What one call to `run_pending_retries` did.

    :param considered: dagster-tool candidates collect listed
    :param claimed: candidates this process actually claimed
    :param triggered: claimed candidates the webserver re-executed
    :param failed: claimed candidates whose re-execution call failed
    :param skipped: candidates left alone -- out of local scope, missing
        coordinates, no retry credential, or lost the claim race
    """

    considered: int = 0
    claimed: int = 0
    triggered: int = 0
    failed: int = 0
    skipped: int = 0


def _default_owner() -> str:
    """
    A claim owner a human debugging a stuck remedy would find useful.

    :return: this host's name, or a fixed fallback when it cannot be read
    """
    try:
        return socket.gethostname()
    except Exception:  # pylint: disable=broad-exception-caught
        return "convalesce-emit-dagster"


def run_pending_retries(
    *,
    config: Optional[cemit.Config] = None,
    owner: Optional[str] = None,
    timeout: float = 10.0,
) -> RetryRunSummary:
    """
    Poll collect for approved dagster retries and execute what this
    webserver is responsible for.

    Wire this into your own scheduled job -- a cron entry, never a
    sensor::

        from convalesce_emit_dagster.retry import run_pending_retries

        run_pending_retries()

    :param config: where collect is and how hard to try; read from the
        environment when not given
    :param owner: identifies this process's claims to a human debugging a
        stuck remedy later; this host's name when not given
    :param timeout: seconds to wait on each webserver GraphQL call
    :return: a summary of what happened
    """
    cfg = config or cemit.Config.from_env()
    candidates = [c for c in ceretry.list_pending(config=cfg) if c.tool == TOOL]
    considered = len(candidates)
    if not candidates:
        return RetryRunSummary(considered=considered)

    target = _read_target()
    if target is None:
        return RetryRunSummary(considered=considered, skipped=considered)

    claim_owner = owner or _default_owner()
    claimed = triggered = failed = skipped = 0
    for candidate in candidates:
        run_id = candidate.coordinates.get("run_id")
        if not run_id:
            _LOG.warning(
                "convalesce: retry %s is missing run_id; skipping",
                candidate.remedy_id,
            )
            skipped += 1
            continue
        if not run_exists_locally(target, run_id, timeout=timeout):
            _LOG.info(
                "convalesce: run %s is not known to this webserver; "
                "leaving retry %s to whoever owns it",
                run_id,
                candidate.remedy_id,
            )
            skipped += 1
            continue
        if not ceretry.claim(candidate.remedy_id, owner=claim_owner, config=cfg):
            skipped += 1
            continue
        claimed += 1
        try:
            native_ref = _reexecute_run(target, run_id, timeout=timeout)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.warning("convalesce: could not re-execute %s: %s", run_id, exc)
            failed += 1
            ceretry.report_outcome(
                candidate.remedy_id,
                outcome=ceretry.FAILED_TO_TRIGGER,
                error=str(exc)[:500],
                config=cfg,
            )
            continue
        triggered += 1
        ceretry.report_outcome(
            candidate.remedy_id,
            outcome=ceretry.TRIGGERED,
            native_run_ref=native_ref,
            config=cfg,
        )

    return RetryRunSummary(
        considered=considered,
        claimed=claimed,
        triggered=triggered,
        failed=failed,
        skipped=skipped,
    )

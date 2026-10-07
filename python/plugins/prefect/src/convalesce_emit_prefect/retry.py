"""
Prefect retry executor: reschedules a claimed flow run via the Prefect
API's own `set_state` endpoint.

Not wired into `hooks.py`, and never fired just because a state hook is
registered: this is a separate, opt-in entrypoint --
`run_pending_retries()` -- the customer schedules themselves, per
`plan/04-retry-remedy-kind.md`'s Phase 4.

**The one call the plan flagged for empirical confirmation before
shipping.** The plan named `PrefectClient.set_flow_run_state(flow_run_id,
state=Scheduled(...))` -- the same action Prefect's own UI "Retry" button
performs. That method is real, confirmed by reading the real `prefect`
package source (`prefect/client/orchestration/_flow_runs/client.py`'s
`set_flow_run_state`), and this module calls the exact REST endpoint it
wraps directly (`POST /flow_runs/{id}/set_state`) rather than depending on
the `prefect` package -- zero new runtime dependencies, the same
discipline every module in this distribution holds to. What follows is
confirmed against that real source, field for field:

- The request body is `{"state": <StateCreate>, "force": false}`, where
  `StateCreate` (`prefect/client/schemas/actions.py`) needs only `type:
  "SCHEDULED"` and a `state_details.scheduled_time`; `name` is left unset,
  the same as `to_state_create()`'s own default, and the server fills it
  in. `state_details.flow_run_id` and a fresh `transition_id` are set the
  same way `set_flow_run_state()` itself sets them
  (`prefect/client/schemas/objects.py`'s `StateDetails` carries both
  fields for real).
- The response is an `OrchestrationResult` (`prefect/client/schemas/
  responses.py`): `status` is one of `ACCEPT`/`REJECT`/`ABORT`/`WAIT`
  (`SetStateStatus` is a plain `AutoEnum`, so those are the literal wire
  values). Only `ACCEPT` is treated as success; `REJECT`/`ABORT`/`WAIT`
  all fail closed to `FAILED_TO_TRIGGER`, never retried in the same pass.

**Live-verified against a real Prefect 3.8.5 server (the version this
project's own `collect/e2e-observe/stacks/prefect` matrix already pins),
not just against source -- see `plan/04-retry-remedy-kind.md`'s closing
state section for the full run.** A real bug was caught doing this,
fixed here: a successful `ACCEPT`ed transition comes back as HTTP **201**,
not 200 -- confirmed with a direct `curl` against a real server before
touching the code, and independently reproduced through this exact
function. The pre-existing code treated anything but 200 as a transport
failure, which meant every real, successful reschedule was misreported as
`FAILED_TO_TRIGGER`; the unit tests never caught it because their fake
server's default status was 200 for every scenario, including the
`ACCEPT` one. A rejected/aborted transition (e.g. no deployment, a
terminal state that refuses this transition) comes back as HTTP 200 with
`status: "ABORT"`/`"REJECT"` in the body -- also confirmed live, calling
`set_state` on a real terminal, no-deployment flow run and getting back
exactly `{"status": "ABORT", "details": {"reason": "Cannot reschedule a
run without an associated deployment."}}` at HTTP 200. A nonexistent flow
run's id returns a real 404. The fix: treat both 200 and 201 as "the
request was processed, now look at the body's own `status` field" and
only 404 as the not-found case, unchanged from before.

A flow run with no `deployment_id` has no worker or work pool watching
for a `SCHEDULED` state to pick up, so setting one would leave it
permanently stuck rather than actually retried -- confirmed by reading
`FlowRun.deployment_id`'s own docstring (`prefect/client/schemas/
objects.py`: "the id of the deployment associated with this flow run, if
available"). This is the local-scope check here: a flow run this
process's own read of `GET /flow_runs/{id}` shows has no deployment is
skipped, never claimed, fail-closed exactly as the plan's own reasoning
requires ("a flow run with no deployment/work pool can't be rescheduled
this way; fail closed").

The retry flow is a flow run like any other, so it already holds what
reaches its own Prefect API: `PREFECT_API_URL`, and `PREFECT_API_KEY` on
Prefect Cloud or `PREFECT_API_AUTH_STRING` on a secured server. Those are
what is used. `CONVALESCE_PREFECT_RETRY_API_URL`,
`CONVALESCE_PREFECT_RETRY_API_KEY` and
`CONVALESCE_PREFECT_RETRY_AUTH_STRING` replace them one by one, for a
deployment that wants retries to sign in as somebody else. Never the key
that talks to collect: this talks to Prefect's own API. A key is sent as
a bearer token, which is how Prefect Cloud's API authenticates.

Import as:

import convalesce_emit_prefect.retry as ceprefretry
"""

import base64
import dataclasses
import json
import logging
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit.retry as ceretry
import convalesce_emit_prefect._version as ceprefectver

# Airflow, Dagster and Prefect each run the same poll/claim/act/report shape
# here, differing only in the native API each calls to act on a claim. Each
# plugin is independently installable with zero cross-plugin dependency, so
# sharing this would add a dependency for no real benefit; the similarity
# stays and the check is turned off here rather than everywhere.
# pylint: disable=duplicate-code

TOOL = "prefect"

_LOG = logging.getLogger(__name__)

_API_URL_ENV = "CONVALESCE_PREFECT_RETRY_API_URL"
_API_KEY_ENV = "CONVALESCE_PREFECT_RETRY_API_KEY"
# A self-hosted server that is secured takes `user:password`, the same value
# as Prefect's own `PREFECT_API_AUTH_STRING`. One that is not takes nothing.
_AUTH_STRING_ENV = "CONVALESCE_PREFECT_RETRY_AUTH_STRING"
_CLOUD_HOST_SUFFIX = "prefect.cloud"

# What a flow run already holds to reach its own Prefect API. The retry flow
# is such a run, so with nothing set for retries these are what it uses.
_AMBIENT = {
    _API_URL_ENV: "PREFECT_API_URL",
    _API_KEY_ENV: "PREFECT_API_KEY",
    _AUTH_STRING_ENV: "PREFECT_API_AUTH_STRING",
}


# #############################################################################
# _RetryTarget
# #############################################################################


@dataclasses.dataclass(frozen=True)
class _RetryTarget:
    """
    Where and how to call the Prefect API.

    :param base_url: the API's base URL, no trailing slash -- the same
        shape as `PREFECT_API_URL`, including `/api` and any Cloud
        account/workspace path segments
    :param api_key: a Prefect Cloud API key, sent as a bearer token;
        never logged
    :param auth_string: a self-hosted server's `user:password`, sent as
        basic auth when there is no key; never logged
    """

    base_url: str
    api_key: Optional[str] = None
    auth_string: Optional[str] = None


def _prefect_setting(name: str) -> Optional[str]:
    """
    One of Prefect's own settings, as this process holds it.

    The environment first, then Prefect's settings, which also cover a
    value that came from a profile. Imported here so this module still
    imports where Prefect is absent.

    :param name: the setting, such as `PREFECT_API_URL`
    :return: its value, or None when it is unset or cannot be read
    """
    value: Any = os.environ.get(name)
    if not value:
        try:
            import prefect.settings as settings  # pylint: disable=import-outside-toplevel

            value = getattr(settings, name).value()
        except Exception:  # pylint: disable=broad-exception-caught
            return None
    reveal = getattr(value, "get_secret_value", None)
    if callable(reveal):
        value = reveal()
    text = str(value).strip() if value else ""
    return text or None


def _setting(name: str) -> Optional[str]:
    """
    A retry setting: the one set for retries, else what the flow run
    already uses for the same thing.

    :param name: the retry setting's own variable
    :return: its value, or None when neither is set
    """
    own = os.environ.get(name)
    if own and own.strip():
        return own.strip()
    return _prefect_setting(_AMBIENT[name])


def _read_target() -> Optional[_RetryTarget]:
    """
    Resolve where the Prefect API is and how to sign in, failing closed
    on anything missing.

    :return: the target to call, or None when no address is known, or it
        is Prefect Cloud's and no key is
    """
    url = _setting(_API_URL_ENV)
    api_key = _setting(_API_KEY_ENV)
    if not url:
        _LOG.warning(
            "convalesce: neither %s nor PREFECT_API_URL is set; not "
            "executing prefect retries",
            _API_URL_ENV,
        )
        return None
    base_url = url.rstrip("/")
    auth_string = _setting(_AUTH_STRING_ENV)
    host = urllib.parse.urlparse(base_url).hostname or ""
    if api_key is None and host.endswith(_CLOUD_HOST_SUFFIX):
        _LOG.warning(
            "convalesce: neither %s nor PREFECT_API_KEY is set; not "
            "executing prefect retries",
            _API_KEY_ENV,
        )
        return None
    return _RetryTarget(
        base_url=base_url, api_key=api_key, auth_string=auth_string
    )


def _sign_in(target: _RetryTarget) -> Dict[str, str]:
    """
    The header that signs a request in to this Prefect API, if it takes one.

    :param target: where and how to call it
    :return: the header, or nothing when the server is open
    """
    if target.api_key is not None:
        return {"Authorization": f"Bearer {target.api_key}"}
    if target.auth_string is not None:
        pair = base64.b64encode(target.auth_string.encode("utf-8"))
        return {"Authorization": "Basic " + pair.decode("ascii")}
    return {}


# #############################################################################
# transport
# #############################################################################


def _call(
    target: _RetryTarget,
    method: str,
    path: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: float,
) -> Tuple[int, bytes]:
    """
    Make one HTTP request against the Prefect API.

    :param target: where and how to call it
    :param method: HTTP method
    :param path: relative to `target.base_url`, leading slash included
    :param json_body: request body, when there is one
    :param timeout: seconds to wait on the request
    :return: the response status and body
    :raises Exception: on anything that means the request never got a
        response at all -- unreachable, timed out, refused
    """
    data = (
        json.dumps(json_body).encode("utf-8") if json_body is not None else None
    )
    headers = {
        **_sign_in(target),
        "User-Agent": f"convalesce-emit-prefect/{ceprefectver.__version__}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(  # nosec B310
        target.base_url + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(  # nosec B310
            request, timeout=timeout
        ) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


# #############################################################################
# local scope
# #############################################################################


def flow_run_is_reschedulable(
    target: _RetryTarget, flow_run_id: str, *, timeout: float
) -> bool:
    """
    Whether this flow run exists and can actually be rescheduled.

    The local-scope check the plan requires before ever calling `claim()`:
    a shared collect endpoint serves every tenant deployment, and only a
    real read against this exact Prefect API can say whether a flow run
    is both real and reschedulable.

    :param target: where and how to call the Prefect API
    :param flow_run_id: the flow run id a candidate names
    :return: True only when the flow run exists and has a `deployment_id`
        -- one Prefect's own scheduler will actually pick a `SCHEDULED`
        state up for. False on a 404, on a flow run with no deployment,
        or on any exception, timeout or malformed response
    """
    try:
        status, body = _call(
            target,
            "GET",
            f"/flow_runs/{urllib.parse.quote(flow_run_id, safe='')}",
            timeout=timeout,
        )
        if status != 200:
            return False
        flow_run = json.loads(body)
        if not isinstance(flow_run, dict):
            return False
        deployment_id = flow_run.get("deployment_id")
        if not deployment_id:
            _LOG.info(
                "convalesce: flow run %s has no deployment; it cannot be "
                "rescheduled this way",
                flow_run_id,
            )
            return False
        return True
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not check flow run %s: %s", flow_run_id, exc
        )
        return False


# #############################################################################
# the native call
# #############################################################################


def _reschedule_flow_run(
    target: _RetryTarget, flow_run_id: str, *, timeout: float
) -> str:
    """
    Move a flow run to `SCHEDULED`, the same transition Prefect's own UI
    "Retry" button performs.

    :param target: where and how to call the Prefect API
    :param flow_run_id: the flow run to reschedule
    :param timeout: seconds to wait on the request
    :return: `flow_run_id` itself -- unlike Dagster's re-execution, a
        rescheduled flow run keeps its own id, it does not get a new one
    :raises RuntimeError: on anything short of an `ACCEPT`ed transition
    :raises Exception: on anything that means the request never got a
        response at all
    """
    state = {
        "type": "SCHEDULED",
        "state_details": {
            "scheduled_time": datetime.now(timezone.utc).isoformat(),
            "flow_run_id": flow_run_id,
            "transition_id": str(uuid.uuid4()),
        },
    }
    status, body = _call(
        target,
        "POST",
        f"/flow_runs/{urllib.parse.quote(flow_run_id, safe='')}/set_state",
        json_body={"state": state, "force": False},
        timeout=timeout,
    )
    if status == 404:
        raise RuntimeError(f"flow run {flow_run_id} was not found")
    if status not in (200, 201):
        raise RuntimeError(f"set_state returned unexpected HTTP {status}")
    result = json.loads(body)
    if not isinstance(result, dict) or result.get("status") != "ACCEPT":
        raise RuntimeError(
            f"set_state returned {result.get('status') if isinstance(result, dict) else None}: "
            f"{result.get('details') if isinstance(result, dict) else body[:200]!r}"
        )
    return flow_run_id


# #############################################################################
# run_pending_retries
# #############################################################################


@dataclasses.dataclass(frozen=True)
class RetryRunSummary:
    """
    What one call to `run_pending_retries` did.

    :param considered: prefect-tool candidates collect listed
    :param claimed: candidates this process actually claimed
    :param triggered: claimed candidates the API accepted a SCHEDULED
        transition for
    :param failed: claimed candidates whose reschedule call failed
    :param skipped: candidates left alone -- no deployment to reschedule
        onto, missing coordinates, no retry credential, or lost the claim
        race
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
        return "convalesce-emit-prefect"


def run_pending_retries(
    *,
    config: Optional[cemit.Config] = None,
    owner: Optional[str] = None,
    timeout: float = 10.0,
) -> RetryRunSummary:
    """
    Poll collect for approved prefect retries and execute what this
    process is responsible for.

    Wire this into your own scheduled job -- a cron entry, never a state
    hook::

        from convalesce_emit_prefect.retry import run_pending_retries

        run_pending_retries()

    :param config: where collect is and how hard to try; read from the
        environment when not given
    :param owner: identifies this process's claims to a human debugging a
        stuck remedy later; this host's name when not given
    :param timeout: seconds to wait on each Prefect API call
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
        flow_run_id = candidate.coordinates.get("flow_run_id")
        if not flow_run_id:
            _LOG.warning(
                "convalesce: retry %s is missing flow_run_id; skipping",
                candidate.remedy_id,
            )
            skipped += 1
            continue
        if not flow_run_is_reschedulable(target, flow_run_id, timeout=timeout):
            skipped += 1
            continue
        if not ceretry.claim(candidate.remedy_id, owner=claim_owner, config=cfg):
            skipped += 1
            continue
        claimed += 1
        try:
            native_ref = _reschedule_flow_run(
                target, flow_run_id, timeout=timeout
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.warning(
                "convalesce: could not reschedule flow run %s: %s",
                flow_run_id,
                exc,
            )
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

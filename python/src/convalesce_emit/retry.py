"""
Retrying: polling collect for approved retries, claiming one, and reporting
back what a tool's native API did.

Failure semantics, stated here because they are the deliberate inverse of
`gate.py`'s: `gate` fails open because blocking a pipeline is the unsafe
default for a read. Retrying is a write -- clearing a task, re-executing a
run, rescheduling a flow -- so this module fails closed instead. Every
function here returns "nothing happened" (an empty list, or `False`) on any
exception, timeout, non-2xx status, or malformed response, and none of them
ever raises for a transport problem. The one exception that is not
swallowed is a caller passing an `outcome` this module does not recognize --
that is a bug in the caller's own code, not a transport failure, the same
distinction `record.py` already draws for its `event` argument.

This reads an `api`-scoped key (`CONVALESCE_API_KEY`) -- the same key and
the same server-side actor `gate.py` already uses -- never the `ingest`
key. The credential that actually performs a tool's own native retry call
is a separate one again, resolved and used entirely inside each plugin's
own sibling `retry.py`, never here and never this key.

This module never claims a coordinate on its own initiative: the caller
(a plugin's own `retry.py`) must confirm a listed coordinate is one it is
actually responsible for -- e.g. an Airflow DAG in its own DagBag -- before
ever calling `claim()`. Claiming burns the tenant's single shot even if
this process turns out unable to act on it.

Import as:

import convalesce_emit.retry as ceretry
"""

import dataclasses
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import convalesce_emit._version as ceversio
import convalesce_emit.config as ceconfig

_LOG = logging.getLogger(__name__)

_RETRIES_PATH = "/v1/retries"

# The two outcomes collect's outcome endpoint accepts. Matched against the
# GMS-side `IncidentRemedyRetryOutcome` enum exactly -- see
# `plan/04-retry-remedy-kind.md`'s Phase 2/3 state sections.
TRIGGERED = "TRIGGERED"
FAILED_TO_TRIGGER = "FAILED_TO_TRIGGER"
_VALID_OUTCOMES = frozenset({TRIGGERED, FAILED_TO_TRIGGER})


# #############################################################################
# RetryCandidate
# #############################################################################


@dataclasses.dataclass(frozen=True)
class RetryCandidate:
    """
    One approved, not-yet-claimed retry, as collect reported it.

    :param remedy_id: the remedy to claim and report against
    :param incident_urn: the incident this remedy belongs to
    :param tool: "airflow", "dagster" or "prefect" -- collect never
        proposes a retry for any other tool
    :param coordinates: bare, unnamespaced keys the owning tool's own
        translator captured, e.g. `dag_id`/`task_id`/`run_id`/`map_index`
        for Airflow, `run_id` for Dagster, `flow_run_id` for Prefect
    :param created_at_millis: when the remedy was approved
    """

    remedy_id: str
    incident_urn: str
    tool: str
    coordinates: Dict[str, str]
    created_at_millis: int


def list_pending(  # pylint: disable=too-many-return-statements
    *, config: Optional[ceconfig.Config] = None
) -> List[RetryCandidate]:
    """
    Ask collect which approved retries are waiting to be claimed.

    :param config: where to send and how hard to try; read from the
        environment when not given
    :return: pending retries, or `[]` on any failure -- disabled, dry-run,
        no api key, unreachable, timed out, a non-2xx status, or a body
        that does not parse as the expected shape. A single malformed row
        inside an otherwise well-formed response is skipped and logged on
        its own, rather than discarding every other row alongside it.
    """
    cfg = config or ceconfig.Config.from_env()
    if not cfg.enabled or cfg.dry_run:
        return []
    if not cfg.api_key:
        _LOG.warning("convalesce: no api key configured; not listing retries")
        return []

    url = cfg.endpoint.rstrip("/") + _RETRIES_PATH
    try:
        status, body = _call(url, "GET", cfg)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # Deliberately broad, and deliberately not re-raised: unreachable,
        # timed out, refused -- every one of these means "no definite
        # list", which this function treats as "nothing to claim", not as
        # a reason to guess.
        _LOG.warning("convalesce: could not list pending retries: %s", exc)
        return []
    if status != 200:
        _LOG.warning("convalesce: listing retries returned HTTP %d", status)
        return []

    try:
        parsed = json.loads(body)
    except ValueError as exc:
        _LOG.warning("convalesce: retries list was not valid json: %s", exc)
        return []
    rows = parsed.get("retries") if isinstance(parsed, dict) else None
    if not isinstance(rows, list):
        _LOG.warning("convalesce: retries list had no 'retries' array")
        return []

    out: List[RetryCandidate] = []
    for row in rows:
        candidate = _parse_candidate(row)
        if candidate is None:
            _LOG.warning("convalesce: skipping a malformed pending retry row")
            continue
        out.append(candidate)
    return out


def _parse_candidate(  # pylint: disable=too-many-return-statements
    row: Any,
) -> Optional[RetryCandidate]:
    """
    Read one row of the `retries` list, or reject it outright.

    :param row: one element of the list endpoint's `retries` array
    :return: a candidate, or None when the row is missing a required
        field or has the wrong type for one
    """
    if not isinstance(row, dict):
        return None
    remedy_id = row.get("remedy_id")
    incident_urn = row.get("incident_urn")
    tool = row.get("tool")
    coordinates = row.get("coordinates")
    created_at_millis = row.get("created_at_millis")
    if not isinstance(remedy_id, str) or not remedy_id:
        return None
    if not isinstance(incident_urn, str) or not incident_urn:
        return None
    if not isinstance(tool, str) or not tool:
        return None
    if not isinstance(coordinates, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in coordinates.items()
    ):
        return None
    if not isinstance(created_at_millis, int):
        return None
    return RetryCandidate(
        remedy_id=remedy_id,
        incident_urn=incident_urn,
        tool=tool,
        coordinates=dict(coordinates),
        created_at_millis=created_at_millis,
    )


# #############################################################################
# claim
# #############################################################################


def claim(  # pylint: disable=too-many-return-statements
    remedy_id: str, *, owner: str, config: Optional[ceconfig.Config] = None
) -> bool:
    """
    Claim one pending retry, burning its single shot.

    :param remedy_id: the remedy to claim
    :param owner: identifies this claim to a human debugging a stuck
        `APPLYING` remedy later -- a hostname or job id, not a placeholder
    :param config: where to send and how hard to try
    :return: `True` only on a real, well-formed 201 acceptance; `False`
        for every other outcome -- a 409 (a race lost to another
        executor, the correct answer for the loser), a 404 (unknown
        remedy), or any exception, timeout or malformed response. The
        caller must not act on a tool's native API unless this returns
        `True`, and must never retry a claim call that returned `False`
    """
    cfg = config or ceconfig.Config.from_env()
    if not cfg.enabled or cfg.dry_run:
        return False
    if not cfg.api_key:
        _LOG.warning(
            "convalesce: no api key configured; not claiming %s", remedy_id
        )
        return False

    url = _remedy_url(cfg, remedy_id) + "/claim"
    try:
        status, body = _call(url, "POST", cfg, json_body={"owner": owner})
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning("convalesce: could not claim retry %s: %s", remedy_id, exc)
        return False

    if status == 201:
        try:
            parsed = json.loads(body)
        except ValueError as exc:
            _LOG.warning(
                "convalesce: claim response for %s was not valid json: %s",
                remedy_id,
                exc,
            )
            return False
        if isinstance(parsed, dict) and parsed.get("claimed") is True:
            return True
        _LOG.warning(
            "convalesce: claim response for %s did not confirm claimed=true",
            remedy_id,
        )
        return False
    if status == 409:
        _LOG.info(
            "convalesce: retry %s is already claimed; leaving it to whoever "
            "holds it",
            remedy_id,
        )
    elif status == 404:
        _LOG.warning("convalesce: no pending retry %s to claim", remedy_id)
    else:
        _LOG.warning(
            "convalesce: claiming retry %s returned HTTP %d", remedy_id, status
        )
    return False


# #############################################################################
# report_outcome
# #############################################################################


def report_outcome(
    remedy_id: str,
    *,
    outcome: str,
    native_run_ref: Optional[str] = None,
    error: Optional[str] = None,
    config: Optional[ceconfig.Config] = None,
) -> bool:
    """
    Report what happened after a claimed retry's native API call.

    Not `record.py`'s `record()`: that posts under the `ingest` key to
    `/v1/observations`, which lands in the translators built for a tool's
    own output, not for a state-machine mutation like this one. This posts
    under the `api` key to collect's dedicated outcome endpoint instead.

    :param remedy_id: the remedy this outcome belongs to; must already be
        claimed by this same caller
    :param outcome: `TRIGGERED` or `FAILED_TO_TRIGGER`, exactly
    :param native_run_ref: whatever the tool's own API returned identifying
        the retried run, where there is one
    :param error: what went wrong, when `outcome` is `FAILED_TO_TRIGGER`
    :param config: where to send and how hard to try
    :return: `True` only on a real 200 first report; `False` for every
        other outcome -- a 409 (already reported, including a second
        report that agrees with the first, or one sent before any claim
        exists), a 404, or any exception, timeout or malformed response.
        Never retried automatically on a `False`: a second attempt after
        a 409 would only ever 409 again, by design
    :raises ValueError: if `outcome` is not `TRIGGERED` or
        `FAILED_TO_TRIGGER` -- a mistake in the caller's own code, not a
        transport failure, and not swallowed for the same reason
        `record.record()` does not swallow an unrecognized `event`
    """
    if outcome not in _VALID_OUTCOMES:
        raise ValueError(
            f"outcome must be one of {sorted(_VALID_OUTCOMES)}, got {outcome!r}"
        )
    cfg = config or ceconfig.Config.from_env()
    if not cfg.enabled or cfg.dry_run:
        return False
    if not cfg.api_key:
        _LOG.warning(
            "convalesce: no api key configured; not reporting outcome for %s",
            remedy_id,
        )
        return False

    payload: Dict[str, Any] = {"outcome": outcome}
    if native_run_ref is not None:
        payload["native_run_ref"] = native_run_ref
    if error is not None:
        payload["error"] = error

    url = _remedy_url(cfg, remedy_id) + "/outcome"
    try:
        status, _body = _call(url, "POST", cfg, json_body=payload)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not report outcome for %s: %s", remedy_id, exc
        )
        return False

    if status == 200:
        return True
    if status == 409:
        _LOG.info(
            "convalesce: an outcome for %s was already reported; not retrying",
            remedy_id,
        )
    elif status == 404:
        _LOG.warning(
            "convalesce: no retry remedy %s to report against", remedy_id
        )
    else:
        _LOG.warning(
            "convalesce: reporting outcome for %s returned HTTP %d",
            remedy_id,
            status,
        )
    return False


# #############################################################################
# transport
# #############################################################################


def _remedy_url(cfg: ceconfig.Config, remedy_id: str) -> str:
    """
    The base URL for one remedy's claim/outcome endpoints.

    :param cfg: where collect is
    :param remedy_id: the remedy in question
    :return: `<endpoint>/v1/retries/<remedy_id>`, url-encoded
    """
    return (
        cfg.endpoint.rstrip("/")
        + _RETRIES_PATH
        + "/"
        + urllib.parse.quote(remedy_id, safe="")
    )


def _call(
    url: str,
    method: str,
    cfg: ceconfig.Config,
    *,
    json_body: Optional[Dict[str, Any]] = None,
) -> Tuple[int, bytes]:
    """
    Make one HTTP request against collect, as the `api` actor.

    Unlike `gate.py`'s equivalent, this returns the status on a non-2xx
    response instead of raising, because callers here need to branch on
    409/404 specifically rather than treat every non-2xx alike.

    :param url: full URL to call
    :param method: HTTP method
    :param cfg: where to send and how hard to try
    :param json_body: request body, when there is one
    :return: the response status and body
    :raises Exception: on anything that means the request never got a
        response at all -- unreachable, timed out, refused
    """
    data = (
        json.dumps(json_body).encode("utf-8") if json_body is not None else None
    )
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "User-Agent": f"convalesce-emit/{ceversio.__version__}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(  # nosec B310
        url, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(  # nosec B310
            request, timeout=cfg.timeout
        ) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()

"""
Airflow retry executor: clears a claimed task instance via Airflow's own
REST API.

Not wired into `plugin.py` or `listener.py`, and never fired just because
the listener is registered: this is a separate, opt-in entrypoint --
`run_pending_retries()` -- the customer schedules themselves, in their own
cron or a small maintenance DAG, per `plan/04-retry-remedy-kind.md`'s
Phase 4.

Fail-closed at every step, the deliberate inverse of the listener's own
"never let a read stop the pipeline" stance: a `dag_id` this process does
not recognize -- checked against this Airflow's own `DagBag` -- is
skipped, never claimed, because claiming burns the tenant's single shot
even when this process cannot act on it. Any failure calling Airflow's own
REST API after a claim is reported back as `FAILED_TO_TRIGGER`, never
silently retried; nothing here loops or re-claims on its own.

The credential used to call Airflow's REST API is
`CONVALESCE_AIRFLOW_RETRY_TOKEN` -- an Airflow connection id, resolved via
`BaseHook.get_connection` the same way `listener.py`'s own
`connection_coordinates()` resolves one -- never the listener's ambient
in-process access, and never `CONVALESCE_API_KEY`/`CONVALESCE_INGEST_KEY`
(those talk to collect; this talks to Airflow itself).

Airflow's REST base path for clearing task instances moved between majors
-- confirmed against Airflow's own source, not guessed: `/api/v1/dags/
{dag_id}/clearTaskInstances` through Airflow 2, `/api/v2/dags/{dag_id}/
clearTaskInstances` from Airflow 3 (`airflow-core/src/airflow/
api_fastapi/core_api/routes/public/task_instances.py`'s
`post_clear_task_instances`, mounted under `public_router`'s `/api/v2`
prefix). The running Airflow's major version, read via
`convalesce_emit.version_of`, picks which one to call; a version that
cannot be read is treated as a reason not to guess, not as a default.

Import as:

import convalesce_emit_airflow.retry as cealretry
"""

import dataclasses
import json
import logging
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, List, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit.retry as ceretry
import convalesce_emit_airflow._version as ceairflowver

_LOG = logging.getLogger(__name__)

TOOL = "airflow"

# Names an Airflow *connection id*, not a literal token -- resolved via
# BaseHook.get_connection the same way connection_coordinates() resolves
# one, so the actual secret lives in Airflow's own connection store, never
# in this process's environment as plaintext.
_RETRY_CONNECTION_ENV = "CONVALESCE_AIRFLOW_RETRY_TOKEN"

_CLEAR_PATH_V1 = "/api/v1/dags/{dag_id}/clearTaskInstances"
_CLEAR_PATH_V2 = "/api/v2/dags/{dag_id}/clearTaskInstances"


# #############################################################################
# _RetryTarget
# #############################################################################


@dataclasses.dataclass(frozen=True)
class _RetryTarget:
    """
    Where and how to call this Airflow's own REST API.

    :param base_url: the webserver's base URL, no trailing slash
    :param token: sent as a bearer token; never logged
    """

    base_url: str
    token: str


def _read_target() -> Optional[_RetryTarget]:
    """
    Resolve the retry credential, failing closed on anything missing.

    :return: the target to call, or None when the connection id is unset,
        does not resolve, or is missing a host or a password
    """
    conn_id = os.environ.get(_RETRY_CONNECTION_ENV)
    if not conn_id or not conn_id.strip():
        _LOG.warning(
            "convalesce: %s is not set; not executing airflow retries",
            _RETRY_CONNECTION_ENV,
        )
        return None
    try:
        connection = _get_connection(conn_id.strip())
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not resolve retry connection %s: %s",
            conn_id,
            exc,
        )
        return None
    base_url = _webserver_base_url(connection)
    token = getattr(connection, "password", None)
    if not base_url or not token:
        _LOG.warning(
            "convalesce: retry connection %s has no host or no token", conn_id
        )
        return None
    return _RetryTarget(base_url=base_url, token=token)


def _get_connection(conn_id: str) -> Any:
    """
    Ask Airflow for one connection, by id.

    Imported here, not at module level, so this module still imports
    where Airflow is absent -- the same reason `listener.py`'s own copy
    of this does the same thing.

    :param conn_id: the connection id to resolve
    :return: Airflow's own Connection object
    """
    from airflow.hooks.base import (  # pylint: disable=import-outside-toplevel
        BaseHook,
    )

    return BaseHook.get_connection(conn_id)


def _webserver_base_url(connection: Any) -> Optional[str]:
    """
    Build the webserver's base URL from a resolved connection.

    :param connection: Airflow's own Connection object
    :return: the base URL, no trailing slash; None when the connection
        names no host at all
    """
    host = getattr(connection, "host", None)
    if not host:
        return None
    host = str(host)
    if host.startswith("http://") or host.startswith("https://"):
        return host.rstrip("/")
    scheme = getattr(connection, "schema", None) or "http"
    port = getattr(connection, "port", None)
    suffix = f":{port}" if port else ""
    return f"{scheme}://{host}{suffix}"


# #############################################################################
# local scope
# #############################################################################


def _dag_exists_locally(dag_id: str) -> bool:
    """
    Whether this process's own DagBag recognizes a dag id.

    The local-scope check the plan requires before ever calling `claim()`:
    a shared collect endpoint serves every deployment in the tenant, and
    only this process can say whether a dag is actually its own to act on.

    :param dag_id: the dag id a candidate names
    :return: True only when a real DagBag resolves it; False on any
        failure to check -- Airflow absent, a broken DagBag, anything --
        because "could not tell" and "yes, it is mine" are not the same
        answer
    """
    try:
        from airflow.models import (  # pylint: disable=import-outside-toplevel
            DagBag,
        )

        bag = DagBag(read_dags_from_db=True)
        return bag.get_dag(dag_id) is not None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not check dag %s locally: %s", dag_id, exc
        )
        return False


def _parse_map_index(raw: Optional[str]) -> Tuple[bool, Optional[int]]:
    """
    Read a coordinate's `map_index`, when there is one.

    :param raw: the coordinate's string value, or None when absent
    :return: `(True, None)` when absent, `(True, value)` when it parses,
        `(False, None)` when present but not an integer -- a malformed
        coordinate, not a value to guess at
    """
    if raw is None:
        return True, None
    try:
        return True, int(raw)
    except ValueError:
        return False, None


# #############################################################################
# the native call
# #############################################################################


def _clear_url(target: _RetryTarget, major: Optional[int], dag_id: str) -> str:
    """
    Build the version-appropriate clearTaskInstances URL.

    :param target: where this Airflow's REST API is
    :param major: the running Airflow's major version; None when it could
        not be read
    :param dag_id: the dag to clear a task instance on
    :return: the full URL
    :raises RuntimeError: if `major` is None -- a version that cannot be
        read is a reason not to guess which API shape to call, not a
        reason to default to one
    """
    if major is None:
        raise RuntimeError(
            "could not determine the running Airflow's major version"
        )
    template = _CLEAR_PATH_V1 if major < 3 else _CLEAR_PATH_V2
    path = template.format(dag_id=urllib.parse.quote(dag_id, safe=""))
    return target.base_url + path


def _clear_task_instance(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    target: _RetryTarget,
    major: Optional[int],
    *,
    dag_id: str,
    task_id: str,
    run_id: str,
    map_index: Optional[int],
    timeout: float,
) -> str:
    """
    Clear one task instance via Airflow's own REST API -- the primitive
    that makes the scheduler re-queue it.

    :param target: where and how to call this Airflow
    :param major: the running Airflow's major version
    :param dag_id: the dag the task belongs to
    :param task_id: the task to clear
    :param run_id: the dag run the task instance belongs to
    :param map_index: the mapped task index, when the task is mapped
    :param timeout: seconds to wait on the request
    :return: a native run reference identifying what was cleared
    :raises RuntimeError: on any non-2xx response or unreadable version
    :raises Exception: on anything that means the request never got a
        response at all -- unreachable, timed out, refused
    """
    url = _clear_url(target, major, dag_id)
    task_ids: List[Any] = (
        [task_id] if map_index is None else [[task_id, map_index]]
    )
    body = {
        "dry_run": False,
        "only_failed": False,
        "reset_dag_runs": False,
        "dag_run_id": run_id,
        "task_ids": task_ids,
    }
    headers = {
        "Authorization": f"Bearer {target.token}",
        "Content-Type": "application/json",
        "User-Agent": f"convalesce-emit-airflow/{ceairflowver.__version__}",
    }
    request = urllib.request.Request(  # nosec B310
        url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(  # nosec B310
            request, timeout=timeout
        ) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"clearTaskInstances returned HTTP {exc.code}: {exc.read()[:200]!r}"
        ) from exc
    if status not in (200, 201):
        raise RuntimeError(
            f"clearTaskInstances returned unexpected HTTP {status}"
        )
    ref = f"{dag_id}/{run_id}/{task_id}"
    return ref if map_index is None else f"{ref}[{map_index}]"


# #############################################################################
# run_pending_retries
# #############################################################################


@dataclasses.dataclass(frozen=True)
class RetryRunSummary:
    """
    What one call to `run_pending_retries` did.

    :param considered: airflow-tool candidates collect listed
    :param claimed: candidates this process actually claimed
    :param triggered: claimed candidates Airflow's API cleared
    :param failed: claimed candidates whose clear call failed
    :param skipped: candidates left alone -- out of local scope, missing
        or malformed coordinates, no retry credential, or lost the claim
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
        return "convalesce-emit-airflow"


def run_pending_retries(  # pylint: disable=too-many-locals
    *,
    config: Optional[cemit.Config] = None,
    owner: Optional[str] = None,
    timeout: float = 10.0,
) -> RetryRunSummary:
    """
    Poll collect for approved airflow retries and execute what this
    process is responsible for.

    Wire this into your own scheduled job -- a cron entry or a small
    maintenance DAG -- never into a listener callback::

        from convalesce_emit_airflow.retry import run_pending_retries

        run_pending_retries()

    :param config: where collect is and how hard to try; read from the
        environment when not given
    :param owner: identifies this process's claims to a human debugging a
        stuck remedy later; this host's name when not given
    :param timeout: seconds to wait on each Airflow REST call
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

    major = _airflow_major_version()
    claim_owner = owner or _default_owner()
    claimed = triggered = failed = skipped = 0
    for candidate in candidates:
        coords = candidate.coordinates
        dag_id = coords.get("dag_id")
        task_id = coords.get("task_id")
        run_id = coords.get("run_id")
        if not dag_id or not task_id or not run_id:
            _LOG.warning(
                "convalesce: retry %s is missing dag_id/task_id/run_id; "
                "skipping",
                candidate.remedy_id,
            )
            skipped += 1
            continue
        map_ok, map_index = _parse_map_index(coords.get("map_index"))
        if not map_ok:
            _LOG.warning(
                "convalesce: retry %s has a malformed map_index; skipping",
                candidate.remedy_id,
            )
            skipped += 1
            continue
        if not _dag_exists_locally(dag_id):
            _LOG.info(
                "convalesce: dag %s is not local to this process; leaving "
                "retry %s to whoever owns it",
                dag_id,
                candidate.remedy_id,
            )
            skipped += 1
            continue
        if not ceretry.claim(candidate.remedy_id, owner=claim_owner, config=cfg):
            skipped += 1
            continue
        claimed += 1
        try:
            native_ref = _clear_task_instance(
                target,
                major,
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
                timeout=timeout,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            _LOG.warning(
                "convalesce: could not clear %s/%s: %s", dag_id, task_id, exc
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


def _airflow_major_version() -> Optional[int]:
    """
    The running Airflow's major version number.

    :return: the leading version component, or None when Airflow is
        absent or its version does not parse
    """
    raw = cemit.version_of("airflow")
    if not raw:
        return None
    try:
        return int(raw.split(".", 1)[0])
    except ValueError:
        return None

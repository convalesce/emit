"""
Blocking: asking collect whether a dataset's circuit breaker is open.

Failure semantics, stated here because they are the whole point of this
module: an endpoint that is unreachable or slow means proceed, not block.
A metadata service having a bad minute must never stop a customer's
pipeline. This function never raises, and it returns `True` (open, proceed)
for every outcome except one -- a response that actually says `{"open":
false, ...}`. A timeout, a connection error, a non-2xx status, a body that
does not parse as JSON, or a body with no `open` key are all treated as
"could not get a definite answer", which is not the same thing as "closed",
and are all logged and treated as open.

This reads an `api`-scoped key (`CONVALESCE_API_KEY`), never the `ingest`
key: the two are different actors server-side (see
`ConvalesceIngestKeyAuthenticator`), and only the `api` actor has a policy
letting it ask this question at all.

Import as:

import convalesce_emit.gate as cegate
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

import convalesce_emit._version as ceversio
import convalesce_emit.config as ceconfig

_LOG = logging.getLogger(__name__)

_GATE_PATH = "/v1/gate"


def is_open(  # pylint: disable=too-many-return-statements
    dataset: str, *, config: Optional[ceconfig.Config] = None
) -> bool:
    """
    Ask whether `dataset`'s circuit breaker is open.

    :param dataset: the dataset's identity, exactly as the caller passes it
        (collect's gate endpoint reads a urn; this plugin builds no urns of
        its own, so the caller supplies whatever it already builds today to
        report against a specific asset)
    :param config: where to send and how hard to try; read from the
        environment when not given. Uses the same `CONVALESCE_ENDPOINT` as
        every other plugin, plus `CONVALESCE_API_KEY` -- the api-scoped key,
        never the ingest key
    :return: `True` unless a request actually completed and answered
        `{"open": false, ...}`. Every other outcome -- disabled, dry-run, no
        api key configured, unreachable, timed out, a non-2xx status, an
        unparseable or malformed body -- also returns `True`, logging why,
        because "could not find out" and "found out it is closed" are not
        the same thing and only the second one may stop a pipeline
    """
    cfg = config or ceconfig.Config.from_env()
    if not cfg.enabled or cfg.dry_run:
        return True
    if not cfg.api_key:
        _LOG.warning(
            "convalesce: no api key configured for the gate; proceeding for %s",
            dataset,
        )
        return True

    url = (
        cfg.endpoint.rstrip("/")
        + _GATE_PATH
        + "?dataset="
        + urllib.parse.quote(dataset, safe="")
    )
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "User-Agent": f"convalesce-emit/{ceversio.__version__}",
    }
    # The endpoint is validated as http(s) in Config.validate; this function
    # does not call validate() itself since dry-run/disabled already
    # returned above and api_key has already been checked.
    request = urllib.request.Request(
        url, headers=headers, method="GET"
    )  # nosec B310
    try:
        with urllib.request.urlopen(  # nosec B310
            request, timeout=cfg.timeout
        ) as response:
            body = response.read()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # Deliberately broad, and deliberately not re-raised: unreachable,
        # timed out, refused, a non-2xx status raised by urlopen as
        # HTTPError -- every one of these means "no definite answer", which
        # this function treats identically to an explicit open verdict.
        _LOG.warning(
            "convalesce: could not reach the gate for %s: %s; proceeding",
            dataset,
            exc,
        )
        return True

    try:
        verdict = json.loads(body)
    except ValueError as exc:
        _LOG.warning(
            "convalesce: gate response for %s was not valid json: %s; proceeding",
            dataset,
            exc,
        )
        return True
    if not isinstance(verdict, dict) or "open" not in verdict:
        _LOG.warning(
            "convalesce: gate response for %s had no 'open' field; proceeding",
            dataset,
        )
        return True

    if verdict.get("open") is False:
        _LOG.info(
            "convalesce: gate closed for %s: %s",
            dataset,
            verdict.get("reason", "no reason given"),
        )
        return False
    return True

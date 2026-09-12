"""
Transport: batches observations and posts them.

Built on `urllib` rather than `requests` or `httpx`. This runs inside someone
else's Airflow, Dagster or Prefect process, and the one thing it must never
do is drag a dependency into a resolution that already works.

Nothing here raises into the caller. A customer's DAG must not go red because
our endpoint had a bad minute: we are watching their pipeline, not standing
in it.

Import as:

import convalesce_emit.client as ceclient
"""

import json
import logging
import random
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

import convalesce_emit._version as ceversio
import convalesce_emit.config as ceconfig
import convalesce_emit.envelope as ceenvelo
import convalesce_emit.errors as ceerrors
import convalesce_emit.protocols as ceproto

_LOG = logging.getLogger(__name__)

# Retry only what a retry can fix. A 400 means the receiver understood us and
# said no; sending it again just wastes the pipeline's time.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

_OBSERVATIONS_PATH = "/v1/observations"
# What wrapping a batch costs on the wire, beyond the observations and the
# commas between them: `{"observations":[` and `]}`.
_BODY_OVERHEAD = len(b'{"observations":[]}')
# Caps the backoff so a long outage does not park a worker thread for
# minutes at a time.
_MAX_BACKOFF_SECONDS = 30


# #############################################################################
# Emitter
# #############################################################################


class Emitter:
    """
    Sends a tool's raw output to Convalesce.

    :param config: where to send and how hard to try; read from the
        environment when not given
    """

    def __init__(self, config: Optional[ceconfig.Config] = None) -> None:
        self.config = config or ceconfig.Config.from_env()
        self.config.validate()
        self._lock = threading.Lock()
        self._batch: List[ceenvelo.Observation] = []
        self._batch_bytes = 0

    def __enter__(self) -> "Emitter":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def emit(
        self,
        *,
        tool: str,
        event: str,
        payload: Any,
        tool_version: Optional[str] = None,
    ) -> None:
        """
        Queue one observation, sending the batch when it is full.

        Full by count or by size: the receiver refuses a body above its
        limit whole, so an observation that would push the batch past it
        goes into the next batch, and one that is over the limit on its own
        is sent on its own, so at worst one is refused rather than fifty.

        :param tool: which tool produced this
        :param event: which callback fired
        :param payload: the tool's own output, untouched
        :param tool_version: the tool's version, where it could be read
        :return: nothing
        """
        if not self.config.enabled:
            return
        observation = ceenvelo.build(
            tool=tool,
            event=event,
            payload=payload,
            tool_version=tool_version,
        )
        size = len(_encode(observation))
        if size + _BODY_OVERHEAD > self.config.max_body_bytes:
            _LOG.warning(
                "convalesce: %s/%s is %d bytes, above the receiver's limit "
                "of %d; sending it alone and it may be refused",
                tool,
                event,
                size,
                self.config.max_body_bytes,
            )
        with self._lock:
            # A comma per observation joins them in the body.
            projected = (
                self._batch_bytes + size + len(self._batch) + _BODY_OVERHEAD
            )
            if self._batch and projected > self.config.max_body_bytes:
                batch, self._batch = self._batch, []
                self._batch_bytes = 0
            else:
                batch = []
            self._batch.append(observation)
            self._batch_bytes += size
            ready = len(self._batch) >= self.config.batch_size
        if batch:
            self._send(batch)
        if ready:
            self.flush()

    def flush(self) -> None:
        """
        Send whatever is queued, logging rather than raising on failure.

        :return: nothing
        """
        with self._lock:
            batch, self._batch = self._batch, []
            self._batch_bytes = 0
        self._send(batch)

    def _send(self, batch: List[ceenvelo.Observation]) -> None:
        """
        Send one batch, logging rather than raising on failure.

        :param batch: observations to deliver; nothing is sent for none
        :return: nothing
        """
        if not batch:
            return
        if self.config.dry_run:
            for observation in batch:
                _LOG.info(
                    "convalesce dry-run: %s",
                    json.dumps(observation.to_dict(), default=str),
                )
            return
        try:
            self._post(batch)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # Deliberately broad: see the module docstring. Anything escaping
            # here would surface inside the customer's task.
            _LOG.warning(
                "convalesce: dropped %d observation(s): %s", len(batch), exc
            )

    def close(self) -> None:
        """
        Flush anything still queued.

        :return: nothing
        """
        self.flush()

    def _post(self, batch: List[ceenvelo.Observation]) -> None:
        """
        Send one batch, retrying what a retry can fix.

        :param batch: observations to deliver
        :return: nothing
        :raises TransportError: if every attempt failed
        """
        body = (
            b'{"observations":['
            + b",".join(_encode(obs) for obs in batch)
            + b"]}"
        )
        url = self.config.endpoint.rstrip("/") + _OBSERVATIONS_PATH
        for attempt in range(self.config.max_retries + 1):
            try:
                self._attempt(url, body)
                return
            except ceerrors.TransportError as exc:
                retryable = exc.status is None or exc.status in RETRYABLE_STATUS
                if not retryable or attempt == self.config.max_retries:
                    raise
                # Full jitter: several workers failing at once should not
                # come back in lockstep.
                delay = min(2**attempt, _MAX_BACKOFF_SECONDS) * random.random()
                _LOG.debug("convalesce: retrying in %.2fs (%s)", delay, exc)
                time.sleep(delay)

    def _attempt(self, url: str, body: bytes) -> None:
        """
        Make one HTTP request.

        :param url: where to post
        :param body: the encoded batch
        :return: nothing
        :raises TransportError: on any HTTP or connection failure
        """
        # The key is the only thing here that says who is calling. No
        # header names an account: one that did would be an unauthenticated
        # claim sitting next to the credential that actually proves it.
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.config.ingest_key}",
            "User-Agent": f"convalesce-emit/{ceversio.__version__}",
        }
        # The endpoint is validated as http(s) in Config.validate.
        request = urllib.request.Request(  # nosec B310
            url, data=body, method="POST", headers=headers
        )
        try:
            with urllib.request.urlopen(  # nosec B310
                request, timeout=self.config.timeout
            ):
                return
        except urllib.error.HTTPError as exc:
            raise ceerrors.TransportError(
                f"HTTP {exc.code} from {url}", status=exc.code
            ) from exc
        except urllib.error.URLError as exc:
            raise ceerrors.TransportError(
                f"could not reach {url}: {exc.reason}"
            ) from exc


def _encode(observation: ceenvelo.Observation) -> bytes:
    """
    Encode one observation the way it goes on the wire.

    :param observation: the observation to encode
    :return: its JSON, as bytes
    """
    return json.dumps(
        observation.to_dict(), default=str, separators=(",", ":")
    ).encode("utf-8")


def send_one(
    *,
    tool: str,
    event: str,
    payload: Any,
    emitter: Optional[ceproto.EmitterLike] = None,
    tool_version: Optional[str] = None,
) -> None:
    """
    Send a single observation and flush, swallowing any failure.

    The one-shot path plugins use, where events arrive singly from a hook
    rather than in a stream worth batching.

    :param tool: which tool produced this
    :param event: which callback fired
    :param payload: the tool's own output, untouched
    :param emitter: emitter to send through; built from the environment when
        not given
    :param tool_version: the tool's version, where it could be read
    :return: nothing
    """
    try:
        target = emitter or Emitter()
        target.emit(
            tool=tool, event=event, payload=payload, tool_version=tool_version
        )
        target.flush()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A customer's run must not fail because we could not report on it.
        _LOG.warning("convalesce: could not emit %s: %s", event, exc)

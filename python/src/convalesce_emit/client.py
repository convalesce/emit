"""
Transport: batches observations and posts them.

Built on `urllib` rather than `requests` or `httpx`. This runs inside someone
else's Airflow, Dagster or Prefect process, and the one thing it must never
do is drag a dependency into a resolution that already works.

Nothing here raises into the caller. A customer's DAG must not go red because
our endpoint had a bad minute: we are watching their pipeline, not standing
in it.

Nothing here is thrown away either. A batch that cannot be delivered goes to
the local spool and is sent again after the next send that succeeds; a batch
the receiver refuses is split so one bad observation cannot take the others
with it, and whatever is still refused is kept on disk rather than dropped.

Import as:

import convalesce_emit.client as ceclient
"""

import atexit
import gzip
import json
import logging
import random
import threading
import time
import urllib.error
import urllib.request
import weakref
from typing import Any, Dict, List, Optional

import convalesce_emit._version as ceversio
import convalesce_emit.chunk as cechunk
import convalesce_emit.config as ceconfig
import convalesce_emit.envelope as ceenvelo
import convalesce_emit.errors as ceerrors
import convalesce_emit.protocols as ceproto
import convalesce_emit.spool as cespool

_LOG = logging.getLogger(__name__)

# Retry only what a retry can fix. A 400 means the receiver understood us and
# said no; sending it again just wastes the pipeline's time.
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
# What the receiver says when it read the batch and will never take it as
# sent. Anything else that fails -- an outage, a wrong key, no network -- is
# worth sending again later.
REFUSED_STATUS = frozenset({400, 413, 422})
# Spooled batches sent after one successful send, at most. Bounds how long a
# long outage's backlog can hold up the caller that finally got through.
_DRAIN_PER_SEND = 20

_OBSERVATIONS_PATH = "/v1/observations"
# What wrapping a batch costs on the wire, beyond the observations and the
# commas between them: `{"observations":[` and `]}`.
_BODY_OVERHEAD = len(b'{"observations":[]}')
# Caps the backoff so a long outage does not park a worker thread for
# minutes at a time.
_MAX_BACKOFF_SECONDS = 30
# Slack between a chunk's budget and the receiver's own limit, covering the
# few bytes a chunk's own `chunk_index`/`chunk_count` add over the shell
# probe used to size that budget (see `_chunk`).
_CHUNK_SAFETY_MARGIN = 64


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
        self._spool = cespool.Spool(
            self.config.spool_directory(), self.config.spool_max_bytes
        )
        # A caller that never flushes still gets its last partial batch out
        # when the interpreter exits normally.
        atexit.register(_flush_at_exit, weakref.ref(self))

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
        excluded: Optional[List[Dict[str, str]]] = None,
    ) -> None:
        """
        Queue one observation, sending the batch when it is full.

        Full by count or by size: the receiver refuses a body above its
        limit whole, so an observation that would push the batch past it
        goes into the next batch. One that is over the limit on its own is
        split into chunks that share one `observation_id` -- each chunk then
        rides this same queueing path as an observation in its own right --
        unless it cannot be split any smaller, in which case it is sent
        alone, oversized, and may be refused.

        :param tool: which tool produced this
        :param event: which callback fired
        :param payload: the tool's own output, untouched
        :param tool_version: the tool's version, where it could be read
        :param excluded: everything in `payload` that did not cross whole,
            by path and reason
        :return: nothing
        """
        if not self.config.enabled:
            return
        observation = ceenvelo.build(
            tool=tool,
            event=event,
            payload=payload,
            tool_version=tool_version,
            excluded=excluded,
        )
        size = len(_encode(observation))
        if size + _BODY_OVERHEAD > self.config.max_body_bytes:
            for chunk in self._chunk(observation, size):
                self._enqueue_one(chunk)
            return
        self._enqueue_one(observation)

    def _chunk(
        self, observation: ceenvelo.Observation, whole_size: int
    ) -> List[ceenvelo.Observation]:
        """
        Split one oversized observation into chunk observations, when
        possible.

        :param observation: the observation, already known to be too big
        :param whole_size: its encoded size, for the fallback warning
        :return: chunk observations sharing `observation.observation_id`,
            or `[observation]` unchanged when it cannot be shrunk further
        """
        if not isinstance(observation.payload, dict):
            _LOG.warning(
                "convalesce: %s/%s is %d bytes and its payload is not a "
                "dict, so it cannot be split into chunks; sending it whole "
                "and it may be refused",
                observation.tool,
                observation.event,
                whole_size,
            )
            return [observation]
        shell = ceenvelo.build(
            tool=observation.tool,
            event=observation.event,
            payload={},
            tool_version=observation.tool_version,
            excluded=observation.excluded,
            observation_id=observation.observation_id,
            chunk_index=0,
            chunk_count=1,
        )
        budget = (
            self.config.max_body_bytes
            - len(_encode(shell))
            - _BODY_OVERHEAD
            - _CHUNK_SAFETY_MARGIN
        )
        parts = (
            cechunk.split(observation.payload, budget)
            if budget > 0
            else [observation.payload]
        )
        if len(parts) <= 1:
            return [observation]
        count = len(parts)
        return [
            ceenvelo.build(
                tool=observation.tool,
                event=observation.event,
                payload=part,
                tool_version=observation.tool_version,
                # Only the first chunk carries what was excluded from the
                # whole payload; the receiver reassembles before any of it
                # is read, and carrying it on every chunk would count it
                # once per chunk instead of once per observation.
                excluded=observation.excluded if i == 0 else None,
                observation_id=observation.observation_id,
                chunk_index=i,
                chunk_count=count,
            )
            for i, part in enumerate(parts)
        ]

    def _enqueue_one(self, observation: ceenvelo.Observation) -> None:
        """
        Queue one already-built observation, sending the batch when full.

        :param observation: a whole observation, or one chunk of one
        :return: nothing
        """
        size = len(_encode(observation))
        if size + _BODY_OVERHEAD > self.config.max_body_bytes:
            _LOG.warning(
                "convalesce: %s/%s is %d bytes, above the receiver's limit "
                "of %d; sending it alone and it may be refused",
                observation.tool,
                observation.event,
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
        body = b""
        try:
            body = _compress(batch)
            self._post_body(body)
        except ceerrors.TransportError as exc:
            if exc.status in REFUSED_STATUS:
                self._refused(batch, body, exc)
            else:
                self._keep(body, len(batch), exc)
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            # Deliberately broad: see the module docstring. Anything escaping
            # here would surface inside the customer's task.
            self._keep(body, len(batch), exc)
            return
        self._drain()

    def _refused(
        self,
        batch: List[ceenvelo.Observation],
        body: bytes,
        exc: ceerrors.TransportError,
    ) -> None:
        """
        Deal with a batch the receiver read and said no to.

        :param batch: what was sent
        :param body: it, compressed
        :param exc: the refusal
        """
        if len(batch) > 1:
            # One observation the receiver will not take must not cost the
            # others in its batch.
            for observation in batch:
                self._send([observation])
            return
        path = self._spool.save(body, cespool.REJECTED)
        _LOG.warning(
            "convalesce: %s/%s was refused (%s); kept at %s",
            batch[0].tool,
            batch[0].event,
            exc,
            path,
        )

    def _keep(self, body: bytes, count: int, exc: Exception) -> None:
        """
        Spool a batch that could not be delivered, to send again later.

        :param body: the batch, compressed; empty if it never got that far
        :param count: how many observations it holds
        :param exc: why it was not delivered
        """
        path = self._spool.save(body) if body else None
        if path:
            _LOG.warning(
                "convalesce: could not deliver %d observation(s), kept at %s "
                "to send again: %s",
                count,
                path,
                exc,
            )
        else:
            _LOG.error(
                "convalesce: lost %d observation(s), could not keep them: %s",
                count,
                exc,
            )

    def _drain(self) -> None:
        """
        Send spooled batches, oldest first, now that the receiver answers.

        Stops at the first one that fails again; it stays in the spool.
        """
        for path in self._spool.pending()[:_DRAIN_PER_SEND]:
            claimed = self._spool.claim(path)
            if claimed is None:
                continue
            try:
                self._post_body(self._spool.read(claimed))
            except ceerrors.TransportError as exc:
                if exc.status in REFUSED_STATUS:
                    self._spool.reject(claimed)
                    continue
                self._spool.release(claimed)
                return
            except Exception:  # pylint: disable=broad-exception-caught
                self._spool.release(claimed)
                return
            self._spool.done(claimed)

    def close(self) -> None:
        """
        Flush anything still queued.

        :return: nothing
        """
        self.flush()

    def _post_body(self, compressed: bytes) -> None:
        """
        Send one compressed batch, retrying what a retry can fix.

        :param compressed: the gzip-compressed request body
        :return: nothing
        :raises TransportError: if every attempt failed
        """
        url = self.config.endpoint.rstrip("/") + _OBSERVATIONS_PATH
        for attempt in range(self.config.max_retries + 1):
            try:
                self._attempt(url, compressed)
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
        :param body: the gzip-compressed batch
        :return: nothing
        :raises TransportError: on any HTTP or connection failure
        """
        # The key is the only thing here that says who is calling. No
        # header names an account: one that did would be an unauthenticated
        # claim sitting next to the credential that actually proves it.
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
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


def _flush_at_exit(ref: "weakref.ReferenceType[Emitter]") -> None:
    """
    Flush an emitter still alive at interpreter exit.

    :param ref: the emitter, weakly, so registering it does not keep it alive
    """
    emitter = ref()
    if emitter is not None:
        emitter.flush()


def _compress(batch: List[ceenvelo.Observation]) -> bytes:
    """
    Build the request body for one batch.

    Whole-payload forwarding means a batch is bigger than it used to be;
    gzip is what keeps the wire cost from growing at the same rate. The
    receiver decides its size cap against the decompressed bytes, not these,
    so the local batching above is unaffected.

    :param batch: observations to deliver
    :return: the gzip-compressed body
    """
    return gzip.compress(
        b'{"observations":[' + b",".join(_encode(obs) for obs in batch) + b"]}"
    )


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
    excluded: Optional[List[Dict[str, str]]] = None,
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
    :param excluded: everything in `payload` that did not cross whole, by
        path and reason
    :return: nothing
    """
    try:
        target = emitter or Emitter()
        target.emit(
            tool=tool,
            event=event,
            payload=payload,
            tool_version=tool_version,
            excluded=excluded,
        )
        target.flush()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A customer's run must not fail because we could not report on it.
        _LOG.warning("convalesce: could not emit %s: %s", event, exc)

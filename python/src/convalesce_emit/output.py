"""
The last lines a failed unit of work wrote, to send with its failure.

A task that runs a command fails with "the command returned a non-zero exit
code". What the command said, the reason it failed, is in the task's output
and nowhere on the exception. So a plugin keeps the last records a task, a
step or a run logged while it ran, and when that unit fails sends the end of
them as `output_tail`. A unit that succeeds sends none of it.

Two halves. `Ring` holds the last records it was shown and nothing else:
no formatting, no I/O, a bounded `deque`. `watch` shows it every record
the process makes, without putting a handler on the root logger. `tail`
turns text into what is sent: credentials masked, then the end kept, at most
`MAX_LINES` lines and `MAX_BYTES` bytes of UTF-8.

Masked, in this order, and declared once in `excluded` when anything was:

- a PEM block, from its `-----BEGIN` line to its `-----END` line, and one
  whose beginning was already gone from the ring;
- a JSON Web Token;
- the value of a name that reads as a credential, written `name=value`,
  `name: value` or `"name": "value"`, and what follows `Bearer` or `Basic`
  after one;
- the password of a URI with user-info, and what `redact_secrets` masks in
  any string.

The first three are what `convalesce_emit.settings` takes as the marks of a
credential in a value. Its fourth, a long run of random characters, is not
applied: in output it is as often a path, a model's name or a query id, and
those are what the tail is sent for.

On unless `CONVALESCE_SEND_OUTPUT` is false. Nothing here raises, and a
failure is logged by its class alone: its message could quote the output.

Import as:

import convalesce_emit.output as ceoutput
"""

import collections
import logging
import re
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import convalesce_emit.config as ceconfig
import convalesce_emit.redact as ceredact

_LOG = logging.getLogger(__name__)

SETTING = "CONVALESCE_SEND_OUTPUT"
_FALSY = frozenset({"0", "false", "no", "off"})

# Where the tail goes in a payload, and what its `excluded` paths name.
FIELD = "output_tail"

# What crosses: enough for the lines around an error, and small beside the
# event it rides on.
MAX_BYTES = 4096
MAX_LINES = 60
# How many records a ring holds. More than `MAX_LINES`, because a record is
# often shorter than a line is long and masking can only shrink them.
MAX_RECORDS = 200
# A record longer than this keeps its end. Twice `MAX_BYTES`, so where the
# cut fell is always outside what is sent: nothing cut in half before the
# masking can reach the tail.
MAX_RECORD_CHARS = 2 * MAX_BYTES

# The loggers of this package and of every plugin built on it.
_OWN = "convalesce_emit"

# What a ring that holds one unit's records files each of them under.
_ONE_UNIT: Any = None

MASKED = "credential masked"
_MASK = "***"

_PEM_BEGIN = "-----BEGIN"
_PEM_END = "-----END"
# A line of a PEM block's body, as it reads once its beginning is gone, and
# the last of them, which is as short as the block's length leaves it.
_PEM_BODY = re.compile(r"^[A-Za-z0-9+/=]{16,}$")
_PEM_BODY_END = re.compile(r"^[A-Za-z0-9+/=]+$")
_JWT = re.compile(r"eyJ[A-Za-z0-9_\-]{4,}\.[A-Za-z0-9_\-]{4,}\.[A-Za-z0-9_\-]*")
# `password=x`, `password: x`, `"password": "x"`, `Authorization: Bearer x`.
# The name may run on (`secret_key`, `tokens`) and need not start a word
# (`db_password`); `sig` alone must be the whole of one, or every `signal`
# would be. Wide on purpose: a word masked by mistake costs that
# word, and a credential missed is sent. Spaces are spelled out so the
# Java library, whose `\s` is narrower, masks exactly the same text.
_ASSIGNED = re.compile(
    r"(?P<head>(?:(?:password|passwd|pwd|passphrase|secret|token"
    r"|api[_\-]?key|access[_\-]?key|private[_\-]?key|credential"
    r"|authorization|signature)[A-Za-z0-9_\-]*|(?<![A-Za-z])sig)"
    r"[\"']?[ \t]*[=:][ \t]*"
    r"(?:(?:bearer|basic)[ \t]+)?)"
    r"(?P<secret>\"[^\"\r\n]*\"|'[^'\r\n]*'|[^ \t\r\n&;,\"']+)",
    re.IGNORECASE | re.ASCII,
)

Excluded = List[Dict[str, str]]


def enabled(environ: Optional[Mapping[str, str]] = None) -> bool:
    """
    Whether a failure's output may be sent.

    :param environ: the variables to read; the process's own when not given
    :return: False only when `CONVALESCE_SEND_OUTPUT` says so
    """
    value = ceconfig.read_setting(SETTING, environ)
    return (value or "").strip().lower() not in _FALSY


# #############################################################################
# Ring
# #############################################################################


class Ring(logging.Handler):
    """
    Hold the last records logged, for the tail a failure is sent with.

    A record costs one `getMessage` and one append. Its level, its logger
    and its time are not kept: a line of output is what a tail is made of.

    :param capacity: how many records are held before the oldest is dropped
    """

    def __init__(self, capacity: int = MAX_RECORDS) -> None:
        super().__init__(level=logging.NOTSET)
        self._records: Deque[Tuple[Any, str]] = collections.deque(
            maxlen=capacity
        )
        self._last: Optional[logging.LogRecord] = None

    def emit(self, record: logging.LogRecord) -> None:
        """
        Keep one record.

        :param record: what was logged
        :return: nothing
        """
        try:
            # A record is seen as it is made and again by any logger this
            # ring is on. What this package and its plugins log is about
            # the reporting, not the unit reported on.
            if record is self._last or record.name.startswith(_OWN):
                return
            text = self.text(record)
            if text is not None:
                self._last = record
                self._records.append((self.scope(record), text))
        except Exception:  # pylint: disable=broad-exception-caught
            # Called from inside the tool's own logging call, which must
            # return; and logging from a handler would come straight back.
            pass

    def hold(self, text: str, scope: Any = None) -> None:
        """
        Keep one piece of output that did not come as a log record.

        :param text: what was written
        :param scope: the unit it belongs to, as `scope` would name it
        :return: nothing
        """
        self._records.append((scope, clip(text)))

    def text(self, record: logging.LogRecord) -> Optional[str]:
        """
        What a record is held as.

        Its message here. A plugin whose tool wraps what its user logged
        in words of its own answers with the user's.

        :param record: what was logged
        :return: the text to hold; None for a record that is not output
        """
        return text_of(record)

    def scope(self, record: logging.LogRecord) -> Any:
        """
        Which unit of work a record belongs to.

        One unit to a ring here. A plugin whose tool runs several side by
        side in one process answers with the unit's id instead.

        :param record: what was logged
        :return: what `lines` is asked for to get this record back
        """
        del record
        return _ONE_UNIT

    def lines(self, scope: Any = None) -> List[str]:
        """
        What is held for one unit, oldest first.

        :param scope: the unit, as `scope` named it
        :return: each record's text; one may hold several lines
        """
        # A copy first: a thread still logging appends as this is read.
        return [text for held, text in list(self._records) if held == scope]

    def clear(self) -> None:
        """
        Forget everything held.

        :return: nothing
        """
        self._records.clear()
        self._last = None


def text_of(record: logging.LogRecord) -> str:
    """
    A record as output: its message.

    A traceback logged with it is left behind. A failure crosses with its
    exception's own, as `error_detail`, and a tool that logs "task failed"
    with one after the output would fill the tail with frames and push
    out the lines it is sent for.

    :param record: what was logged
    :return: its text, the end of it when it is very long
    """
    return clip(record.getMessage())


def clip(text: str, limit: int = MAX_RECORD_CHARS) -> str:
    """
    The end of a text that is too long to hold.

    :param text: one record's text
    :param limit: how many characters are held
    :return: the text, or its last `limit` characters from the first line
        break among them
    """
    if len(text) <= limit:
        return text
    kept = text[-limit:]
    line_break = kept.find("\n")
    return kept[line_break + 1 :] if line_break >= 0 else kept


def watch(*loggers: Any, ring: Optional[Ring] = None) -> Optional[Ring]:
    """
    Start holding what this process logs.

    Every record is seen as it is made, through the factory `logging`
    makes records with, and no handler is put on the root logger. A
    handler there would be the first: `logging.basicConfig` then does
    nothing, and a warning that reached nobody is no longer printed. Both
    change what a driver or a task says for itself, and a PySpark driver
    run with one lost its own log lines to it.

    :param loggers: loggers whose records are wanted as their handlers
        see them, with what the tool added to each: a ring is put on
        these, and it holds a record once whichever way it comes
    :param ring: the ring to feed; a new one when not given
    :return: the ring, or None when output is not sent or it could not be
        put in place
    """
    try:
        if not enabled():
            return None
        held = Ring() if ring is None else ring
        for logger in loggers:
            if held not in logger.handlers:
                logger.addHandler(held)
        _feed(held)
        return held
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not watch output: %s", type(exc).__name__
        )
        return None


def unwatch(ring: Optional[Ring], *loggers: Any) -> None:
    """
    Stop holding what this process logs.

    :param ring: what `watch` returned; None is a ring that was never there
    :param loggers: the loggers `watch` was given
    :return: nothing
    """
    if ring is None:
        return
    try:
        _starve(ring)
        for logger in loggers:
            logger.removeHandler(ring)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not stop watching output: %s", type(exc).__name__
        )


# The rings fed each record this process makes, and the factory that was
# making them before this module's went in front of it.
_FED: List[Ring] = []
_FACTORY: Dict[str, Any] = {}


def _made(*args: Any, **kwargs: Any) -> logging.LogRecord:
    """
    Make a record as it was being made, and show it to each ring.

    :param args: what `logging` hands the factory
    :param kwargs: the same, by name
    :return: the record, unchanged
    """
    record: logging.LogRecord = _FACTORY["previous"](*args, **kwargs)
    for ring in tuple(_FED):
        ring.emit(record)
    return record


def _feed(ring: Ring) -> None:
    """
    Show a ring every record made from here on.

    :param ring: the ring
    :return: nothing
    """
    if ring not in _FED:
        _FED.append(ring)
    current = logging.getLogRecordFactory()
    if current is not _made:
        _FACTORY["previous"] = current
        logging.setLogRecordFactory(_made)


def _starve(ring: Ring) -> None:
    """
    Stop showing a ring the records made, and step aside when no ring is
    left.

    :param ring: the ring
    :return: nothing
    """
    if ring in _FED:
        _FED.remove(ring)
    # Only when nobody has put a factory of their own in front since: then
    # this one stays where it is, showing records to no one.
    if not _FED and logging.getLogRecordFactory() is _made:
        logging.setLogRecordFactory(_FACTORY.pop("previous"))


# #############################################################################
# tail
# #############################################################################


def tail(
    output: Union[str, Iterable[str], None], *, path: str = FIELD
) -> Tuple[str, Excluded]:
    """
    What is sent of a failed unit's output.

    :param output: the output, whole or as the records a ring held
    :param path: where in the payload the tail is put
    :return: the end of the output with its credentials masked, empty when
        there is none or output is not sent, and what was masked or cut
        from it, by path and reason
    """
    try:
        if output is None or not enabled():
            return "", []
        text = output if isinstance(output, str) else "\n".join(output)
        return _tail(text, path)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not read the output: %s", type(exc).__name__
        )
        return "", []


def add(
    payload: Dict[str, Any],
    output: Union[str, Iterable[str], None],
    *,
    path: str = FIELD,
) -> Excluded:
    """
    Put a failed unit's output on the payload it is sent with.

    :param payload: what is about to be sent, changed in place; left as it
        is when there is no output to send
    :param output: the output, whole or as the records a ring held
    :param path: where the field is from the envelope's root, for a
        payload that is not the root itself
    :return: what was masked or cut, by path and reason
    """
    text, excluded = tail(output, path=path)
    if text:
        payload[FIELD] = text
    return excluded if text else []


def _tail(text: str, path: str) -> Tuple[str, Excluded]:
    """
    Mask a text and keep its end.

    :param text: the output
    :param path: where in the payload the tail is put
    :return: what is sent, and what was masked or cut
    """
    # A character that cannot be encoded would fail the byte count below
    # and the send after it.
    text = text.encode("utf-8", "replace").decode("utf-8")
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    while lines and not lines[-1].strip():
        lines.pop()
    excluded: Excluded = []
    masked = mask("\n".join(lines))
    if masked != "\n".join(lines):
        excluded.append({"path": path, "reason": MASKED})
    # Cut after the masking, so a credential is never cut in half and left
    # unrecognisable.
    lines = masked.split("\n")
    if len(lines) > MAX_LINES:
        lines = lines[-MAX_LINES:]
        excluded.append(
            {"path": path, "reason": f"limited to {MAX_LINES} lines"}
        )
    sizes = [len(line.encode("utf-8")) for line in lines]
    size = sum(sizes) + len(lines) - 1
    first = 0
    while size > MAX_BYTES and first < len(lines) - 1:
        size -= sizes[first] + 1
        first += 1
    lines = lines[first:]
    if size > MAX_BYTES:
        # One line longer than everything allowed: its end, from a whole
        # character.
        data = lines[0].encode("utf-8")[-MAX_BYTES:]
        start = 0
        while start < len(data) and (data[start] & 0xC0) == 0x80:
            start += 1
        lines = [data[start:].decode("utf-8")]
    if first or size > MAX_BYTES:
        excluded.append(
            {"path": path, "reason": f"limited to {MAX_BYTES} bytes"}
        )
    return "\n".join(lines), excluded


def mask(text: str) -> str:
    """
    Mask the credentials in a text, leaving the rest of it as it is.

    :param text: output of any kind
    :return: the text with each credential found replaced by a mask
    """
    text = _mask_pem(text)
    text = _JWT.sub(_MASK, text)
    text = _ASSIGNED.sub(rf"\g<head>{_MASK}", text)
    masked, _ = ceredact.redact_secrets(text)
    return masked if isinstance(masked, str) else text


def _mask_pem(text: str) -> str:
    """
    Mask each PEM block, whole or with its beginning missing.

    :param text: the output
    :return: the output with each block replaced by a mask
    """
    if _PEM_BEGIN not in text and _PEM_END not in text:
        return text
    out: List[str] = []
    inside = False
    for line in text.split("\n"):
        if inside:
            inside = _PEM_END not in line
            continue
        begin = line.find(_PEM_BEGIN)
        if begin >= 0:
            # What is before the block on its line is kept; what is after
            # it on its last line goes with it.
            out.append(line[:begin] + _MASK)
            inside = _PEM_END not in line[begin:]
        elif _PEM_END in line:
            # The end of a block whose beginning the ring had dropped: the
            # lines of its body are the ones just above.
            body = _PEM_BODY_END
            while out and body.match(out[-1].strip()):
                out.pop()
                body = _PEM_BODY
            out.append(_MASK)
        else:
            out.append(line)
    return "\n".join(out)

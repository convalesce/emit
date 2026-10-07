"""
Read the source of what a tool is about to run, to send with what it did.

A repository says what the code was meant to be. What ran is whatever was
deployed, and when the two differ an investigation that reads only the
repository explains the wrong program. So a plugin sends the text of the
function a step ran, and a hash of the file it came from, read here from
the running process.

On unless `CONVALESCE_SEND_SOURCE` is false. Nothing here raises: a
function with no readable source (a builtin, a lambda in a REPL, compiled
code) simply has none.

Import as:

import convalesce_emit.source as cesource
"""

import hashlib
import inspect
import os
from typing import Any, Dict, Optional

_ENV = "CONVALESCE_SEND_SOURCE"
_FALSY = frozenset({"0", "false", "no", "off"})

# A function longer than this is cut, keeping its head: the signature and
# the first statements say more about what it is than its last lines do.
MAX_CHARS = 60_000


def enabled() -> bool:
    """
    Whether source may be sent.

    :return: False only when the setting says so
    """
    return os.environ.get(_ENV, "").strip().lower() not in _FALSY


def _unwrapped(fn: Any) -> Any:
    """The function a decorator wrapped, where it says which."""
    try:
        return inspect.unwrap(fn)
    except Exception:  # pylint: disable=broad-exception-caught
        return fn


def of(fn: Any) -> Optional[Dict[str, Any]]:
    """
    What a callable is, as text.

    :param fn: the function a step runs
    :return: `{"text", "language", "name", "file", "line", "sha256",
        "file_sha256", "truncated"}`; None when sending is off or the
        source cannot be read
    """
    if not enabled() or fn is None:
        return None
    try:
        target = _unwrapped(fn)
        text = inspect.getsource(target)
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    out: Dict[str, Any] = {
        "text": text[:MAX_CHARS],
        "language": "python",
        "name": getattr(target, "__qualname__", None),
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "truncated": len(text) > MAX_CHARS,
    }
    try:
        out["file"] = inspect.getsourcefile(target)
        out["line"] = inspect.getsourcelines(target)[1]
    except Exception:  # pylint: disable=broad-exception-caught
        pass
    file_hash = file_sha256(out.get("file"))
    if file_hash:
        out["file_sha256"] = file_hash
    return out


def file_sha256(path: Optional[str]) -> Optional[str]:
    """
    The hash of a file as it is on this machine.

    :param path: the file
    :return: its sha256, or None when it cannot be read
    """
    if not path:
        return None
    try:
        with open(path, "rb") as handle:
            return hashlib.sha256(handle.read()).hexdigest()
    except OSError:
        return None

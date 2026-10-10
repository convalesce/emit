"""
The settings a run had, with every secret among them sent as a keyed hash.

A run's outcome can turn on a value nothing in its event carries: an
environment variable on its worker, a variable it looks up for itself. A
receiver that holds them for the last good run and for this one can say
which changed. That is all this is for, and it is off until
`CONVALESCE_SEND_SETTINGS` turns it on.

A setting crosses in one of two forms. An ordinary one crosses as its value.
One whose name or value reads as a credential, or that is too long to be
sure of, crosses as an HMAC-SHA-256 of its kind, name and value, cut to
sixteen hex characters: enough to tell that it changed, and nothing of what
it is. A bare hash of a short value could be reversed by trying candidates,
so it is keyed. The key is `CONVALESCE_FINGERPRINT_KEY` where that is set,
which never leaves the process, and otherwise one derived from the ingest
key under a fixed context. With neither, no hash is made and the setting is
declared as left out.

Hashes made under different keys cannot be compared, so they travel with
`keyed_by`: a hash of the key itself, equal between two events only when
their key was.

Neither a key nor a hashed value is logged or sent.

Import as:

import convalesce_emit.settings as cesettin
"""

import hashlib
import hmac
import logging
import math
import os
import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

import convalesce_emit.config as ceconfig

_LOG = logging.getLogger(__name__)

SETTING = "CONVALESCE_SEND_SETTINGS"
KEY_SETTING = "CONVALESCE_FINGERPRINT_KEY"
SKIP_SETTING = "CONVALESCE_SETTINGS_SKIP"

# Where the settings go in a payload, and what `excluded` paths start with.
FIELD = "settings"
ITEMS = "items"
KEYED_BY = "keyed_by"
ENVIRONMENT = "environment"
VARIABLE = "variable"

# What a key is mixed with to make the key hashes are made under. Changing
# it changes every hash, and `keyed_by` with them.
_CONTEXT = b"convalesce-emit/fingerprint/v1"
# Sixteen hex characters, 64 bits: two different values of one setting meet
# by chance about once in 2**64, and less of the digest crosses.
_LENGTH = 16
# How many settings of one kind cross. A worker's environment is a few
# hundred names at most; past this something else is being listed.
MAX_ITEMS = 500
# A value longer than this is hashed whatever it holds: a certificate, a
# JSON document of connection details, a script.
MAX_VALUE_CHARS = 300
_TRUTHY = frozenset({"1", "true", "yes", "on"})

# Matched against a name lower-cased with everything but letters and digits
# removed. Substrings, and wide on purpose: a name caught by mistake is
# hashed, which costs a value and never leaks one.
_SECRET_NAME_PARTS = (
    "key",
    "token",
    "secret",
    "auth",
    "pass",
    "pwd",
    "credential",
    "cookie",
    "sas",
    "signature",
    "private",
    "cert",
    "dsn",
    # A connection is where a password usually lives: `AIRFLOW_CONN_*` holds
    # one whole, as a URI or as JSON.
    "conn",
)
_URI_USERINFO = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*://[^/\s@]*:[^/\s@]+@")
_ASSIGNED = re.compile(
    r"(?i)(password|passwd|pwd|secret|token|api[_-]?key|credential"
    r"|signature|sig)[\"']?\s*[=:]"
)
_PEM = "-----BEGIN"
_JWT = re.compile(r"^eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.")
_TOKEN_CHARS = re.compile(r"^[A-Za-z0-9+/=_\-.~:]+$")
# How long an unbroken run of token characters is before its randomness is
# looked at, and how random it has to be, in bits a character. English and
# file paths sit near 3; a generated key near 4.5 and above.
_TOKEN_MIN_CHARS = 20
_TOKEN_MIN_ENTROPY = 3.5
# This package's own keys. They are what a hash is keyed with, so a hash of
# one would be made with itself.
_OWN_KEYS = frozenset(
    {"CONVALESCE_INGEST_KEY", "CONVALESCE_API_KEY", KEY_SETTING}
)


def enabled(environ: Optional[Mapping[str, str]] = None) -> bool:
    """
    Whether settings are sent at all. They are not unless asked for.

    :param environ: the variables to read; the process's own when not given
    :return: whether `CONVALESCE_SEND_SETTINGS` is switched on
    """
    value = ceconfig.read_setting(SETTING, environ)
    return (value or "").strip().lower() in _TRUTHY


def derive(secret: str) -> bytes:
    """
    The key hashes are made under.

    :param secret: the fingerprint key, or the ingest key where there is none
    :return: a key that says nothing of `secret` to anyone without it
    """
    return hmac.new(secret.encode("utf-8"), _CONTEXT, hashlib.sha256).digest()


def key_of(
    ingest_key: Optional[str], environ: Optional[Mapping[str, str]] = None
) -> Optional[bytes]:
    """
    The key this process hashes with.

    :param ingest_key: the key this process sends with, if it has one
    :param environ: the variables to read; the process's own when not given
    :return: the key, or None when there is nothing to make one from
    """
    own = (ceconfig.read_setting(KEY_SETTING, environ) or "").strip()
    secret = own or (ingest_key or "")
    return derive(secret) if secret else None


def of(key: bytes, kind: str, name: str, value: str) -> str:
    """
    Hash one value.

    :param key: what `key_of` returned
    :param kind: `environment`, `variable` or another kind a plugin names
    :param name: the setting's name
    :param value: its value; hashed, and not kept
    :return: the same sixteen hex characters for the same three, and others
        for any other
    """
    # A NUL cannot be in an environment variable's name or value, so no two
    # different triples read as the same bytes.
    message = "\0".join((kind, name, value)).encode("utf-8", "surrogatepass")
    return hmac.new(key, message, hashlib.sha256).hexdigest()[:_LENGTH]


def is_secret(name: str, value: str) -> bool:
    """
    Whether a setting crosses as a hash rather than as its value.

    :param name: the setting's name
    :param value: its value
    :return: whether its name or its value reads as a credential, or it is
        too long to be sure of
    """
    if len(value) > MAX_VALUE_CHARS:
        return True
    flat = "".join(ch for ch in name.lower() if ch.isalnum())
    if any(part in flat for part in _SECRET_NAME_PARTS):
        return True
    if _PEM in value or _URI_USERINFO.search(value) or _ASSIGNED.search(value):
        return True
    if _JWT.match(value):
        return True
    return _reads_as_token(value)


def collect(
    ingest_key: Optional[str],
    kinds: Mapping[str, Mapping[str, str]],
    environ: Optional[Mapping[str, str]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    """
    Turn a run's settings into what is sent for them.

    :param ingest_key: the key this process sends with, if it has one
    :param kinds: the settings, by kind and then by name
    :param environ: where this package's own settings are read; the
        process's variables when not given
    :return: what to send under `settings`, empty when there is nothing to
        send, and everything that was left out, by path and reason
    """
    try:
        if not enabled(environ):
            return {}, []
        key = key_of(ingest_key, environ)
        skip = _skipped(environ)
        excluded: List[Dict[str, str]] = []
        items: List[Dict[str, str]] = []
        unkeyed = 0
        for kind in sorted(kinds):
            names = sorted(n for n in kinds[kind] if _wanted(n, skip))
            if len(names) > MAX_ITEMS:
                names = names[:MAX_ITEMS]
                excluded.append(
                    {
                        "path": f"{FIELD}.{kind}",
                        "reason": f"limited to {MAX_ITEMS} names",
                    }
                )
            for name in names:
                value = kinds[kind][name]
                if not isinstance(value, str):
                    value = str(value)
                if not is_secret(name, value):
                    items.append({"kind": kind, "name": name, "value": value})
                elif key is None:
                    unkeyed += 1
                else:
                    items.append(
                        {
                            "kind": kind,
                            "name": name,
                            "fingerprint": of(key, kind, name, value),
                        }
                    )
        if unkeyed:
            excluded.append(
                {
                    "path": FIELD,
                    "reason": f"no key to fingerprint {unkeyed} settings with",
                }
            )
        if not items:
            return {}, excluded
        out: Dict[str, Any] = {ITEMS: items}
        if key is not None and any("fingerprint" in item for item in items):
            out[KEYED_BY] = of(key, KEYED_BY, "", "")
        return out, excluded
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # A run must not fail because its settings could not be read. The
        # exception's class and not its message, which may hold a value.
        _LOG.warning(
            "convalesce: could not collect settings: %s", type(exc).__name__
        )
        return {}, []


def environment(
    environ: Optional[Mapping[str, str]] = None,
) -> Dict[str, str]:
    """
    The environment variables of this process, as one kind of setting.

    :param environ: the variables to read; the process's own when not given
    :return: each variable's name to its value
    """
    return dict(os.environ if environ is None else environ)


# The variables a run looked up for itself, by kind and name. A tool's own
# reader is wrapped to note each read here, and what was noted is sent and
# forgotten when the run ends.
_NOTED: Dict[str, Dict[str, str]] = {}


def note(kind: str, name: str, value: Any) -> None:
    """
    Remember one setting a run read for itself.

    :param kind: the kind it is sent as, such as `variable`
    :param name: its name
    :param value: what the read returned; None is a read that found nothing
        and is not kept
    :return: nothing
    """
    try:
        if value is None or not enabled():
            return
        held = _NOTED.setdefault(kind, {})
        if name in held or len(held) < MAX_ITEMS:
            held[name] = value if isinstance(value, str) else repr(value)
    except Exception:  # pylint: disable=broad-exception-caught
        # Called from inside a tool's own reader, which must still return.
        pass


def noted(clear: bool = True) -> Dict[str, Dict[str, str]]:
    """
    What `note` has been told since it was last asked.

    :param clear: whether to forget it, as a run that has ended does
    :return: the settings read, by kind and then by name
    """
    out = {kind: dict(held) for kind, held in _NOTED.items()}
    if clear:
        _NOTED.clear()
    return out


def _skipped(environ: Optional[Mapping[str, str]]) -> frozenset:
    """
    The names `CONVALESCE_SETTINGS_SKIP` says are never sent.

    :param environ: the variables to read; the process's own when not given
    :return: the names it lists, comma-separated, exactly as written
    """
    listed = ceconfig.read_setting(SKIP_SETTING, environ) or ""
    return frozenset(n.strip() for n in listed.split(",") if n.strip())


def _wanted(name: str, skip: frozenset) -> bool:
    """
    Whether a setting is sent in either form.

    :param name: the setting's name
    :param skip: the names the operator listed as never sent
    :return: False for a listed name and for this package's own keys
    """
    if name in skip:
        return False
    bare = name[len(ceconfig.PLATFORM_PREFIX) :]
    if name.startswith(ceconfig.PLATFORM_PREFIX) and bare in _OWN_KEYS:
        return False
    return name not in _OWN_KEYS


def _reads_as_token(value: str) -> bool:
    """
    Whether a value looks generated rather than written.

    :param value: a setting's value
    :return: whether it is one long run of token characters, with letters
        and digits both, as random as a generated key is
    """
    if len(value) < _TOKEN_MIN_CHARS or not _TOKEN_CHARS.match(value):
        return False
    if not any(ch.isdigit() for ch in value):
        return False
    if not any(ch.isalpha() for ch in value):
        return False
    counts: Dict[str, int] = {}
    for ch in value:
        counts[ch] = counts.get(ch, 0) + 1
    total = float(len(value))
    entropy = -sum((n / total) * math.log2(n / total) for n in counts.values())
    return entropy >= _TOKEN_MIN_ENTROPY

"""
Strips sample data values, and secrets, from a payload before it is sent.

Great Expectations puts sample failing values straight into its validation
result, so forwarding a payload verbatim would send real row data. That
contradicts what the handbook promises: "We read metadata: table shapes, run
outcomes, row counts, lineage. Not the rows themselves."

Only the sample lists are replaced. Counts survive, because counts are what
detection reads, and the key survives too so the receiver never has to care
whether redaction ran. Every replacement is also declared, by path and
reason, in the list this returns alongside the payload -- the same
`excluded` shape `serialize.dump()`'s budget produces, so a caller can
concatenate the two and hand the envelope one list that says everything that
did not cross whole.

Secrets are a separate pass, `redact_secrets()`, for payloads a tool
assembled from its own configuration rather than from a run's results: an
OpenLineage event's facets can carry an operator's attributes and a
connection's URI. A value under a key named like a credential is replaced,
and a password inside a URI or a `key=value` string is masked in place, so
the SQL or URI around it still crosses.

Import as:

import convalesce_emit.redact as ceredact
"""

import logging
import re
from typing import Any, Dict, FrozenSet, List, Tuple

_LOG = logging.getLogger(__name__)

# `observed_value` is deliberately absent. For most expectations it is an
# aggregate -- a row count, a mean, a percentage -- and it is the substance
# of the result. For a few, such as a column max, it is a single value drawn
# from the data; that is a narrow, deliberate exception, worth revisiting if
# a customer ever objects.
SAMPLE_KEYS = frozenset(
    {
        "unexpected_list",
        "partial_unexpected_list",
        "unexpected_values",
        "partial_unexpected_counts",
        "unexpected_index_list",
        "partial_unexpected_index_list",
        "unexpected_index_query",
        "unexpected_rows",
    }
)


# Matched against a key lower-cased with everything but letters and digits
# removed, so `api_key`, `apiKey` and `API-KEY` are one name. Substrings, so
# `db_password` and `clientSecret` are caught; `token` is matched as a suffix
# only, because as a substring it is also in names like `tokenizer`.
SECRET_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "apikey",
    "accesskey",
    "privatekey",
    "credential",
    "authorization",
)
_SECRET_KEY_SUFFIX = "token"
_MASK = "***"
# `scheme://user:password@host`: the password is masked, the user and host
# kept, since they are what a receiver matches a dataset's source on.
_URI_PASSWORD = re.compile(
    r"(?P<head>[A-Za-z][A-Za-z0-9+.\-]*://[^:@/\s]*:)(?P<secret>[^@/\s]+)@"
)
# `password=...` in a query string or a JDBC/ODBC connection string.
_PARAM_SECRET = re.compile(
    r"(?P<head>(?:password|passwd|pwd|secret|token|api_?key)=)"
    r"(?P<secret>[^&;\s]+)",
    re.IGNORECASE,
)


def redact_samples(
    payload: Any,
    *,
    path: str = "",
    extra_keys: FrozenSet[str] = frozenset(),
) -> Tuple[Any, List[Dict[str, str]]]:
    """
    Replace sample data values with their counts.

    Walks the payload without knowing its schema, so a Great Expectations
    upgrade that adds another sample-bearing field is still caught as long as
    it is named like its siblings.

    :param payload: the tool's output, possibly carrying row values
    :param path: dotted path of `payload` from the envelope root, so a
        redaction made anywhere but the top can still be placed
    :param extra_keys: names to redact on top of `SAMPLE_KEYS`, additive and
        scoped to this call -- a caller with its own author-written free-text
        surface (Dagster's materialisation metadata, say) names it here
        rather than widening what every other caller redacts too
    :return: the same shape, with sample lists summarised, and what was
        redacted, by path and reason
    """
    keys = SAMPLE_KEYS | extra_keys
    excluded: List[Dict[str, str]] = []
    redacted = _walk(payload, path, keys, excluded)
    return redacted, excluded


def _walk(
    payload: Any, path: str, keys: FrozenSet[str], excluded: List[Dict[str, str]]
) -> Any:
    """
    Recurse through one value, redacting sample keys as they are found.

    :param payload: the value being walked
    :param path: dotted path of `payload` from the envelope root
    :param keys: names that mean a sample wherever they occur
    :param excluded: accumulator every redaction is appended to
    :return: the same shape, with sample lists summarised
    """
    if isinstance(payload, dict):
        out: Dict[str, Any] = {}
        for key, value in payload.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key in keys:
                out[key] = _summarise(value)
                excluded.append(
                    {"path": child_path, "reason": "sample redacted"}
                )
            else:
                out[key] = _walk(value, child_path, keys, excluded)
        return out
    if isinstance(payload, list):
        return [
            _walk(item, f"{path}[{i}]", keys, excluded)
            for i, item in enumerate(payload)
        ]
    return payload


def _summarise(value: Any) -> Any:
    """
    Describe a sample without disclosing it.

    :param value: the sample list or mapping being replaced
    :return: a marker carrying the count, where there was one
    """
    if isinstance(value, (list, dict)):
        return {"redacted": True, "count": len(value)}
    if value is None:
        return None
    return {"redacted": True}


def redact_secrets(
    payload: Any, *, path: str = ""
) -> Tuple[Any, List[Dict[str, str]]]:
    """
    Replace credentials: values under secret-named keys, and passwords
    inside strings.

    Like `redact_samples()`, walks the payload without knowing its schema
    and declares every replacement in the same `excluded` shape.

    :param payload: JSON-shaped data that may carry configuration
    :param path: dotted path of `payload` from the envelope root
    :return: the same shape, with credentials replaced or masked, and what
        was redacted, by path and reason
    """
    excluded: List[Dict[str, str]] = []
    redacted = _walk_secrets(payload, path, excluded)
    return redacted, excluded


def _walk_secrets(
    payload: Any, path: str, excluded: List[Dict[str, str]]
) -> Any:
    """
    Recurse through one value, redacting credentials as they are found.

    :param payload: the value being walked
    :param path: dotted path of `payload` from the envelope root
    :param excluded: accumulator every redaction is appended to
    :return: the same shape, with credentials replaced or masked
    """
    if isinstance(payload, dict):
        out: Dict[str, Any] = {}
        for key, value in payload.items():
            child_path = f"{path}.{key}" if path else str(key)
            if _is_secret_key(key) and value not in (None, ""):
                out[key] = {"redacted": True}
                excluded.append(
                    {"path": child_path, "reason": "secret redacted"}
                )
            else:
                out[key] = _walk_secrets(value, child_path, excluded)
        return out
    if isinstance(payload, list):
        return [
            _walk_secrets(item, f"{path}[{i}]", excluded)
            for i, item in enumerate(payload)
        ]
    if isinstance(payload, str):
        masked = _mask_string(payload)
        if masked != payload:
            excluded.append({"path": path or "$", "reason": "credential masked"})
        return masked
    return payload


def _is_secret_key(key: Any) -> bool:
    """
    Whether a key names a credential.

    :param key: a mapping key, of any type
    :return: whether its value should be withheld
    """
    if not isinstance(key, str):
        return False
    name = "".join(ch for ch in key.lower() if ch.isalnum())
    if name.endswith(_SECRET_KEY_SUFFIX):
        return True
    return any(part in name for part in SECRET_KEY_PARTS)


def _mask_string(text: str) -> str:
    """
    Mask passwords embedded in a string, leaving the rest of it intact.

    :param text: any string, such as a URI or SQL text
    :return: the string, with each embedded password replaced by a mask
    """
    text = _URI_PASSWORD.sub(rf"\g<head>{_MASK}@", text)
    return _PARAM_SECRET.sub(rf"\g<head>{_MASK}", text)

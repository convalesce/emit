"""
Strips sample data values from a payload before it is sent.

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

Import as:

import convalesce_emit.redact as ceredact
"""

import logging
from typing import Any, Dict, List, Tuple

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


def redact_samples(
    payload: Any, *, path: str = ""
) -> Tuple[Any, List[Dict[str, str]]]:
    """
    Replace sample data values with their counts.

    Walks the payload without knowing its schema, so a Great Expectations
    upgrade that adds another sample-bearing field is still caught as long as
    it is named like its siblings.

    :param payload: the tool's output, possibly carrying row values
    :param path: dotted path of `payload` from the envelope root, so a
        redaction made anywhere but the top can still be placed
    :return: the same shape, with sample lists summarised, and what was
        redacted, by path and reason
    """
    excluded: List[Dict[str, str]] = []
    redacted = _walk(payload, path, excluded)
    return redacted, excluded


def _walk(payload: Any, path: str, excluded: List[Dict[str, str]]) -> Any:
    """
    Recurse through one value, redacting sample keys as they are found.

    :param payload: the value being walked
    :param path: dotted path of `payload` from the envelope root
    :param excluded: accumulator every redaction is appended to
    :return: the same shape, with sample lists summarised
    """
    if isinstance(payload, dict):
        out: Dict[str, Any] = {}
        for key, value in payload.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key in SAMPLE_KEYS:
                out[key] = _summarise(value)
                excluded.append(
                    {"path": child_path, "reason": "sample redacted"}
                )
            else:
                out[key] = _walk(value, child_path, excluded)
        return out
    if isinstance(payload, list):
        return [
            _walk(item, f"{path}[{i}]", excluded)
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

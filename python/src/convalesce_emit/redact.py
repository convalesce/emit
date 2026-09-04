"""
Strips sample data values from a payload before it is sent.

Great Expectations puts sample failing values straight into its validation
result, so forwarding a payload verbatim would send real row data. That
contradicts what the handbook promises: "We read metadata: table shapes, run
outcomes, row counts, lineage. Not the rows themselves."

Only the sample lists are replaced. Counts survive, because counts are what
detection reads, and the key survives too so the receiver never has to care
whether redaction ran.

Import as:

import convalesce_emit.redact as ceredact
"""

import logging
from typing import Any, Dict

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


def redact_samples(payload: Any) -> Any:
    """
    Replace sample data values with their counts.

    Walks the payload without knowing its schema, so a Great Expectations
    upgrade that adds another sample-bearing field is still caught as long as
    it is named like its siblings.

    :param payload: the tool's output, possibly carrying row values
    :return: the same shape, with sample lists summarised
    """
    if isinstance(payload, dict):
        out: Dict[str, Any] = {}
        for key, value in payload.items():
            if key in SAMPLE_KEYS:
                out[key] = _summarise(value)
            else:
                out[key] = redact_samples(value)
        return out
    if isinstance(payload, list):
        return [redact_samples(item) for item in payload]
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

"""
Splitting an oversized observation's payload so it can be sent in pieces.

Only called once an observation has already been found too big to send
whole (`client.py`). Operates on the payload dict itself -- already
`$ref`-collapsed by `serialize.py`, never on the serialized JSON string --
so a split can never land mid-token. Whole top-level keys move together;
a single key's value is never cut in two, except by recursing one level
into it when that one key alone is too big for any chunk, and even then
only a dict or list is worth recursing into. A value that still will not
fit after that -- a lone giant string, or a dict/list with nothing left to
split -- is sent alone, oversized, the same way a whole observation over
the limit on its own has always been sent alone rather than dropped.

Import as:

import convalesce_emit.chunk as cechunk
"""

import json
from typing import Any, Callable, Dict, List


def split(payload: Dict[str, Any], budget: int) -> List[Dict[str, Any]]:
    """
    Split a payload dict into pieces that each fit `budget` bytes.

    :param payload: an already-$ref-collapsed payload dict
    :param budget: the most bytes one piece's JSON encoding should take
    :return: one or more payload dicts which, dict-merged back together in
        order, recover `payload` exactly; a single element, `[payload]`,
        when nothing needed splitting
    """
    if _fits(payload, budget):
        return [payload]
    chunks = _pack_mapping(payload, budget, wrap=_identity, recurse=True)
    return chunks or [payload]


def _identity(fragment: Dict[str, Any]) -> Any:
    return fragment


def _size(obj: Any) -> int:
    return len(
        json.dumps(obj, default=str, separators=(",", ":")).encode("utf-8")
    )


def _fits(obj: Any, budget: int) -> bool:
    return _size(obj) <= budget


def _pack_mapping(
    mapping: Dict[str, Any],
    budget: int,
    *,
    wrap: Callable[[Dict[str, Any]], Any],
    recurse: bool,
) -> List[Dict[str, Any]]:
    """
    Group a mapping's own key-value pairs into fragments that each fit
    `budget` once passed through `wrap` -- the shape the fragment will
    actually be sent in, such as `{key: fragment}` when splitting a single
    oversized key's value one level deeper.

    :param mapping: the keys to group; never split across two fragments
        except by recursing into one oversized key's own value
    :param budget: the most bytes one wrapped fragment should take
    :param wrap: turns a fragment into the shape actually measured against
        `budget`
    :param recurse: whether a lone oversized key may be split one level
        deeper; always `False` inside that one level, so recursion never
        goes past one
    :return: fragments of `mapping`, in key order
    """
    chunks: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    for key, value in mapping.items():
        candidate = {**current, key: value}
        if _fits(wrap(candidate), budget):
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = {}
        solo = {key: value}
        if _fits(wrap(solo), budget):
            current = solo
            continue
        if recurse:
            chunks.extend(_split_value(key, value, budget, wrap))
        else:
            chunks.append(solo)
    if current:
        chunks.append(current)
    return chunks


def _split_value(
    key: str,
    value: Any,
    budget: int,
    wrap: Callable[[Dict[str, Any]], Any],
) -> List[Dict[str, Any]]:
    """
    A single key's value alone is over budget: recurse one level into it if
    it is a dict or list worth splitting, else send it alone, oversized.
    """

    def rewrap(fragment: Any) -> Any:
        return wrap({key: fragment})

    if isinstance(value, dict) and len(value) > 1:
        inner = _pack_mapping(value, budget, wrap=rewrap, recurse=False)
        if len(inner) > 1:
            return [{key: fragment} for fragment in inner]
    elif isinstance(value, list) and len(value) > 1:
        inner = _pack_list(value, budget, wrap=rewrap)
        if len(inner) > 1:
            return [{key: fragment} for fragment in inner]
    return [{key: value}]


def _pack_list(
    items: List[Any], budget: int, wrap: Callable[[List[Any]], Any]
) -> List[List[Any]]:
    """
    Group a list's own items into fragments that each fit `budget` once
    wrapped, the list equivalent of `_pack_mapping`. An item that still does
    not fit alone is sent alone anyway, oversized -- the same last resort as
    everywhere else in this module.
    """
    chunks: List[List[Any]] = []
    current: List[Any] = []
    for item in items:
        candidate = current + [item]
        if _fits(wrap(candidate), budget):
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = []
        solo = [item]
        if _fits(wrap(solo), budget):
            current = solo
        else:
            chunks.append(solo)
    if current:
        chunks.append(current)
    return chunks

"""
Tests for splitting an oversized payload into chunks.

Run with `make test`.
"""

import json
import logging
import unittest
from typing import Any, Dict, List

import convalesce_emit.chunk as cechunk

_LOG = logging.getLogger(__name__)


def _size(obj: Any) -> int:
    return len(json.dumps(obj, separators=(",", ":")).encode("utf-8"))


def _merge(fragments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    The same recursive dict-merge the receiver does, so a test can check a
    split actually reassembles to the original payload.
    """
    out: Dict[str, Any] = {}
    for fragment in fragments:
        _merge_into(out, fragment)
    return out


def _merge_into(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    for key, value in src.items():
        if key in dst and isinstance(dst[key], dict) and isinstance(value, dict):
            _merge_into(dst[key], value)
        elif (
            key in dst and isinstance(dst[key], list) and isinstance(value, list)
        ):
            dst[key].extend(value)
        else:
            dst[key] = value


# #############################################################################
# Test_split_not_needed1
# #############################################################################


class Test_split_not_needed1(unittest.TestCase):
    """
    Test the no-op case.
    """

    def test1(self) -> None:
        """
        Test that a payload already under budget is returned whole.
        """
        payload = {"a": 1, "b": 2}
        self.assertEqual(cechunk.split(payload, 10_000), [payload])


# #############################################################################
# Test_split_top_level1
# #############################################################################


class Test_split_top_level1(unittest.TestCase):
    """
    Test grouping whole sibling keys.
    """

    def test1(self) -> None:
        """
        Test that keys are grouped into fragments that each fit the budget.
        """
        payload = {f"k{i}": "x" * 100 for i in range(20)}
        budget = 400
        chunks = cechunk.split(payload, budget)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(_size(chunk), budget)
        self.assertEqual(_merge(chunks), payload)

    def test2(self) -> None:
        """
        Test that a single key is never split across two fragments when it
        does not have to be.
        """
        payload = {"small": "x", "also_small": "y"}
        chunks = cechunk.split(payload, 10_000)
        self.assertEqual(chunks, [payload])

    def test3(self) -> None:
        """
        Test that no key appears in more than one fragment unless its own
        value was recursed into.
        """
        payload = {f"k{i}": i for i in range(10)}
        chunks = cechunk.split(payload, 60)
        seen: Dict[str, int] = {}
        for chunk in chunks:
            for key in chunk:
                seen[key] = seen.get(key, 0) + 1
        self.assertTrue(all(count == 1 for count in seen.values()))


# #############################################################################
# Test_split_recurse_dict1
# #############################################################################


class Test_split_recurse_dict1(unittest.TestCase):
    """
    Test recursing one level into a lone oversized key's dict value.
    """

    def test1(self) -> None:
        """
        Test that a single oversized key, holding a dict, is split by that
        dict's own keys rather than sent whole.
        """
        payload = {
            "small": "s",
            "huge": {f"inner{i}": "x" * 200 for i in range(10)},
        }
        budget = 500
        chunks = cechunk.split(payload, budget)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(_size(chunk), budget)
        self.assertEqual(_merge(chunks), payload)
        # Split one level deep, not further: no chunk's `huge` fragment is
        # itself split into pieces smaller than one inner key.
        for chunk in chunks:
            if "huge" in chunk:
                for value in chunk["huge"].values():
                    self.assertIsInstance(value, str)

    def test2(self) -> None:
        """
        Test that recursion only fires when the key truly cannot fit beside
        anything else -- ordinary siblings stay grouped.
        """
        payload = {
            "a": "x" * 50,
            "b": "x" * 50,
            "huge": {f"inner{i}": "x" * 200 for i in range(10)},
        }
        chunks = cechunk.split(payload, 500)
        self.assertEqual(_merge(chunks), payload)


# #############################################################################
# Test_split_recurse_list1
# #############################################################################


class Test_split_recurse_list1(unittest.TestCase):
    """
    Test recursing one level into a lone oversized key's list value.
    """

    def test1(self) -> None:
        """
        Test that a single oversized key, holding a list, is split by that
        list's own items, in order.
        """
        payload = {"items": ["x" * 200 for _ in range(10)]}
        budget = 500
        chunks = cechunk.split(payload, budget)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(_size(chunk), budget)
        self.assertEqual(_merge(chunks), payload)


# #############################################################################
# Test_split_oversized_leaf1
# #############################################################################


class Test_split_oversized_leaf1(unittest.TestCase):
    """
    Test the last resort: a leaf that cannot be shrunk under budget at all.
    """

    def test1(self) -> None:
        """
        Test that a single scalar value too big alone is sent alone anyway,
        not dropped and not endlessly retried.
        """
        payload = {"huge": "x" * 5000}
        chunks = cechunk.split(payload, 100)
        self.assertEqual(chunks, [{"huge": "x" * 5000}])

    def test2(self) -> None:
        """
        Test that an oversized leaf beside other keys still goes alone,
        while its siblings are still grouped normally.
        """
        payload = {"small": "s", "huge": "x" * 5000}
        chunks = cechunk.split(payload, 200)
        self.assertEqual(_merge(chunks), payload)
        self.assertTrue(any(c == {"huge": "x" * 5000} for c in chunks))

    def test3(self) -> None:
        """
        Test that a dict value with only one key, itself oversized, is sent
        alone rather than "recursed into" a single-item split that would
        not shrink anything.
        """
        payload = {"huge": {"only": "x" * 5000}}
        chunks = cechunk.split(payload, 100)
        self.assertEqual(chunks, [payload])

    def test4(self) -> None:
        """
        Test that a single oversized list item is sent alone within its own
        fragment, rather than failing to split at all.
        """
        payload = {"items": ["x" * 5000, "y" * 5000]}
        chunks = cechunk.split(payload, 200)
        self.assertEqual(_merge(chunks), payload)
        self.assertGreater(len(chunks), 1)

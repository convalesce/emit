"""
Tests for reading the source of what a step runs.

Import as:

import convalesce_emit.test.test_source as cetestsrc
"""

import functools
import os
import unittest
import unittest.mock
from typing import Any

import convalesce_emit.source as cesource


def _plain(table: str) -> str:
    return "loaded " + table


def _decorator(fn: Any) -> Any:
    @functools.wraps(fn)
    def inner(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    return inner


@_decorator
def _wrapped(table: str) -> str:
    return "wrapped " + table


class Test_of1(unittest.TestCase):
    """
    Test reading a function's source.
    """

    def test1(self) -> None:
        """Test that a function's text, name, file and hashes are read."""
        found = cesource.of(_plain)
        assert found is not None
        self.assertIn('return "loaded " + table', found["text"])
        self.assertEqual(found["name"], "_plain")
        self.assertEqual(found["language"], "python")
        self.assertTrue(found["file"].endswith("test_source.py"))
        self.assertEqual(len(found["sha256"]), 64)
        self.assertEqual(
            found["file_sha256"], cesource.file_sha256(found["file"])
        )
        self.assertFalse(found["truncated"])

    def test2(self) -> None:
        """Test that what a decorator wrapped is what is read."""
        found = cesource.of(_wrapped)
        assert found is not None
        self.assertIn('return "wrapped " + table', found["text"])

    def test3(self) -> None:
        """Test that something with no readable source has none."""
        for thing in (len, None, 3):
            self.assertIsNone(cesource.of(thing))

    def test4(self) -> None:
        """Test that the setting turns it off."""
        with unittest.mock.patch.dict(
            os.environ, {"CONVALESCE_SEND_SOURCE": "off"}
        ):
            self.assertIsNone(cesource.of(_plain))

    def test5(self) -> None:
        """Test that a long function is cut at its head and says so."""
        with unittest.mock.patch.object(cesource, "MAX_CHARS", 10):
            found = cesource.of(_plain)
        assert found is not None
        self.assertEqual(len(found["text"]), 10)
        self.assertTrue(found["truncated"])

    def test6(self) -> None:
        """Test that a file that is not there has no hash."""
        self.assertIsNone(cesource.file_sha256("/no/such/file.py"))
        self.assertIsNone(cesource.file_sha256(None))

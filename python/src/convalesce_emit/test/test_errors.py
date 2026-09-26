"""
Tests for describing a tool's exception.

Run with `make test`.
"""

import logging
import unittest
from typing import Callable

import convalesce_emit.errors as ceerrors

_LOG = logging.getLogger(__name__)


def _raised(exc: BaseException) -> BaseException:
    """
    Raise and catch an exception, so it carries a real traceback.

    :param exc: the exception to raise
    :return: the same exception, as caught
    """

    def fail() -> None:
        raise exc

    return _caught(fail)


def _caught(fail: Callable[[], None]) -> BaseException:
    """
    What a function raises.

    :param fail: a function that raises
    :return: the exception it raised, as caught
    """
    try:
        fail()
    except BaseException as caught:  # pylint: disable=broad-exception-caught
        return caught
    raise AssertionError("did not raise")


# #############################################################################
# Test_error_detail1
# #############################################################################


class Test_error_detail1(unittest.TestCase):
    """
    Test that an exception is described by class, message and traceback.
    """

    def test1(self) -> None:
        """
        Test that the type names the module and class, and the message and
        traceback are Python's own.
        """
        detail = ceerrors.error_detail(_raised(ValueError("bad row")))
        self.assertEqual(detail["type"], "builtins.ValueError")
        self.assertEqual(detail["message"], "bad row")
        self.assertIn("Traceback (most recent call last)", detail["traceback"])
        self.assertIn("ValueError: bad row", detail["traceback"])
        self.assertIn("in fail", detail["traceback"])
        self.assertNotIn("cause", detail)

    def test2(self) -> None:
        """
        Test that a class defined in a module and nested in another is
        named by its qualified name.
        """

        class Outer:
            """Holds the exception class."""

            class Refused(Exception):
                """A tool's own exception."""

        detail = ceerrors.error_detail(Outer.Refused("no"))
        self.assertEqual(
            detail["type"],
            f"{__name__}.Test_error_detail1.test2.<locals>.Outer.Refused",
        )

    def test3(self) -> None:
        """
        Test that an exception never raised has no frames but still has a
        type and message.
        """
        detail = ceerrors.error_detail(KeyError("orders"))
        self.assertEqual(detail["message"], "'orders'")
        self.assertEqual(detail["traceback"], "KeyError: 'orders'\n")

    def test4(self) -> None:
        """
        Test that a message whose `__str__` raises is replaced by the type
        name rather than raising.
        """

        class Unprintable(Exception):
            """An exception that cannot render itself."""

            def __str__(self) -> str:
                raise RuntimeError("no")

        detail = ceerrors.error_detail(Unprintable())
        self.assertEqual(detail["message"], "<Unprintable>")
        self.assertIsInstance(detail["traceback"], str)


# #############################################################################
# Test_error_detail_cap1
# #############################################################################


class Test_error_detail_cap1(unittest.TestCase):
    """
    Test that a long traceback keeps its tail.
    """

    def test1(self) -> None:
        """
        Test that the traceback is capped at the limit and keeps the line
        naming the exception, which is at the end.
        """
        message = "x" * (ceerrors.TRACEBACK_LIMIT * 2) + " END"
        detail = ceerrors.error_detail(_raised(RuntimeError(message)))
        self.assertEqual(len(detail["traceback"]), ceerrors.TRACEBACK_LIMIT)
        self.assertTrue(detail["traceback"].endswith(" END\n"))
        self.assertEqual(detail["message"], message)

    def test2(self) -> None:
        """
        Test that a traceback under the limit is left whole.
        """
        detail = ceerrors.error_detail(_raised(RuntimeError("short")))
        self.assertTrue(detail["traceback"].startswith("Traceback"))


# #############################################################################
# Test_error_detail_cause1
# #############################################################################


class Test_error_detail_cause1(unittest.TestCase):
    """
    Test that the exception behind this one is described one level deep.
    """

    def test1(self) -> None:
        """
        Test that an explicit cause is described, without its own cause,
        and is not repeated in the outer traceback.
        """
        middle = OSError("socket")
        middle.__cause__ = ConnectionError("refused")

        def fail() -> None:
            try:
                raise middle
            except OSError as exc:
                raise RuntimeError("load failed") from exc

        detail = ceerrors.error_detail(_caught(fail))
        self.assertEqual(detail["cause"]["type"], "builtins.OSError")
        self.assertEqual(detail["cause"]["message"], "socket")
        self.assertNotIn("cause", detail["cause"])
        self.assertNotIn("socket", detail["traceback"])

    def test2(self) -> None:
        """
        Test that an exception raised while handling another carries that
        one as its cause.
        """

        def fail() -> None:
            try:
                raise KeyError("id")
            except KeyError:
                # The implicit chain is the point of the test.
                raise ValueError("lookup")  # pylint: disable=raise-missing-from

        detail = ceerrors.error_detail(_caught(fail))
        self.assertEqual(detail["cause"]["type"], "builtins.KeyError")

    def test3(self) -> None:
        """
        Test that a context suppressed with `from None` is not described.
        """

        def fail() -> None:
            try:
                raise KeyError("id")
            except KeyError:
                raise ValueError("lookup") from None

        detail = ceerrors.error_detail(_caught(fail))
        self.assertNotIn("cause", detail)

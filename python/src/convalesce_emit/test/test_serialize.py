"""
Tests for shaping a tool's own objects into JSON.

Run with `make test`.
"""

import json
import logging
import unittest
from typing import Any

import convalesce_emit.serialize as ceserial

_LOG = logging.getLogger(__name__)


# #############################################################################
# Test_dump1
# #############################################################################


class Test_dump1(unittest.TestCase):
    """
    Test that an arbitrary object crosses without raising.
    """

    def test1(self) -> None:
        """
        Test that scalars and containers survive unchanged.
        """
        self.assertEqual(ceserial.dump({"a": (1, 2)}), {"a": [1, 2]})
        self.assertEqual(ceserial.dump([None, True, 1.5]), [None, True, 1.5])

    def test2(self) -> None:
        """
        Test that a plain object is described by its public attributes.

        Leading underscores are the tool's internals, not its state, so they
        are left behind.
        """

        class Task:
            """Stands in for an Airflow task instance."""

            def __init__(self) -> None:
                self.dag_id = "orders"
                self._private = "hidden"

        out = ceserial.dump(Task())
        self.assertEqual(out["dag_id"], "orders")
        self.assertNotIn("_private", out)

    def test3(self) -> None:
        """
        Test that an object's own dump method is preferred.
        """

        class Model:
            """Stands in for a pydantic model."""

            def model_dump(self) -> Any:
                """Describe itself the way pydantic does."""
                return {"from": "model_dump"}

        self.assertEqual(ceserial.dump(Model()), {"from": "model_dump"})

    def test4(self) -> None:
        """
        Test that a failing dump method does not stop the observation.

        A tool's own serialiser is not ours to fix; a partial observation is
        worth more than none.
        """

        class Awkward:
            """An object whose own serialiser is broken."""

            def to_dict(self) -> Any:
                """Fail, the way a half-built tool object does."""
                raise RuntimeError("nope")

            def __str__(self) -> str:
                return "awkward"

        self.assertEqual(ceserial.dump(Awkward()), "awkward")

    def test5(self) -> None:
        """
        Test that a self-referential object terminates rather than spinning.
        """
        node: dict = {"name": "a"}
        node["self"] = node
        # Deep, but finite.
        self.assertIsInstance(ceserial.dump(node), dict)


# #############################################################################
# Test_dump_budget1
# #############################################################################


class Test_dump_budget1(unittest.TestCase):
    """
    Test that an object graph cannot produce an unbounded payload.
    """

    def test1(self) -> None:
        """
        Test that a logger-shaped graph does not drag in the runtime.

        A real Prefect flow reaches its task runner, then that runner's
        logger, then the logging manager and every logger in the process:
        one flow run produced a 256 MB payload before the walker had
        budgets. The customer's worker is the one that pays for that.
        """

        class Runner:
            """Stands in for a task runner holding a live logger."""

            def __init__(self) -> None:
                self.name = "threadpool"
                self.logger = logging.getLogger()

        class Flow:
            """Stands in for a Prefect flow."""

            def __init__(self) -> None:
                self.name = "nightly"
                self.task_runner = Runner()

        out = ceserial.dump(Flow())
        encoded = json.dumps(out, default=str)
        self.assertEqual(out["name"], "nightly")
        self.assertLess(len(encoded), 100_000)

    def test2(self) -> None:
        """
        Test that breadth is capped, not only depth.

        One object holding thousands of entries is shallow and still
        enormous, so a depth limit alone would not have caught it.
        """
        wide = {f"k{i}": {"v": i} for i in range(10_000)}
        encoded = json.dumps(ceserial.dump(wide), default=str)
        self.assertLess(len(encoded), 200_000)

    def test3(self) -> None:
        """
        Test that a cycle is reported rather than followed.
        """
        node: dict = {"name": "a"}
        node["self"] = node
        out = ceserial.dump(node)
        self.assertEqual(out["name"], "a")
        self.assertIn("cycle", str(out["self"]))

    def test4(self) -> None:
        """
        Test that a namedtuple keeps its field names.

        A Dagster run is a namedtuple, and namedtuples are tuples, so a
        tuple branch placed first turns one into an anonymous array and the
        receiver loses every field name.
        """
        import collections

        Run = collections.namedtuple("Run", ["job_name", "run_id"])
        out = ceserial.dump(Run(job_name="nightly", run_id="abc"))
        self.assertEqual(out, {"job_name": "nightly", "run_id": "abc"})

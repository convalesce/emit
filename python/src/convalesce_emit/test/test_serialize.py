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


# #############################################################################
# Test_dump_depth1
# #############################################################################


class Test_dump_depth1(unittest.TestCase):
    """
    Test that nesting is counted the way the tools nest.
    """

    def test1(self) -> None:
        """
        Test that an object and its mapping are one level, and a list none.

        An Airflow task instance holds a task, which holds a list of inlet
        objects: three objects deep, and they arrived as their repr while an
        object spent one level on itself and another on its dict.
        """

        class Inlet:
            """Stands in for an Airflow Dataset."""

            def __init__(self, uri: str) -> None:
                self.uri = uri

        class Task:
            """Stands in for an operator."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.inlets = [Inlet("s3://bucket/orders")]
                self.upstream_task_ids = {"extract"}

        class TaskInstance:
            """Stands in for a task instance."""

            def __init__(self) -> None:
                self.task = Task()

        out = ceserial.dump({"task_instance": TaskInstance()})
        task = out["task_instance"]["task"]
        self.assertEqual(task["inlets"], [{"uri": "s3://bucket/orders"}])
        self.assertEqual(task["upstream_task_ids"], ["extract"])

    def test2(self) -> None:
        """
        Test that an object dumped through its own method costs one level.

        A Dagster run is a namedtuple holding a namedtuple origin; the origin
        arrived as text.
        """
        import collections

        Origin = collections.namedtuple("Origin", ["module", "fn_name"])
        Run = collections.namedtuple("Run", ["run_id", "origin"])
        out = ceserial.dump({"run": Run("r1", Origin("pipelines", "defs"))})
        self.assertEqual(
            out["run"]["origin"], {"module": "pipelines", "fn_name": "defs"}
        )

    def test3(self) -> None:
        """
        Test that the depth limit still holds on pathological nesting.
        """
        deep: Any = "leaf"
        for _ in range(20):
            deep = {"child": deep}
        out = ceserial.dump(deep)
        text = json.dumps(out)
        self.assertIn("child", text)
        self.assertNotIn('"child": "leaf"', text)

    def test4(self) -> None:
        """
        Test that a Great Expectations result keeps its counts.

        The plugin wraps what the action was handed in `args`/`kwargs`, so
        the per-expectation counts detection reads sit eight levels down:
        the counts arrived as text, and so did the column the expectation
        was about.
        """

        class Identifier:
            """Stands in for a ValidationResultIdentifier."""

            def __str__(self) -> str:
                return "ValidationResultIdentifier::orders"

        class SuiteResult:
            """Stands in for an ExpectationSuiteValidationResult."""

            def to_json_dict(self) -> Any:
                """Describe itself the way GX does."""
                return {
                    "success": False,
                    "suite_name": "orders",
                    "results": [
                        {
                            "success": False,
                            "expectation_config": {
                                "type": "expect_column_values_to_not_be_null",
                                "kwargs": {"column": "email"},
                            },
                            "result": {
                                "element_count": 4,
                                "unexpected_count": 1,
                            },
                        }
                    ],
                }

        class CheckpointResult:
            """Stands in for a CheckpointResult."""

            def __init__(self) -> None:
                self.success = False
                self.run_results = {Identifier(): SuiteResult()}

        out = ceserial.dump(
            {"args": [], "kwargs": {"checkpoint_result": CheckpointResult()}}
        )
        results = out["kwargs"]["checkpoint_result"]["run_results"]
        suite = results["ValidationResultIdentifier::orders"]
        expectation = suite["results"][0]
        self.assertEqual(expectation["result"]["element_count"], 4)
        self.assertEqual(expectation["result"]["unexpected_count"], 1)
        self.assertEqual(
            expectation["expectation_config"]["kwargs"]["column"], "email"
        )


# #############################################################################
# Test_dump_property1
# #############################################################################


class Test_dump_property1(unittest.TestCase):
    """
    Test that a property backed by a private attribute is read.
    """

    def test1(self) -> None:
        """
        Test that the public name carries the property's value.

        Airflow keeps `dag_id` in `_dag_id`, a run's `state` in `_state`
        and, before 2.10, `try_number` in `_try_number`; all three were
        dropped with the underscore.
        """

        class DagRun:
            """Stands in for an Airflow DagRun."""

            def __init__(self) -> None:
                self._state = "running"
                self._dag_id = "orders"
                self._secret = "hidden"
                self.run_id = "manual__1"

            @property
            def state(self) -> str:
                """The run's state."""
                return self._state.upper()

            @property
            def dag_id(self) -> str:
                """The DAG's id."""
                return self._dag_id

        out = ceserial.dump(DagRun())
        self.assertEqual(out["state"], "RUNNING")
        self.assertEqual(out["dag_id"], "orders")
        self.assertEqual(out["run_id"], "manual__1")
        self.assertNotIn("_secret", out)
        self.assertNotIn("secret", out)

    def test2(self) -> None:
        """
        Test that a skipped name stays skipped when it is a property.

        Airflow's task instance keeps its logger in `_log` behind a `log`
        property, and reading the property put a live logger back into the
        payload that the skip list exists to keep out.
        """

        class TaskInstance:
            """Stands in for an Airflow task instance."""

            def __init__(self) -> None:
                self._log = logging.getLogger("airflow.task")
                self._state = "failed"

            @property
            def log(self) -> Any:
                """The task's logger."""
                return self._log

            @property
            def state(self) -> str:
                """The task's state."""
                return self._state

        out = ceserial.dump(TaskInstance())
        self.assertEqual(out["state"], "failed")
        self.assertNotIn("log", out)
        self.assertNotIn("_log", out)

    def test3(self) -> None:
        """
        Test that a property that raises does not stop the object.
        """

        class Awkward:
            """An object whose property needs something it lacks."""

            def __init__(self) -> None:
                self._log_url = "stored"
                self.name = "ok"

            @property
            def log_url(self) -> str:
                """Fail, the way a property needing a session does."""
                raise RuntimeError("no session")

        out = ceserial.dump(Awkward())
        self.assertEqual(out["name"], "ok")
        self.assertIn("unreadable", out["log_url"])


# #############################################################################
# Test_dump_keys1
# #############################################################################


class Test_dump_keys1(unittest.TestCase):
    """
    Test that mappings keyed by objects and enum values cross as data.
    """

    def test1(self) -> None:
        """
        Test that a non-string key becomes its text and the value is walked.

        A Great Expectations checkpoint keys its results by identifier
        objects, and the whole mapping arrived as one string.
        """

        class Identifier:
            """Stands in for a ValidationResultIdentifier."""

            def __str__(self) -> str:
                return "ValidationResultIdentifier::orders"

        class Result:
            """Stands in for a validation result."""

            def to_json_dict(self) -> Any:
                """Describe itself the way GX does."""
                return {"success": False, "results": [{"success": False}]}

        class Checkpoint:
            """Stands in for a checkpoint result."""

            def __init__(self) -> None:
                self.run_results = {Identifier(): Result()}

        out = ceserial.dump({"kwargs": {"checkpoint_result": Checkpoint()}})
        results = out["kwargs"]["checkpoint_result"]["run_results"]
        self.assertEqual(
            results["ValidationResultIdentifier::orders"]["results"],
            [{"success": False}],
        )

    def test2(self) -> None:
        """
        Test that an enum member crosses as its value.
        """
        import enum

        class Format(enum.Enum):
            """Stands in for GX's ResultFormat."""

            SUMMARY = "SUMMARY"

        out = ceserial.dump({"result_format": Format.SUMMARY})
        self.assertEqual(out, {"result_format": "SUMMARY"})


# #############################################################################
# Test_dump_bulk_data1
# #############################################################################


class Test_dump_bulk_data1(unittest.TestCase):
    """
    Test that the customer's rows cannot reach the wire.
    """

    def test1(self) -> None:
        """
        Test that a frame is named by its shape, never opened.

        A frame answers to `to_dict`, which returns every row, and its
        `__str__` prints rows too. The Great Expectations 0.x action is
        handed a validator, and the frame it validated is four objects away
        through the data context.
        """

        class Frame:
            """Stands in for a pandas DataFrame."""

            shape = (4, 2)

            def to_dict(self) -> Any:
                """Return every row, the way pandas does."""
                return {"email": {"0": "alice@example.com"}}

            def __str__(self) -> str:
                return "0  alice@example.com\\n1  bob@example.com"

        class Engine:
            """Stands in for an execution engine holding the batch."""

            def __init__(self) -> None:
                self.batch_cache = {"orders": {"data": Frame()}}

        class Validator:
            """Stands in for a GX validator."""

            def __init__(self) -> None:
                self.interactive_evaluation = True
                self.execution_engine = Engine()

        out = ceserial.dump({"data_asset": Validator()})
        text = json.dumps(out)
        self.assertNotIn("alice@example.com", text)
        self.assertIn("Frame [4, 2]", text)

    def test2(self) -> None:
        """
        Test that a frame under a telling name is named too.

        A Spark or polars frame has no shape; the key it sits under says
        what it is either way.
        """

        class Opaque:
            """Stands in for a frame this walker cannot measure."""

            def to_dict(self) -> Any:
                """Return every row."""
                return {"email": ["carol@example.com"]}

        out = ceserial.dump({"dataframe": Opaque(), "df": Opaque()})
        text = json.dumps(out)
        self.assertNotIn("carol@example.com", text)
        self.assertEqual(out["dataframe"], "<Opaque>")
        self.assertEqual(out["df"], "<Opaque>")

    def test2b(self) -> None:
        """
        Test that a row count is still a number.

        Only a frame is a frame: a key that names one carrying a scalar is
        a count, and blocking it would lose what detection reads.
        """
        out = ceserial.dump({"rows": 42, "df": None, "dataframe": "orders"})
        self.assertEqual(out["rows"], 42)
        self.assertIsNone(out["df"])
        self.assertEqual(out["dataframe"], "orders")

    def test3(self) -> None:
        """
        Test that a frame-shaped object with a schema is caught by it.
        """

        class SparkFrame:
            """Stands in for a Spark DataFrame."""

            columns = ["email", "amount"]
            dtypes = [("email", "string"), ("amount", "int")]

            def to_dict(self) -> Any:
                """A Spark frame has no to_dict; a polars one does."""
                return {"email": ["dave@example.com"]}

        out = ceserial.dump({"batch": SparkFrame()})
        self.assertEqual(out["batch"], "<SparkFrame>")

    def test4(self) -> None:
        """
        Test that a zero-dimensional value is still a number.

        A numpy scalar is what a count or an observed value arrives as.
        """

        class Count:
            """Stands in for a numpy int64."""

            shape = ()

            def __init__(self, value: int) -> None:
                self.value = value

        out = ceserial.dump({"element_count": Count(4)})
        self.assertEqual(out["element_count"], {"value": 4})


# #############################################################################
# Test_dump_non_finite1
# #############################################################################


class Test_dump_non_finite1(unittest.TestCase):
    """
    Test that a number JSON cannot carry becomes null.
    """

    def test1(self) -> None:
        """
        Test that NaN and the infinities cross as null.

        Python writes them as bare literals, which a strict parser refuses,
        and the refusal loses every observation in the batch.
        """
        out = ceserial.dump(
            {
                "duration": float("nan"),
                "percent": float("inf"),
                "drift": float("-inf"),
                "rows": 4.5,
            }
        )
        self.assertIsNone(out["duration"])
        self.assertIsNone(out["percent"])
        self.assertIsNone(out["drift"])
        self.assertEqual(out["rows"], 4.5)
        self.assertNotIn("NaN", json.dumps(out))


# #############################################################################
# Test_dump_fields1
# #############################################################################


class Test_dump_fields1(unittest.TestCase):
    """
    Test that an object declaring its own field names is read by them.
    """

    def test1(self) -> None:
        """
        Test that a record keeping nothing in `__dict__` still crosses.

        Dagster's `@record` classes are this shape: `_fields` names them,
        `vars()` is empty, and `_asdict` raises "Iteration is not allowed",
        so a job snapshot arrived as its repr and the ops in it were lost.
        """

        class Record:
            """Stands in for a Dagster JobSnap."""

            _fields = ("name", "node_defs_snapshot")

            def __init__(self) -> None:
                # Nothing in __dict__: a record keeps its values elsewhere.
                pass

            def _asdict(self) -> Any:
                """Raise, the way a record does."""
                raise RuntimeError("Iteration is not allowed on `@record`")

            @property
            def name(self) -> str:
                """The job's name."""
                return "nightly"

            @property
            def node_defs_snapshot(self) -> Any:
                """The ops the job is made of."""
                return {"op_def_snaps": [{"name": "extract"}]}

        out = ceserial.dump({"job_snapshot": Record()})
        snapshot = out["job_snapshot"]
        self.assertEqual(snapshot["name"], "nightly")
        self.assertEqual(
            snapshot["node_defs_snapshot"]["op_def_snaps"][0]["name"],
            "extract",
        )

    def test2(self) -> None:
        """
        Test that a dump method raising does not abandon the rest.

        A decoy `to_dict` used to end the search, leaving the object as its
        repr even though a later method would have answered.
        """

        class Awkward:
            """An object whose first serialiser is broken."""

            def to_dict(self) -> Any:
                """Fail, the way a half-built tool object does."""
                raise RuntimeError("nope")

            def _asdict(self) -> Any:
                """Answer properly."""
                return {"run_id": "abc"}

        self.assertEqual(ceserial.dump(Awkward()), {"run_id": "abc"})

    def test3(self) -> None:
        """
        Test that a field that cannot be read is noted, not fatal.
        """

        class Record:
            """A record one of whose fields needs a live connection."""

            _fields = ("step_key", "materialization_events")

            @property
            def step_key(self) -> str:
                """The step this is about."""
                return "load"

            @property
            def materialization_events(self) -> Any:
                """Fail, the way a lazy field without storage does."""
                raise RuntimeError("no storage")

        out = ceserial.dump(Record())
        self.assertEqual(out["step_key"], "load")
        self.assertIn("unreadable", out["materialization_events"])


# #############################################################################
# Test_dump_summarise1
# #############################################################################


class Test_dump_summarise1(unittest.TestCase):
    """
    Test that a field a tool knows is duplication is named, not walked.
    """

    def test1(self) -> None:
        """
        Test that the named field arrives as its description.

        An Airflow task belongs to a task group, and a task group holds its
        own copy of the whole DAG, which the payload already carries twice
        over: 40% of a task event, every byte of it a repeat.
        """

        class Group:
            """Stands in for an Airflow task group."""

            def __init__(self, dag: Any) -> None:
                self.dag = dag
                self.children = {"extract": "...", "load": "..."}

            def __str__(self) -> str:
                return "<TaskGroup: nightly>"

        class Task:
            """Stands in for an operator."""

            def __init__(self) -> None:
                self.task_id = "load"
                self.dag = {"dag_id": "nightly", "task_dict": {"load": "..."}}
                self.task_group = Group(self.dag)

        out = ceserial.dump(
            {"task": Task()}, summarise=frozenset({"task_group"})
        )
        task = out["task"]
        self.assertEqual(task["task_id"], "load")
        self.assertEqual(task["dag"]["dag_id"], "nightly")
        self.assertEqual(task["task_group"], "<TaskGroup: nightly>")

    def test2(self) -> None:
        """
        Test that nothing is summarised unless a tool asks for it.
        """

        class Task:
            """Stands in for an operator."""

            def __init__(self) -> None:
                self.task_group = {"children": ["extract"]}

        out = ceserial.dump(Task())
        self.assertEqual(out["task_group"], {"children": ["extract"]})

    def test3(self) -> None:
        """
        Test that a scalar under a summarised name is left as it is.
        """
        out = ceserial.dump(
            {"task_group": "nightly", "other": 1},
            summarise=frozenset({"task_group"}),
        )
        self.assertEqual(out["task_group"], "nightly")

    def test4(self) -> None:
        """
        Test that a record's own field is summarised too.
        """

        class Record:
            """Stands in for a record that names its fields."""

            _fields = ("name", "bulky")

            @property
            def name(self) -> str:
                """What this is called."""
                return "nightly"

            @property
            def bulky(self) -> Any:
                """A copy of something the payload already has."""
                return {"a": 1, "b": 2}

        out = ceserial.dump(Record(), summarise=frozenset({"bulky"}))
        self.assertEqual(out["name"], "nightly")
        self.assertEqual(out["bulky"], "{'a': 1, 'b': 2}")

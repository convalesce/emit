"""
Tests for forwarding a Great Expectations validation result.

The version dispatch itself is exercised against real installs of both
majors; these cover the parts that hold whichever is present.

Run with `make test`.
"""

import json
import logging
import os
import unittest
import unittest.mock
from typing import Any, List

import convalesce_emit_gx._common as cegxcom
import convalesce_emit_gx.action as cegxact

_LOG = logging.getLogger(__name__)

_RESULT = {
    "success": False,
    "result": {
        "element_count": 100,
        "unexpected_count": 2,
        "partial_unexpected_list": ["alice@x.com", "bob@x.com"],
    },
}


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Any] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


# #############################################################################
# Test_gx_forward1
# #############################################################################


class Test_gx_forward1(unittest.TestCase):
    """
    Test that row values do not leave the process by default.
    """

    def test1(self) -> None:
        """
        Test that sample values are redacted before sending.

        This is the test that keeps the handbook honest: a GX result carries
        real failing rows, and we promise not to send them.
        """
        recorder = _Recorder()
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            outcome = cegxcom.forward(_RESULT, recorder)
        wire = json.dumps(recorder.sent, default=str)
        self.assertNotIn("alice@x.com", wire)
        self.assertIn("100", wire)
        self.assertTrue(outcome["convalesce_emitted"])
        self.assertTrue(outcome["redacted"])

    def test2(self) -> None:
        """
        Test that samples can be sent when that is chosen deliberately.
        """
        recorder = _Recorder()
        env = {"CONVALESCE_GX_SEND_SAMPLES": "true"}
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            outcome = cegxcom.forward(_RESULT, recorder)
        self.assertIn("alice@x.com", json.dumps(recorder.sent, default=str))
        self.assertFalse(outcome["redacted"])

    def test3(self) -> None:
        """
        Test that a checkpoint does not fail when we cannot send.
        """

        class _Broken:
            """Stands in for an unreachable endpoint."""

            def emit(self, **_kwargs: Any) -> None:
                """Fail the way a dead endpoint does."""
                raise RuntimeError("down")

            def flush(self) -> None:
                """Nothing is queued, so nothing to send."""

        outcome = cegxcom.forward(_RESULT, _Broken())
        self.assertFalse(outcome["convalesce_emitted"])


# #############################################################################
# Test_gx_dispatch1
# #############################################################################


class Test_gx_dispatch1(unittest.TestCase):
    """
    Test that the right action class is chosen for the installed GX.
    """

    def test1(self) -> None:
        """
        Test that no Great Expectations means no action class.

        Importing the plugin must not fail where GX is absent; it simply has
        nothing to offer.
        """
        self.assertIsNone(cegxact.gx_major())
        self.assertIsNone(cegxact.ConvalesceValidationAction)


# #############################################################################
# Test_gx_runtime1
# #############################################################################


class _Frame:
    """Stands in for the pandas frame a validator owns."""

    shape = (4, 2)

    def to_dict(self) -> Any:
        """Return every row, the way pandas does."""
        return {"email": {"0": "alice@x.com"}}


class _Engine:
    """Stands in for a GX execution engine holding the batch."""

    def __init__(self) -> None:
        self.batch_cache = {"orders": {"data": _Frame()}}


class _Validator:
    """Stands in for the 0.x `data_asset`: a live validator."""

    def __init__(self) -> None:
        self.interactive_evaluation = True
        self.execution_engine = _Engine()
        self.data_context = {"stores": {"expectations_store": {"class": "X"}}}


class Test_gx_runtime1(unittest.TestCase):
    """
    Test that the tool's runtime is not forwarded with the result.
    """

    def test1(self) -> None:
        """
        Test that the 0.x validator is left behind by name.

        It owns the data context, every store's configuration and the frame
        that was validated: most of the payload, none of it read, and the
        shortest path to the customer's rows.
        """
        recorder = _Recorder()
        cegxcom.forward(
            {
                "args": [],
                "kwargs": {
                    "validation_result_suite": {"results": [_RESULT]},
                    "data_asset": _Validator(),
                    "expectation_suite_identifier": "orders",
                },
            },
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("data_asset", payload["kwargs"])
        self.assertIn("validation_result_suite", payload["kwargs"])
        self.assertEqual(
            payload["kwargs"]["expectation_suite_identifier"], "orders"
        )
        self.assertNotIn("alice@x.com", json.dumps(payload))

    def test2(self) -> None:
        """
        Test that a runtime arriving positionally is left behind too.

        GX 0.x has changed which arguments it passes positionally across
        point releases, so the name alone is not enough.
        """
        recorder = _Recorder()
        cegxcom.forward(
            {"args": [{"results": [_RESULT]}, _Validator()], "kwargs": {}},
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(len(payload["args"]), 1)
        self.assertIn("results", payload["args"][0])
        self.assertNotIn("alice@x.com", json.dumps(payload))

    def test3(self) -> None:
        """
        Test that the 1.x checkpoint result is not mistaken for runtime.

        It is the whole point of the observation, and it names no engine or
        context of its own.
        """

        class CheckpointResult:
            """Stands in for a 1.x CheckpointResult."""

            def __init__(self) -> None:
                self.success = False
                self.run_results = {"orders": {"results": [_RESULT]}}

        recorder = _Recorder()
        cegxcom.forward(
            {"args": [], "kwargs": {"checkpoint_result": CheckpointResult()}},
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertIn("checkpoint_result", payload["kwargs"])
        self.assertFalse(payload["kwargs"]["checkpoint_result"]["success"])


# #############################################################################
# Test_runtime_platform1
# #############################################################################


class _Dialect:
    """Stands in for a SQLAlchemy dialect."""

    def __init__(self, name: str) -> None:
        self.name = name


class _Url:
    """Stands in for a SQLAlchemy URL, credentials and all."""

    def __init__(self, drivername: str, host: str, database: str) -> None:
        self.drivername = drivername
        self.username = "user"
        self.password = "s3cret"
        self.host = host
        self.database = database

    def __str__(self) -> str:
        return f"{self.drivername}://user:s3cret@{self.host}/{self.database}"


class _SqlAlchemyEngine:
    """Stands in for a live SQLAlchemy engine, credentials and all."""

    def __init__(self, dialect_name: str) -> None:
        self.dialect = _Dialect(dialect_name)
        self.url = _Url(dialect_name, "warehouse", "orders")


class _SqlExecutionEngine:
    """Stands in for GX's `SqlAlchemyExecutionEngine`."""

    def __init__(self, dialect_name: str) -> None:
        self.engine = _SqlAlchemyEngine(dialect_name)


class Test_runtime_platform1(unittest.TestCase):
    """
    Test that the execution engine's identity is read before it is dropped,
    and that only the dialect name crosses, never the engine's own URL.
    """

    def test1(self) -> None:
        """
        Test that a SQL execution engine's dialect names the platform.
        """

        class Validator:
            """Stands in for the 0.x `data_asset`."""

            def __init__(self) -> None:
                self.execution_engine = _SqlExecutionEngine("mysql")

        recorder = _Recorder()
        cegxcom.forward(
            {
                "args": [],
                "kwargs": {
                    "validation_result_suite": {"results": [_RESULT]},
                    "data_asset": Validator(),
                },
            },
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["dialect_name"], "mysql")
        self.assertEqual(payload["database"], "orders")
        self.assertEqual(
            payload["execution_engine_class"], "_SqlExecutionEngine"
        )
        self.assertNotIn("s3cret", json.dumps(payload))
        self.assertNotIn("warehouse", json.dumps(payload))

    def test2(self) -> None:
        """
        Test that a pandas execution engine, which owns no SQLAlchemy
        engine, still names its own class with no dialect at all.
        """
        recorder = _Recorder()
        cegxcom.forward(
            {
                "args": [],
                "kwargs": {
                    "validation_result_suite": {"results": [_RESULT]},
                    "data_asset": _Validator(),
                },
            },
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["execution_engine_class"], "_Engine")
        self.assertNotIn("dialect_name", payload)

    def test3(self) -> None:
        """
        Test that a payload with nothing runtime in it adds neither field.
        """
        recorder = _Recorder()
        cegxcom.forward(
            {"args": [], "kwargs": {"checkpoint_result": {"success": True}}},
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("execution_engine_class", payload)
        self.assertNotIn("dialect_name", payload)

    def test4(self) -> None:
        """
        Test that a 0.x engine names the datasource's platform and database
        under the datasource the result names, and nothing more of its URL.
        """

        class Validator:
            """Stands in for the 0.x `data_asset`."""

            def __init__(self) -> None:
                self.execution_engine = _SqlExecutionEngine("postgresql")

        suite = {
            "results": [_RESULT],
            "meta": {"active_batch_definition": {"datasource_name": "shop"}},
        }
        recorder = _Recorder()
        cegxcom.forward(
            {
                "args": [],
                "kwargs": {
                    "validation_result_suite": suite,
                    "data_asset": Validator(),
                },
            },
            recorder,
        )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(
            payload["datasources"],
            {"shop": {"type": "postgres", "database": "orders"}},
        )
        self.assertNotIn("s3cret", json.dumps(payload))

    def test5(self) -> None:
        """
        Test that BigQuery's project, the URL's host, is its database.
        """

        class Validator:
            """Stands in for the 0.x `data_asset`."""

            def __init__(self) -> None:
                self.execution_engine = _SqlExecutionEngine("bigquery")
                self.execution_engine.engine.url = _Url(
                    "bigquery", "my-project", "my_dataset"
                )

        recorder = _Recorder()
        cegxcom.forward(
            {"args": [], "kwargs": {"data_asset": Validator()}}, recorder
        )
        self.assertEqual(recorder.sent[0]["payload"]["database"], "my-project")


# #############################################################################
# Test_datasources_v1
# #############################################################################


class _Datasource:
    """Stands in for a GX 1.x fluent datasource."""

    def __init__(self, name: str, kind: str, url: Any = None) -> None:
        self.name = name
        self.type = kind
        self.connection_string = "postgresql://user:s3cret@db:5432/shop"
        self._url = url

    def get_engine(self) -> Any:
        """Return the engine the datasource validated with."""
        if self._url is None:
            raise RuntimeError("no engine")

        class Engine:
            """Stands in for a SQLAlchemy engine."""

            url = self._url

        return Engine()


class _Definition:
    """Stands in for a 1.x `ValidationDefinition`."""

    def __init__(self, source: Any) -> None:
        self._source = source

    @property
    def data_source(self) -> Any:
        """Reach the datasource, or fail as a detached definition does."""
        if self._source is None:
            raise ValueError("detached")
        return self._source


def _checkpoint_result(names: List[str], sources: List[Any]) -> Any:
    """
    Build a 1.x `CheckpointResult` whose validations ran on `names`.

    :param names: each validation's datasource name
    :param sources: the datasources the checkpoint's definitions hold
    :return: the result
    """

    class Checkpoint:
        """Stands in for the 1.x `Checkpoint` on the result."""

        validation_definitions = [_Definition(s) for s in sources]

    class Batch:
        """Stands in for a `LegacyBatchDefinition`."""

        def __init__(self, name: str) -> None:
            self.datasource_name = name

    class Result:
        """Stands in for a 1.x `CheckpointResult`."""

        checkpoint_config = Checkpoint()
        success = False
        run_results = {
            f"id-{i}": {
                "meta": {"active_batch_definition": Batch(name)},
                "results": [_RESULT],
            }
            for i, name in enumerate(names)
        }

    return Result()


class Test_datasources_v1(unittest.TestCase):
    """
    Test that each 1.x datasource is named by platform and database, and
    that its connection never crosses.
    """

    def test1(self) -> None:
        """
        Test that a postgres datasource names its type and database.
        """
        url = _Url("postgresql+psycopg2", "db", "shop")
        result = _checkpoint_result(
            ["shop", "shop"], [_Datasource("shop", "postgres", url)]
        )
        recorder = _Recorder()
        cegxcom.forward(
            {"args": [], "kwargs": {"checkpoint_result": result}},
            recorder,
            datasources=cegxcom.datasources_v1(result),
        )
        payload = recorder.sent[0]["payload"]
        self.assertEqual(
            payload["datasources"],
            {"shop": {"type": "postgres", "database": "shop"}},
        )
        wire = json.dumps(payload, default=str)
        self.assertNotIn("s3cret", wire)
        self.assertNotIn("connection_string", wire)

    def test2(self) -> None:
        """
        Test that each type is mapped to the platform datasets are named on.
        """
        cases = [
            ("snowflake", _Url("snowflake", "acct", "DB/PUBLIC"), "DB"),
            ("databricks_sql", _Url("databricks", "h", "main"), "main"),
            ("sql", _Url("mysql+pymysql", "h", "shop"), "shop"),
            ("sqlite", _Url("sqlite", "", "/tmp/x.db"), None),
        ]
        expected = ["snowflake", "databricks", "mysql", "sqlite"]
        for (kind, url, database), platform in zip(cases, expected):
            facts = cegxcom.datasource_facts(_Datasource("d", kind, url))
            assert facts is not None
            self.assertEqual(facts["type"], platform)
            self.assertEqual(facts["database"], database)

    def test3(self) -> None:
        """
        Test that a datasource with no SQL engine behind it is left out.
        """
        self.assertIsNone(
            cegxcom.datasource_facts(_Datasource("frames", "pandas"))
        )

    def test4(self) -> None:
        """
        Test that a datasource whose engine cannot be built still names its
        type, with no database.
        """
        facts = cegxcom.datasource_facts(_Datasource("wh", "redshift"))
        self.assertEqual(facts, {"type": "redshift", "database": None})

    def test5(self) -> None:
        """
        Test that a datasource the checkpoint does not hold is looked up on
        the data context, and a missing one is skipped.
        """
        url = _Url("postgresql", "db", "ops")
        result = _checkpoint_result(["ops", "gone"], [None])
        by_name = {"ops": _Datasource("ops", "postgres", url)}
        with unittest.mock.patch.object(
            cegxcom, "project_datasource", side_effect=by_name.get
        ):
            out = cegxcom.datasources_v1(result)
        self.assertEqual(out, {"ops": {"type": "postgres", "database": "ops"}})

    def test6(self) -> None:
        """
        Test that a result this cannot read yields nothing and raises
        nothing: a checkpoint must not fail over it.
        """

        class Broken:
            """A result whose run results cannot be read."""

            @property
            def run_results(self) -> Any:
                """Fail the way an unexpected GX release might."""
                raise RuntimeError("renamed")

        self.assertEqual(cegxcom.datasources_v1(Broken()), {})
        self.assertEqual(cegxcom.datasources_v1(None), {})

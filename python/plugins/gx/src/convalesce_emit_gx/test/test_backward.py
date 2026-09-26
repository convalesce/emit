"""
Tests that whatever the installed Great Expectations produces is forwarded,
or left out for a stated reason.

Collect's payload coverage test is the forward check: does a receiver read
only what emit's contract promises? This is the backward one. It asks the
installed GX what it has -- the fields of its result classes, the keys a
real validation writes under every result format, its datasource, asset and
batch spec types -- and fails on anything the contract does not carry and
`backward_exclusions.json` does not account for. A GX release that adds a
result key or a datasource type therefore fails here until somebody decides
what it means for a receiver.

Needs GX installed, so it skips under `make test`. Run it against each
supported release:

    pip install great-expectations==1.23.2 sqlalchemy pandas
    pytest plugins/gx/src/convalesce_emit_gx/test/test_backward.py

Import as:

import convalesce_emit_gx.test.test_backward as cegxtbac
"""

import fnmatch
import importlib.util
import inspect
import json
import logging
import os
import pathlib
import re
import sqlite3
import tempfile
import unittest
import unittest.mock
from typing import Any, Dict, List, Optional, Set

import convalesce_emit as cemit
import convalesce_emit_gx._common as cegxcom

_LOG = logging.getLogger(__name__)

_HAS_GX = importlib.util.find_spec("great_expectations") is not None

_HERE = pathlib.Path(__file__).resolve().parent
_EXCLUSIONS: Dict[str, Any] = json.loads(
    (_HERE / "backward_exclusions.json").read_text()
)

_RESULT_FORMATS = ("BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE")

# Mappings keyed by data rather than schema, collapsed as collect's coverage
# test collapses them, so a path means the same thing in every capture.
_KEYED_BY_DATA = frozenset(
    {
        "run_results",
        "batch_identifiers",
        "batch_parameters",
        "datasources",
        "result_urls",
    }
)
_GX_KEY = re.compile(
    r"ValidationResultIdentifier::[^\s\[]*?/\d{8}T\d{6}(?:\.\d+)?Z/[^.\[]+"
    r"|MetricConfigurationID\((?:[^()]|\([^()]*\))*\)"
)


def _normalise(path: str) -> str:
    """
    Collapse the data-dependent parts of a field path.

    :param path: a field path
    :return: the path, comparable across captures and versions
    """
    out: List[str] = []
    collapse = False
    for segment in _GX_KEY.sub("{key}", path).split("."):
        out.append("{key}" if collapse else segment)
        collapse = not collapse and segment.rstrip("[]") in _KEYED_BY_DATA
    return ".".join(out)


def _field_paths(value: Any, prefix: str = "") -> Set[str]:
    """
    Every field path a payload carries.

    :param value: the payload, or part of one
    :param prefix: the path of this part
    :return: the paths
    """
    out: Set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in ("$id", "$ref"):
                continue
            path = f"{prefix}.{key}" if prefix else str(key)
            out.add(path)
            out |= _field_paths(item, path)
    elif isinstance(value, list):
        for item in value:
            out |= _field_paths(item, f"{prefix}[]")
    return out


def _resolve_refs(value: Any) -> Any:
    """
    Replace envelope version 2's `$ref` pointers with what they point at.

    :param value: a payload
    :return: the payload, pointers resolved
    """
    targets: Dict[Any, Any] = {}

    def collect(node: Any) -> None:
        if isinstance(node, dict):
            if "$id" in node:
                targets[node["$id"]] = node
            for item in node.values():
                collect(item)
        elif isinstance(node, list):
            for item in node:
                collect(item)

    def resolve(node: Any, depth: int = 0) -> Any:
        if depth > 50:
            return node
        if isinstance(node, dict):
            if set(node) == {"$ref"} and node["$ref"] in targets:
                return resolve(targets[node["$ref"]], depth + 1)
            return {k: resolve(v, depth + 1) for k, v in node.items()}
        if isinstance(node, list):
            return [resolve(item, depth + 1) for item in node]
        return node

    collect(value)
    return resolve(value)


def _contract() -> Set[str]:
    """
    The field paths emit's published contract promises, normalised.

    :return: the paths
    """
    text = (_HERE.parent / "contract.json").read_text()
    paths = json.loads(text)["events"]["validation_result"]
    return {_normalise(path) for path in paths}


def _excluded(path: str) -> Optional[str]:
    """
    The reason a field is deliberately not in the contract.

    :param path: a normalised field path
    :return: the reason, or None when nothing accounts for it
    """
    for entry in _EXCLUSIONS["fields"]:
        if fnmatch.fnmatchcase(path, entry["path"]):
            return str(entry["because"])
    return None


def _jsonable(value: Any) -> Any:
    """
    A GX object as the plain JSON its own serialiser writes.

    :param value: a GX result or configuration
    :return: dicts, lists and scalars
    """
    to_json = getattr(value, "to_json_dict", None)
    if callable(to_json):
        value = to_json()
    return json.loads(json.dumps(value, default=str))


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder:
    """Stands in for an emitter built from the environment."""

    sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        _Recorder.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


# #############################################################################
# _Run
# #############################################################################


class _Run:
    """
    Real checkpoints on the installed GX, over SQLite and a CSV file, once
    per result format: what GX produced, and what the action sent.
    """

    def __init__(self) -> None:
        import great_expectations as gx  # pylint: disable=import-outside-toplevel

        self.gx = gx
        self.major = int(str(gx.__version__).split(".", 1)[0])
        # What GX itself produced, under the prefix the action sends it at.
        self.produced: Dict[str, Set[str]] = {}
        self.sent: Set[str] = set()
        self.payloads: List[Any] = []
        self.checkpoint_results: List[Any] = []
        # Observations a validation run outside a checkpoint fired by itself.
        self.fired_outside: int = -1
        root = tempfile.mkdtemp()
        self._write_data(root)
        _Recorder.sent = []
        env = {"GX_ANALYTICS_ENABLED": "false"}
        with (
            unittest.mock.patch.object(cemit, "Emitter", _Recorder),
            unittest.mock.patch.dict(os.environ, env),
        ):
            if self.major >= 1:
                self._run_v1(root)
            else:
                self._run_v0(root)
        for row in _Recorder.sent:
            payload = _resolve_refs(row["payload"])
            self.payloads.append(payload)
            self.sent |= {_normalise(p) for p in _field_paths(payload)}

    @staticmethod
    def _write_data(root: str) -> None:
        con = sqlite3.connect(os.path.join(root, "shop.db"))
        con.execute(
            "create table orders(id int, email text, amount real, status text)"
        )
        con.executemany(
            "insert into orders values (?, ?, ?, ?)",
            [
                (1, "a@x.com", 10, "new"),
                (2, None, 20, "paid"),
                (3, "c@x.com", -5, "odd"),
                (3, "d@x.com", None, "paid"),
            ],
        )
        con.commit()
        con.close()
        with open(os.path.join(root, "cities.csv"), "w") as handle:
            handle.write("id,city\n1,Pune\n2,Oslo\n3,Lima\n")

    @staticmethod
    def _result_format(name: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {"result_format": name}
        if name == "COMPLETE":
            out["unexpected_index_column_names"] = ["id"]
            out["return_unexpected_index_query"] = True
            out["include_unexpected_rows"] = True
        return out

    def _run_v1(self, root: str) -> None:
        gx = self.gx
        # pylint: disable=import-outside-toplevel
        from convalesce_emit_gx.action import ConvalesceValidationAction

        ctx = gx.get_context(mode="file", project_root_dir=root)
        lite = ctx.data_sources.add_sqlite(
            "lite", connection_string=f"sqlite:///{root}/shop.db"
        )
        files = ctx.data_sources.add_pandas_filesystem(
            "files", base_directory=root
        )
        expectations = gx.expectations
        between: Any = expectations.ExpectColumnValuesToBeBetween

        class ExpectColumnValuesToBeNonNegative(between):  # type: ignore[misc,valid-type]
            """A customer's own expectation, built on a stock one."""

            min_value: float = 0

        table_suite = ctx.suites.add(gx.ExpectationSuite(name="orders"))
        for expectation in _v1_battery(expectations):
            table_suite.add_expectation(expectation)
        table_suite.add_expectation(
            ExpectColumnValuesToBeNonNegative(column="amount")
        )
        file_suite = ctx.suites.add(gx.ExpectationSuite(name="cities"))
        file_suite.add_expectation(
            expectations.ExpectColumnDistinctValuesToEqualSet(
                column="city", value_set=["Pune"]
            )
        )
        definitions = [
            gx.ValidationDefinition(
                name="orders",
                data=lite.add_table_asset(
                    "orders", table_name="orders"
                ).add_batch_definition_whole_table("all"),
                suite=table_suite,
            ),
            gx.ValidationDefinition(
                name="paid",
                data=lite.add_query_asset(
                    "paid", query="select * from orders where status = 'paid'"
                ).add_batch_definition_whole_table("all"),
                suite=table_suite,
            ),
            gx.ValidationDefinition(
                name="cities",
                data=files.add_csv_asset("cities").add_batch_definition_path(
                    "file", path="cities.csv"
                ),
                suite=file_suite,
            ),
        ]
        definitions = [ctx.validation_definitions.add(d) for d in definitions]
        for name in _RESULT_FORMATS:
            checkpoint = ctx.checkpoints.add(
                gx.Checkpoint(
                    name=f"cp_{name.lower()}",
                    validation_definitions=definitions,
                    result_format=self._result_format(name),
                    actions=[
                        gx.checkpoint.UpdateDataDocsAction(name="docs"),
                        ConvalesceValidationAction(),
                    ],
                )
            )
            result = checkpoint.run()
            self.checkpoint_results.append(result)
            self._produced_v1(result)
        before = len(_Recorder.sent)
        lone = definitions[0].run(result_format=self._result_format("SUMMARY"))
        self._forward_lone(lone, before)

    def _produced_v1(self, result: Any) -> None:
        prefix = "kwargs.checkpoint_result"
        add = self._add
        add("CheckpointResult", {f"{prefix}.{f}" for f in _model_fields(result)})
        add(
            "Checkpoint",
            {
                f"{prefix}.checkpoint_config.{f}"
                for f in _model_fields(result.checkpoint_config)
            },
        )
        add(
            "RunIdentifier",
            _init_params(type(result.run_id), f"{prefix}.run_id"),
        )
        described = _jsonable(result.describe_dict())
        add("CheckpointDescribeResult", _described(described))
        for key, suite in result.run_results.items():
            base = _normalise(f"{prefix}.run_results.{key}")
            add(
                "ExpectationSuiteValidationResult",
                _init_params(type(suite), base),
            )
            self._produced_suite(suite, base)
        # pylint: disable=import-outside-toplevel
        from great_expectations.checkpoint.actions import (
            ActionContext,
            ValidationAction,
        )

        add("ValidationAction.run", _init_params(ValidationAction.run, "kwargs"))
        add(
            "ActionContext",
            {
                f"kwargs.action_context.{name}"
                for name, value in vars(ActionContext).items()
                if isinstance(value, property)
            },
        )

    def _run_v0(self, root: str) -> None:
        gx = self.gx
        ctx = gx.get_context(project_root_dir=root)
        lite = ctx.sources.add_sqlite(
            "lite", connection_string=f"sqlite:///{root}/shop.db"
        )
        orders = lite.add_table_asset("orders", table_name="orders")
        paid = lite.add_query_asset(
            "paid", query="select * from orders where status = 'paid'"
        )
        files = ctx.sources.add_pandas_filesystem("files", base_directory=root)
        cities = files.add_csv_asset("cities", batching_regex=r"cities\.csv")
        ctx.add_or_update_expectation_suite("orders")
        validator = ctx.get_validator(
            batch_request=orders.build_batch_request(),
            expectation_suite_name="orders",
        )
        _v0_battery(validator)
        validator.save_expectation_suite(discard_failed_expectations=False)
        ctx.add_or_update_expectation_suite("cities")
        validator = ctx.get_validator(
            batch_request=cities.build_batch_request(),
            expectation_suite_name="cities",
        )
        validator.expect_column_distinct_values_to_equal_set("city", ["Pune"])
        validator.save_expectation_suite(discard_failed_expectations=False)
        validations = [
            {
                "batch_request": orders.build_batch_request(),
                "expectation_suite_name": "orders",
            },
            {
                "batch_request": paid.build_batch_request(),
                "expectation_suite_name": "orders",
            },
            {
                "batch_request": cities.build_batch_request(),
                "expectation_suite_name": "cities",
            },
        ]
        for name in _RESULT_FORMATS:
            checkpoint = ctx.add_or_update_checkpoint(
                name=f"cp_{name.lower()}",
                validations=validations,
                runtime_configuration={
                    "result_format": self._result_format(name)
                },
                action_list=[
                    {
                        "name": "docs",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    },
                    {
                        "name": "convalesce",
                        "action": {
                            "module_name": "convalesce_emit_gx.action",
                            "class_name": "ConvalesceValidationAction",
                        },
                    },
                ],
            )
            result = checkpoint.run()
            self.checkpoint_results.append(result)
            for run in result.run_results.values():
                suite = run["validation_result"]
                base = "kwargs.validation_result_suite"
                self._add(
                    "ExpectationSuiteValidationResult",
                    _init_params(type(suite), base),
                )
                self._produced_suite(suite, base)
        before = len(_Recorder.sent)
        validator = ctx.get_validator(
            batch_request=orders.build_batch_request(),
            expectation_suite_name="orders",
        )
        self._forward_lone(validator.validate(), before, validator)
        # pylint: disable=import-outside-toplevel
        from great_expectations.checkpoint.actions import ValidationAction

        # The 0.x base's `_run` is what hands our action its arguments.
        run = getattr(ValidationAction, "_run")
        self._add("ValidationAction._run", _init_params(run, "kwargs"))

    def _forward_lone(
        self, result: Any, before: int, validator: Any = None
    ) -> None:
        """
        Send a result validated outside a checkpoint, which fires no action.
        """
        self.fired_outside = len(_Recorder.sent) - before
        cegxcom.forward_validation_result(result, validator=validator)
        base = "kwargs.validation_result_suite"
        self._add(
            "ExpectationSuiteValidationResult", _init_params(type(result), base)
        )
        self._produced_suite(result, base)

    def _produced_suite(self, suite: Any, base: str) -> None:
        """
        What one validation result carries, a level into each part whose
        keys are GX's rather than the customer's.
        """
        add = self._add
        data = _jsonable(suite)
        add("ExpectationSuiteValidationResult", {f"{base}.{k}" for k in data})
        meta = data.get("meta") or {}
        add("validation meta", {f"{base}.meta.{k}" for k in meta})
        for part in ("batch_spec", "batch_markers", "active_batch_definition"):
            if isinstance(meta.get(part), dict):
                add(part, {f"{base}.meta.{part}.{k}" for k in meta[part]})
        add(
            "statistics",
            {f"{base}.statistics.{k}" for k in data.get("statistics") or {}},
        )
        for raw, item in zip(suite.results, data.get("results") or []):
            rbase = f"{base}.results[]"
            add("ExpectationValidationResult", _init_params(type(raw), rbase))
            add("ExpectationValidationResult", {f"{rbase}.{k}" for k in item})
            config = raw.expectation_config
            add(
                "ExpectationConfiguration",
                {
                    f"{rbase}.expectation_config.{f}"
                    for f in _schema_fields(config)
                },
            )
            add(
                "ExpectationConfiguration",
                {
                    f"{rbase}.expectation_config.{k}"
                    for k in item.get("expectation_config") or {}
                },
            )
            result = item.get("result") or {}
            add("result", {f"{rbase}.result.{k}" for k in result})
            if isinstance(result.get("details"), dict):
                add(
                    "result details",
                    {f"{rbase}.result.details.{k}" for k in result["details"]},
                )
            info = item.get("exception_info") or {}
            if "raised_exception" in info:
                add(
                    "exception_info",
                    {f"{rbase}.exception_info.{k}" for k in info},
                )

    def _add(self, origin: str, paths: Set[str]) -> None:
        for path in paths:
            self.produced.setdefault(_normalise(path), set()).add(origin)


def _v1_battery(expectations: Any) -> List[Any]:
    """
    Stock expectations covering each shape of result GX 1.x writes.

    :param expectations: `great_expectations.expectations`
    :return: the expectations
    """
    e = expectations
    return [
        e.ExpectColumnValuesToNotBeNull(
            column="email", notes="n", meta={"o": 1}, description="d"
        ),
        e.ExpectColumnValuesToBeBetween(
            column="amount", min_value=0, max_value=100, severity="warning"
        ),
        e.ExpectColumnValuesToBeInSet(
            column="status", value_set=["new", "paid"]
        ),
        e.ExpectColumnValuesToMatchRegex(column="email", regex=r".*@x\.com"),
        e.ExpectColumnValuesToBeUnique(column="id"),
        e.ExpectColumnDistinctValuesToBeInSet(
            column="status", value_set=["new", "paid"]
        ),
        e.ExpectColumnMostCommonValueToBeInSet(
            column="status", value_set=["new"]
        ),
        e.ExpectColumnUniqueValueCountToBeBetween(
            column="status", min_value=1, max_value=2
        ),
        e.ExpectColumnMeanToBeBetween(column="amount", min_value=0, max_value=5),
        e.ExpectColumnMaxToBeBetween(column="amount", min_value=0, max_value=5),
        e.ExpectTableRowCountToBeBetween(min_value=1, max_value=10),
        e.ExpectTableColumnsToMatchSet(column_set=["id", "email"]),
        e.ExpectColumnToExist(column="missing"),
    ]


def _v0_battery(validator: Any) -> None:
    """
    Stock expectations covering each shape of result GX 0.x writes.

    :param validator: a 0.x validator on the orders table
    :return: nothing
    """
    v = validator
    v.expect_column_values_to_not_be_null("email", meta={"o": 1})
    v.expect_column_values_to_be_between("amount", min_value=0, max_value=100)
    v.expect_column_values_to_be_in_set("status", ["new", "paid"])
    v.expect_column_values_to_match_regex("email", r".*@x\.com")
    v.expect_column_values_to_be_unique("id")
    v.expect_column_distinct_values_to_be_in_set("status", ["new", "paid"])
    v.expect_column_most_common_value_to_be_in_set("status", ["new"])
    v.expect_column_unique_value_count_to_be_between("status", 1, 2)
    v.expect_column_mean_to_be_between("amount", 0, 5)
    v.expect_column_max_to_be_between("amount", 0, 5)
    v.expect_table_row_count_to_be_between(1, 10)
    v.expect_table_columns_to_match_set(["id", "email"])
    v.expect_column_to_exist("missing")


def _described(described: Dict[str, Any]) -> Set[str]:
    """
    The fields of a 1.x checkpoint's `describe_dict()`, a level deep.

    :param described: the description
    :return: its paths, under `checkpoint_describe`
    """
    out = {f"checkpoint_describe.{k}" for k in described}
    out |= {
        f"checkpoint_describe.statistics.{k}"
        for k in described.get("statistics") or {}
    }
    for item in described.get("validation_results") or []:
        out |= {f"checkpoint_describe.validation_results[].{k}" for k in item}
    return out


def _model_fields(model: Any) -> List[str]:
    """
    A pydantic model's declared fields, on pydantic 1 or 2.

    :param model: a model instance
    :return: the field names
    """
    fields = getattr(type(model), "model_fields", None) or getattr(
        type(model), "__fields__", {}
    )
    return list(fields)


def _init_params(target: Any, prefix: str) -> Set[str]:
    """
    The parameters a class's constructor, or a function, takes.

    :param target: a class or a function
    :param prefix: the path they sit under in the payload
    :return: one path per parameter
    """
    function = target.__init__ if inspect.isclass(target) else target
    params = inspect.signature(function).parameters.values()
    return {
        f"{prefix}.{p.name}"
        for p in params
        if p.name != "self" and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    }


def _schema_fields(config: Any) -> List[str]:
    """
    Every field an `ExpectationConfiguration` can serialise.

    :param config: an expectation configuration
    :return: the field names its marshmallow schema declares
    """
    module = importlib.import_module(type(config).__module__)
    schema = getattr(module, "ExpectationConfigurationSchema", None)
    if schema is None:
        return []
    return list(schema().fields)


# #############################################################################
# Test_backward_fields1
# #############################################################################


_RUN: Optional[_Run] = None


def _run() -> _Run:
    """
    Run the checkpoints once for every test that needs them.

    :return: the run
    """
    global _RUN
    if _RUN is None:
        _RUN = _Run()
    return _RUN


@unittest.skipUnless(_HAS_GX, "needs Great Expectations installed")
class Test_backward_fields1(unittest.TestCase):
    """
    Test that every field the installed GX hands the action is in emit's
    contract, or excluded with a reason.
    """

    def test1(self) -> None:
        """
        Test that nothing GX produces is unaccounted for.
        """
        contract = _contract()
        missing = sorted(
            f"{path}  (from {', '.join(sorted(origins))})"
            for path, origins in _run().produced.items()
            if path not in contract and _excluded(path) is None
        )
        self.assertFalse(
            missing,
            "Great Expectations produces fields emit's contract does not carry "
            "and backward_exclusions.json does not explain; forward them and "
            "regenerate the contract, or exclude them with a reason:\n"
            + "\n".join(missing),
        )

    def test2(self) -> None:
        """
        Test that what the contract carries was actually sent by this GX.

        The contract is the union across versions; this catches the action
        dropping, on this version, a field this version produced.
        """
        run = _run()
        dropped = sorted(
            path
            for path in run.produced
            if _excluded(path) is None and path not in run.sent
        )
        self.assertFalse(dropped, f"produced but not sent: {dropped}")

    def test3(self) -> None:
        """
        Test that every checkpoint run was forwarded, one observation each.
        """
        run = _run()
        expected = (
            len(_RESULT_FORMATS)
            if run.major >= 1
            else sum(len(r.run_results) for r in run.checkpoint_results)
        )
        # And the one validated outside a checkpoint, sent by hand.
        self.assertEqual(len(run.payloads), expected + 1)

    def test5(self) -> None:
        """
        Test that a validation outside a checkpoint fires no action, so it
        has to be sent with `forward_validation_result`, which does.

        Should GX start running actions there, this fails and the README's
        advice to call it by hand can go.
        """
        run = _run()
        self.assertEqual(run.fired_outside, 0)
        lone = run.payloads[-1]
        self.assertIn("validation_result_suite", lone["kwargs"])
        # Named by platform, not by the datasource's name.
        platform = (
            lone.get("dialect_name") or lone["datasources"]["lite"]["type"]
        )
        self.assertEqual(platform, "sqlite")

    def test6(self) -> None:
        """
        Test that a GX Cloud checkpoint's result pages cross, keyed as the
        results themselves are.

        GX Cloud sets `result_url` on each result it stores, and GX's own
        serialiser drops it. Set here on a real 1.x result, as the Cloud
        store would, since a Cloud run needs an account.
        """
        run = _run()
        if run.major < 1:
            self.skipTest("0.x has no result_url")
        # pylint: disable=import-outside-toplevel
        from great_expectations.checkpoint.actions import ActionContext

        from convalesce_emit_gx.action import ConvalesceValidationAction

        result = run.checkpoint_results[-1]
        for suite in result.run_results.values():
            suite.result_url = "https://app.greatexpectations.io/r/1"
        _Recorder.sent = []
        action = ConvalesceValidationAction()
        action.set_emitter(_Recorder())
        action.run(checkpoint_result=result, action_context=ActionContext())
        payload = _Recorder.sent[0]["payload"]
        self.assertEqual(
            set(payload["result_urls"]),
            set(payload["kwargs"]["checkpoint_result"]["run_results"]),
        )

    def test4(self) -> None:
        """
        Test that every exclusion has a reason.
        """
        for entry in _EXCLUSIONS["fields"]:
            self.assertTrue(entry.get("because"), entry["path"])


# #############################################################################
# Test_backward_types1
# #############################################################################


class _Datasource:
    """A datasource of a given type, with no engine and no connection."""

    def __init__(self, kind: str) -> None:
        self.type = kind


@unittest.skipUnless(_HAS_GX, "needs Great Expectations installed")
class Test_backward_types1(unittest.TestCase):
    """
    Test that every datasource, asset and batch spec type the installed GX
    has is one a receiver knows how to name a dataset from.
    """

    def _unclassified(self, section: str, names: Set[str]) -> List[str]:
        known = _EXCLUSIONS[section]
        for name, entry in known.items():
            self.assertTrue(entry.get("because"), f"{section}.{name}")
        return sorted(names - set(known))

    def test1(self) -> None:
        """
        Test that every datasource type is classified, and that the plugin
        treats each the way its classification says.
        """
        names = _datasource_types()
        self.assertFalse(
            self._unclassified("datasource_types", names),
            "classify these in backward_exclusions.json",
        )
        for name in names:
            named_by = _EXCLUSIONS["datasource_types"][name]["named_by"]
            facts = cegxcom.datasource_facts(_Datasource(name))
            if named_by == "path":
                self.assertEqual(facts, {"type": name, "database": None}, name)
            elif named_by == "none":
                self.assertIsNone(facts, name)
            else:
                self.assertEqual(named_by, "table", name)

    def test2(self) -> None:
        """
        Test that every data asset type is classified.
        """
        self.assertFalse(
            self._unclassified("asset_types", _asset_types()),
            "classify these in backward_exclusions.json",
        )

    def test3(self) -> None:
        """
        Test that every batch spec and execution engine class is classified.
        """
        # pylint: disable=import-outside-toplevel
        import great_expectations.core.batch_spec as gxspec
        import great_expectations.execution_engine as gxengine

        specs = {
            name
            for name, cls in inspect.getmembers(gxspec, inspect.isclass)
            if issubclass(cls, gxspec.BatchSpec)
        }
        engines = {
            name
            for name, cls in inspect.getmembers(gxengine, inspect.isclass)
            if issubclass(cls, gxengine.ExecutionEngine)
        }
        self.assertFalse(self._unclassified("batch_specs", specs))
        self.assertFalse(self._unclassified("execution_engines", engines))


def _type_lookup() -> Any:
    """
    The registry of fluent datasource types, on either major.

    :return: the registry
    """
    # pylint: disable=import-outside-toplevel
    import great_expectations.datasource.fluent.sources as gxsources

    factory = getattr(gxsources, "DataSourceManager", None) or getattr(
        gxsources, "_SourceFactories"
    )
    return factory.type_lookup


def _datasource_types() -> Set[str]:
    """
    Every fluent datasource type the installed GX registers.

    :return: the type names
    """
    return set(_type_lookup().type_names())


def _asset_types() -> Set[str]:
    """
    Every data asset type the installed GX's datasources can hold.

    :return: the type names
    """
    lookup = _type_lookup()
    out: Set[str] = set()
    for name in lookup.type_names():
        for asset in getattr(lookup[name], "asset_types", []):
            fields = getattr(asset, "model_fields", None) or asset.__fields__
            out.add(str(fields["type"].default))
    return out

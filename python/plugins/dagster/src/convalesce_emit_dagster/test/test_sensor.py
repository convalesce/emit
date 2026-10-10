"""
Tests for the run-status sensor body.
"""

import os
import sys
import types
import unittest
import unittest.mock
from typing import Any, Dict, List, Optional

import convalesce_emit_dagster.sensor as cedsens


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Nothing is queued, so nothing to send."""


class _Context:
    """The shape of a run-status context: private state, public properties."""

    def __init__(
        self,
        run: Optional[Dict[str, Any]] = None,
        event: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._run = run or {"job_name": "nightly", "run_id": "abc"}
        self._event = event or {"event_type_value": "PIPELINE_FAILURE"}

    @property
    def dagster_run(self) -> Dict[str, Any]:
        """The run, as Dagster exposes it."""
        return self._run

    @property
    def dagster_event(self) -> Dict[str, Any]:
        """The event, as Dagster exposes it."""
        return self._event

    @property
    def sensor_name(self) -> str:
        """The sensor that fired."""
        return "convalesce_on_failure"

    @property
    def partition_key(self) -> str:
        """Raises, as Dagster's does for an unpartitioned run."""
        raise RuntimeError("not partitioned")


# #############################################################################
# Test_convalesce_sensor1
# #############################################################################


class Test_convalesce_sensor1(unittest.TestCase):
    """
    Test that a run-status context crosses as its run and event.
    """

    def test1(self) -> None:
        """
        Test that the run and event are forwarded, not the wrapper's repr.

        The context keeps its state private, so dumped whole it is a bare
        object description with no job name in it.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor(_Context(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["dagster_run"]["job_name"], "nightly")
        self.assertEqual(
            payload["dagster_event"]["event_type_value"], "PIPELINE_FAILURE"
        )
        self.assertEqual(payload["sensor_name"], "convalesce_on_failure")
        self.assertNotIn("context", payload)

    def test_own_retry_job_is_not_reported(self) -> None:
        """
        Test that a run of the job that asks which retries are approved is
        left unreported: it runs every minute and is not the customer's.
        """

        class _RetryRun(_Context):
            @property
            def dagster_run(self) -> Dict[str, Any]:
                return {"job_name": "convalesce_retries", "run_id": "r1"}

        recorder = _Recorder()
        cedsens.convalesce_sensor(_RetryRun(), emitter=recorder)
        self.assertEqual(recorder.sent, [])

    def test2(self) -> None:
        """
        Test that something that is not a context still crosses whole.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor({"job_name": "nightly"}, emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["context"]["job_name"], "nightly")

    def test3(self) -> None:
        """
        Test that a credential in the run's config or in a step's error does
        not cross, and that the redaction is declared.
        """
        context = _Context(
            run={
                "job_name": "nightly",
                "run_id": "abc",
                "run_config": {
                    "resources": {"db": {"config": {"password": "hunter2"}}}
                },
            },
            event={
                "event_type_value": "PIPELINE_FAILURE",
                "message": "could not reach postgresql://etl:hunter2@db/shop",
            },
        )
        recorder = _Recorder()
        cedsens.convalesce_sensor(context, emitter=recorder)
        sent = recorder.sent[0]
        self.assertNotIn("hunter2", repr(sent["payload"]))
        self.assertEqual(
            sent["payload"]["dagster_event"]["message"],
            "could not reach postgresql://etl:***@db/shop",
        )
        self.assertEqual(
            {entry["path"] for entry in sent["excluded"]},
            {
                "dagster_run.run_config.resources.db.config.password",
                "dagster_event.message",
            },
        )


class _Instance:
    """Stands in for the DagsterInstance the context carries."""

    def __init__(self) -> None:
        self.asked: List[str] = []

    def get_job_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """The job's ops and their inputs and outputs."""
        self.asked.append(snapshot_id)
        return {"name": "nightly", "node_defs": [{"name": "extract"}]}

    def get_execution_plan_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """Which step depends on which."""
        self.asked.append(snapshot_id)
        return {"steps": [{"key": "load", "inputs": ["extract"]}]}

    def get_run_stats(self, run_id: str) -> Dict[str, Any]:
        """When the run began and ended."""
        self.asked.append(run_id)
        return {"run_id": run_id, "start_time": 1.0, "end_time": 2.0}

    def get_run_step_stats(self, run_id: str) -> List[Dict[str, Any]]:
        """When each step began and ended, and how it went."""
        self.asked.append(run_id)
        return [{"step_key": "extract", "status": "SUCCESS"}]


class _Run:
    """Stands in for a DagsterRun, which names what it points at."""

    def __init__(self) -> None:
        self.job_name = "nightly"
        self.run_id = "abc"
        self.job_snapshot_id = "snap-1"
        self.execution_plan_snapshot_id = "plan-1"


class _ReachableContext:
    """A run-status context with an instance behind it."""

    def __init__(self, instance: Any = None) -> None:
        self.instance = _Instance() if instance is None else instance

    @property
    def dagster_run(self) -> Any:
        """The run, as Dagster exposes it."""
        return _Run()

    @property
    def dagster_event(self) -> Dict[str, str]:
        """The event, as Dagster exposes it."""
        return {"event_type_value": "PIPELINE_SUCCESS"}

    @property
    def sensor_name(self) -> str:
        """The sensor that fired."""
        return "convalesce_on_success"


# #############################################################################
# Test_reach_instance1
# #############################################################################


class Test_reach_instance1(unittest.TestCase):
    """
    Test that what the run points at is read and forwarded.
    """

    def test1(self) -> None:
        """
        Test that the snapshots and the stats arrive with the run.

        The run and the event say a job ran and how it ended. What it is
        made of, when each step ran and which step feeds which is on the
        instance, one call away, and was left behind.
        """
        recorder = _Recorder()
        context = _ReachableContext()
        cedsens.convalesce_sensor(context, emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["job_snapshot"]["name"], "nightly")
        self.assertEqual(
            payload["execution_plan_snapshot"]["steps"][0]["key"], "load"
        )
        self.assertEqual(payload["run_stats"]["end_time"], 2.0)
        self.assertEqual(payload["step_stats"][0]["step_key"], "extract")
        # Each was asked for by the id the run named, not by guesswork.
        self.assertEqual(
            context.instance.asked, ["snap-1", "plan-1", "abc", "abc"]
        )

    def test2(self) -> None:
        """
        Test that a storage that cannot answer loses only that one part.
        """

        class Broken:
            """An instance whose storage is unreachable."""

            def get_job_snapshot(self, snapshot_id: str) -> Any:
                """Fail, the way a storage outage does."""
                raise RuntimeError("no storage")

            def get_run_stats(self, run_id: str) -> Dict[str, Any]:
                """Answer anyway."""
                return {"run_id": run_id}

        recorder = _Recorder()
        cedsens.convalesce_sensor(_ReachableContext(Broken()), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("job_snapshot", payload)
        self.assertEqual(payload["run_stats"]["run_id"], "abc")
        self.assertEqual(payload["dagster_run"]["job_name"], "nightly")

    def test3(self) -> None:
        """
        Test that a context with no instance still sends the run.

        Which is what a sensor built by hand in a test looks like.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor(_Context(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(payload["dagster_run"]["job_name"], "nightly")
        self.assertNotIn("job_snapshot", payload)


# #############################################################################
# Test_event_log1
# #############################################################################


# The same five names the plan asks `_FROM_INSTANCE`'s event log read to
# filter on -- restated here, not imported from the module under test, so
# this stays a test of the public contract rather than the private constant.
_EVENT_TYPE_NAMES = (
    "ASSET_CHECK_EVALUATION",
    "ASSET_MATERIALIZATION",
    "ASSET_OBSERVATION",
    "HANDLED_OUTPUT",
    "LOADED_INPUT",
    "RESOURCE_INIT_SUCCESS",
    "STEP_FAILURE",
)


def _fake_dagster_event_type() -> types.SimpleNamespace:
    """A stand-in `DagsterEventType` carrying the names this reads."""
    return types.SimpleNamespace(**{name: name for name in _EVENT_TYPE_NAMES})


class Test_event_log1(unittest.TestCase):
    """
    Test that the run's event log is read, filtered and forwarded, with its
    author-written metadata redacted.
    """

    def test1(self) -> None:
        """
        Test that matching records cross, and each one's `metadata` is
        redacted rather than sent verbatim.

        `all_logs`, the call the plan was written against, is gone on
        Dagster 1.13; this is read through `get_records_for_run` instead,
        which both supported versions carry.
        """

        class Instance(_Instance):
            """An instance whose storage also answers for the event log."""

            def get_records_for_run(
                self, run_id: str, of_type: Any = None
            ) -> types.SimpleNamespace:
                """The event log, the shape `EventLogConnection` has."""
                self.asked.append(run_id)
                self.of_type = of_type
                record = {
                    "event_log_entry": {
                        "dagster_event": {
                            "event_specific_data": {
                                "materialization": {
                                    "asset_key": "orders",
                                    "metadata": {
                                        "row_count": 1000,
                                        "sample": "alice@x.com",
                                    },
                                }
                            }
                        }
                    }
                }
                return types.SimpleNamespace(records=[record])

        fake_module = types.SimpleNamespace(
            DagsterEventType=_fake_dagster_event_type()
        )
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, {"dagster": fake_module}):
            cedsens.convalesce_sensor(
                _ReachableContext(Instance()), emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        entry = payload["event_log"][0]
        materialization = entry["event_log_entry"]["dagster_event"][
            "event_specific_data"
        ]["materialization"]
        self.assertEqual(materialization["asset_key"], "orders")
        self.assertEqual(
            materialization["metadata"],
            {"row_count": 1000, "redacted": True, "count": 1},
        )
        self.assertNotIn("alice@x.com", str(payload))
        excluded = recorder.sent[0]["excluded"]
        self.assertTrue(
            any(entry["reason"] == "sample redacted" for entry in excluded)
        )

    def test2(self) -> None:
        """
        Test that an instance with no event-log storage is left out cleanly.
        """

        class Instance(_Instance):
            """An instance that never learned `get_records_for_run`."""

        fake_module = types.SimpleNamespace(
            DagsterEventType=_fake_dagster_event_type()
        )
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, {"dagster": fake_module}):
            cedsens.convalesce_sensor(
                _ReachableContext(Instance()), emitter=recorder
            )
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("event_log", payload)

    def test3(self) -> None:
        """
        Test that Dagster's absence -- no `DagsterEventType` to resolve --
        is the same as nothing matching, not a raise.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor(_ReachableContext(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertNotIn("event_log", payload)


# #############################################################################
# Test_redact_metadata1
# #############################################################################


class Test_redact_metadata1(unittest.TestCase):
    """
    Test that the metadata entries which describe a table cross as they are,
    and every other entry stays redacted.
    """

    def test1(self) -> None:
        """
        Test that each allowed entry is kept, the rest counted, and the
        redaction declared.
        """
        allowed = {
            "dagster/row_count": {"value": 5},
            "dagster/table_name": {"text": "shop.orders"},
            "dagster/uri": {"text": "s3://b/orders"},
            "path": {"path": "s3://b/orders.parquet"},
            "dagster/relation_identifier": {"text": "db.shop.orders"},
            "size_in_bytes": {"value": 10},
            "dagster/code_version": {"text": "abc"},
            "convalesce_urn": {"text": "urn:cvl:dataset:x"},
            "datahub_urn": {"text": "urn:cvl:dataset:y"},
            "row_count": 5,
            "table_name": "orders",
            "uri": "s3://b/orders",
        }
        metadata = {
            **allowed,
            "preview": "alice@x.com",
            "owner_email": "bob@x.com",
        }
        log = [{"materialization": {"metadata": metadata}}]
        out, excluded = cedsens.redact_metadata(log, "event_log")
        self.assertEqual(
            out[0]["materialization"]["metadata"],
            {**allowed, "redacted": True, "count": 2},
        )
        self.assertNotIn("alice@x.com", str(out))
        self.assertEqual(
            excluded,
            [
                {
                    "path": "event_log[0].materialization.metadata",
                    "reason": "sample redacted",
                }
            ],
        )

    def test2(self) -> None:
        """
        Test that a column schema crosses as its columns' names and types,
        without their tags.
        """
        schema = {
            "schema": {
                "columns": [
                    {
                        "name": "id",
                        "type": "int",
                        "description": None,
                        "constraints": {"nullable": False},
                        "tags": {"pii": "secret-tag"},
                    }
                ],
                "constraints": {"other": ["a"]},
            }
        }
        out, excluded = cedsens.redact_metadata(
            {"metadata": {"dagster/column_schema": schema}}, "x"
        )
        self.assertEqual(
            out["metadata"]["dagster/column_schema"],
            {
                "schema": {
                    "columns": [
                        {
                            "name": "id",
                            "type": "int",
                            "description": None,
                            "constraints": {"nullable": False},
                        }
                    ]
                }
            },
        )
        self.assertNotIn("secret-tag", str(out))
        self.assertEqual(excluded, [])

    def test3(self) -> None:
        """
        Test that metadata which is not a mapping is redacted whole, and a
        column schema with no columns is not sent.
        """
        out, excluded = cedsens.redact_metadata(
            {
                "a": {"metadata": ["alice@x.com", "bob@x.com"]},
                "b": {"metadata": {"dagster/column_schema": "rows"}},
            },
            "",
        )
        self.assertEqual(out["a"]["metadata"], {"redacted": True, "count": 2})
        self.assertEqual(
            out["b"]["metadata"], {"dagster/column_schema": {"redacted": True}}
        )
        self.assertEqual([e["path"] for e in excluded], ["a.metadata"])

    def test4(self) -> None:
        """
        Test that the materialisations a step's stats repeat are redacted
        the same way as the event log's.
        """

        class Instance(_Instance):
            """An instance whose step stats carry a materialisation."""

            def get_run_step_stats(self, run_id: str) -> List[Dict[str, Any]]:
                """A step's stats, with the events it materialised."""
                metadata = {"dagster/row_count": 5, "preview": "alice@x.com"}
                event = {"materialization": {"metadata": metadata}}
                return [{"step_key": "load", "materialization_events": [event]}]

        recorder = _Recorder()
        cedsens.convalesce_sensor(
            _ReachableContext(Instance()), emitter=recorder
        )
        sent = recorder.sent[0]
        event = sent["payload"]["step_stats"][0]["materialization_events"][0]
        self.assertEqual(
            event["materialization"]["metadata"],
            {"dagster/row_count": 5, "redacted": True, "count": 1},
        )
        self.assertNotIn("alice@x.com", str(sent["payload"]))
        self.assertIn(
            "step_stats[0].materialization_events[0].materialization.metadata",
            [e["path"] for e in sent["excluded"]],
        )

    def test5(self) -> None:
        """
        Test that the column lineage, storage kind, a partition's row count
        and a dbt test's failing row count cross: names and counts, not rows.
        """
        allowed = {
            "dagster/column_lineage": {
                "lineage": {
                    "deps_by_column": {
                        "total": [
                            {
                                "asset_key": {"parts": ["raw"]},
                                "column_name": "amount",
                            }
                        ]
                    }
                }
            },
            "dagster/storage_kind": {"text": "snowflake"},
            "dagster/partition_row_count": {"value": 3},
            "dagster_dbt/failed_row_count": {"value": 1},
        }
        out, _ = cedsens.redact_metadata(
            {"metadata": {**allowed, "status": {"text": "fail"}}}, ""
        )
        self.assertEqual(
            out["metadata"], {**allowed, "redacted": True, "count": 1}
        )


# #############################################################################
# Test_asset_check1
# #############################################################################


class Test_asset_check1(unittest.TestCase):
    """
    Test that an asset check's evaluation crosses from the event log.
    """

    def test1(self) -> None:
        """
        Test that `ASSET_CHECK_EVALUATION` is asked for, and its record
        crosses with its outcome and its author's metadata redacted.
        """
        evaluation = {
            "asset_key": {"parts": ["orders"]},
            "check_name": "not_empty",
            "passed": False,
            "severity": "WARN",
            "metadata": {"failing": {"text": "alice@x.com"}},
        }

        class Instance(_Instance):
            """An instance whose event log holds one check evaluation."""

            def get_records_for_run(
                self, run_id: str, of_type: Any = None
            ) -> types.SimpleNamespace:
                """The event log, the shape `EventLogConnection` has."""
                self.asked.append(run_id)
                self.of_type = of_type
                record = {
                    "event_log_entry": {
                        "step_key": "orders_not_empty",
                        "dagster_event": {
                            "event_type_value": "ASSET_CHECK_EVALUATION",
                            "event_specific_data": dict(evaluation),
                        },
                    }
                }
                return types.SimpleNamespace(records=[record])

        instance = Instance()
        fake_module = types.SimpleNamespace(
            DagsterEventType=_fake_dagster_event_type()
        )
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, {"dagster": fake_module}):
            cedsens.convalesce_sensor(
                _ReachableContext(instance), emitter=recorder
            )
        self.assertIn("ASSET_CHECK_EVALUATION", instance.of_type)
        data = recorder.sent[0]["payload"]["event_log"][0]["event_log_entry"][
            "dagster_event"
        ]["event_specific_data"]
        self.assertEqual(data["check_name"], "not_empty")
        self.assertFalse(data["passed"])
        self.assertEqual(data["metadata"], {"redacted": True, "count": 1})
        self.assertNotIn("alice@x.com", str(recorder.sent[0]["payload"]))


# #############################################################################
# Test_step_failure1
# #############################################################################


class Test_step_failure1(unittest.TestCase):
    """
    Test that a failed step's error crosses whole.
    """

    def test1(self) -> None:
        """
        Test that the event log is read for step failures, and the error
        each carries is forwarded with its class, message, stack and cause.
        """
        error = {
            "cls_name": "ValueError",
            "message": "ValueError: bad row\n",
            "stack": ['  File "x.py", line 1\n'],
            "cause": {"cls_name": "KeyError", "message": "k", "stack": []},
        }

        class Instance(_Instance):
            """An instance whose event log holds one step failure."""

            def get_records_for_run(
                self, run_id: str, of_type: Any = None
            ) -> types.SimpleNamespace:
                """The event log, the shape `EventLogConnection` has."""
                self.asked.append(run_id)
                self.of_type = of_type
                record = {
                    "event_log_entry": {
                        "step_key": "load",
                        "dagster_event": {
                            "event_type_value": "STEP_FAILURE",
                            "event_specific_data": {"error": error},
                        },
                    }
                }
                return types.SimpleNamespace(records=[record])

        instance = Instance()
        fake_module = types.SimpleNamespace(
            DagsterEventType=_fake_dagster_event_type()
        )
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, {"dagster": fake_module}):
            cedsens.convalesce_sensor(
                _ReachableContext(instance), emitter=recorder
            )
        self.assertIn("STEP_FAILURE", instance.of_type)
        entry = recorder.sent[0]["payload"]["event_log"][0]
        self.assertEqual(
            entry["event_log_entry"]["dagster_event"]["event_specific_data"][
                "error"
            ],
            error,
        )


# #############################################################################
# Test_asset_group_names1
# #############################################################################


class Test_asset_group_names1(unittest.TestCase):
    """
    Test that asset group names are read from the sensor's repository, not
    from the run or its event log.
    """

    def test1(self) -> None:
        """
        Test that every key a multi-asset definition owns gets its group,
        read once per definition rather than once per key.
        """

        class Key:
            """Stands in for an `AssetKey`."""

            def __init__(self, *path: str) -> None:
                self.path = list(path)

        class AssetsDef:
            """Stands in for one `AssetsDefinition`, multi-asset or not."""

            def __init__(self, groups: Dict[Any, str]) -> None:
                self.group_names_by_key = groups

        raw_key = Key("orders")
        agg_key = Key("daily", "totals")
        combined = AssetsDef({raw_key: "core", agg_key: "core"})

        class Repository:
            """Stands in for the sensor's `RepositoryDefinition`."""

            assets_defs_by_key = {raw_key: combined, agg_key: combined}

        class Context:
            """A context exposing only what this reads."""

            repository_def = Repository()

        groups = cedsens.asset_group_names(Context())
        self.assertEqual(groups, {"orders": "core", "daily.totals": "core"})

    def test2(self) -> None:
        """
        Test that a context with no repository yields nothing, not a raise.
        """
        self.assertEqual(cedsens.asset_group_names(_Context()), {})

    def test3(self) -> None:
        """
        Test that asset definitions that cannot be read lose only the groups.
        """

        class Repository:
            """A repository whose asset definitions are unreadable."""

            @property
            def assets_defs_by_key(self) -> Any:
                """Fail, the way an unloaded repository does."""
                raise RuntimeError("not loaded")

        class Context:
            """A context exposing only what this reads."""

            repository_def = Repository()

        self.assertEqual(cedsens.asset_group_names(Context()), {})


# #############################################################################
# Test_asset_metadata1
# #############################################################################


class Test_asset_metadata1(unittest.TestCase):
    """
    Test that an asset's definition metadata crosses, down to what names
    its table.
    """

    def test1(self) -> None:
        """
        Test that the allowed entries are kept per asset, a dbt manifest is
        left behind, and an asset with none is left out.
        """

        class Key:
            """Stands in for an `AssetKey`."""

            def __init__(self, *path: str) -> None:
                self.path = list(path)

        orders, raw = Key("shop", "orders"), Key("raw")

        class AssetsDef:
            """Stands in for a dbt multi-asset `AssetsDefinition`."""

            metadata_by_key = {
                orders: {
                    "dagster/table_name": "db.main.orders",
                    "dagster/storage_kind": "duckdb",
                    "dagster_dbt/manifest": {"nodes": ["huge"]},
                },
                raw: {"dagster_dbt/unique_id": "model.raw"},
            }

        combined = AssetsDef()

        class Repository:
            """Stands in for the sensor's `RepositoryDefinition`."""

            assets_defs_by_key = {orders: combined, raw: combined}

        class Context(_ReachableContext):
            """A context whose repository defines the dbt assets."""

            repository_def = Repository()

        self.assertEqual(
            cedsens.asset_metadata(Context()),
            {
                "shop.orders": {
                    "dagster/table_name": "db.main.orders",
                    "dagster/storage_kind": "duckdb",
                }
            },
        )
        recorder = _Recorder()
        cedsens.convalesce_sensor(Context(), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        self.assertEqual(
            payload["asset_metadata"],
            {
                "shop.orders": {
                    "dagster/table_name": "db.main.orders",
                    "dagster/storage_kind": "duckdb",
                }
            },
        )
        self.assertNotIn("huge", str(payload))

    def test2(self) -> None:
        """
        Test that a context with no repository yields nothing, not a raise.
        """
        self.assertEqual(cedsens.asset_metadata(_Context()), {})


# #############################################################################
# Test_cloud_environment1
# #############################################################################


class Test_cloud_environment1(unittest.TestCase):
    """
    Test that Dagster Cloud's own environment is forwarded, minus anything
    that looks like a credential.
    """

    def test1(self) -> None:
        """
        Test that a documented Cloud variable crosses, and an unrelated one
        does not.
        """
        env = {
            "DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod",
            "DAGSTER_CLOUD_GIT_SHA": "abc123",
            "OTHER_VAR": "ignored",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=False):
            out = cedsens.cloud_environment()
        self.assertEqual(out.get("DAGSTER_CLOUD_DEPLOYMENT_NAME"), "prod")
        self.assertEqual(out.get("DAGSTER_CLOUD_GIT_SHA"), "abc123")
        self.assertNotIn("OTHER_VAR", out)

    def test2(self) -> None:
        """
        Test that a variable whose name suggests a credential never crosses,
        even though none of Dagster's own documented variables are one.
        """
        env = {"DAGSTER_CLOUD_API_TOKEN": "s3cret"}
        with unittest.mock.patch.dict(os.environ, env, clear=False):
            out = cedsens.cloud_environment()
        self.assertNotIn("DAGSTER_CLOUD_API_TOKEN", out)

    def test3(self) -> None:
        """
        Test that what names the deployment and its code crosses, and who
        wrote the commit and what they said about it do not.
        """
        kept = {
            "DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod",
            "DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT": "0",
            "DAGSTER_CLOUD_LOCATION_NAME": "my_location",
            "DAGSTER_CLOUD_GIT_SHA": "abc123",
            "DAGSTER_CLOUD_GIT_BRANCH": "main",
            "DAGSTER_CLOUD_GIT_URL": "https://example.com/my-org/my-repo",
            "DAGSTER_CLOUD_GIT_REPO": "my-org/my-repo",
            "DAGSTER_CLOUD_PULL_REQUEST_ID": "7",
            "DAGSTER_CLOUD_PULL_REQUEST_STATUS": "OPEN",
        }
        personal = {
            "DAGSTER_CLOUD_GIT_AUTHOR_EMAIL": "someone@example.com",
            "DAGSTER_CLOUD_GIT_AUTHOR_NAME": "Some One",
            "DAGSTER_CLOUD_GIT_MESSAGE": "fix the thing",
            "DAGSTER_CLOUD_AGENT_SOMETHING_NEW": "host-17",
        }
        with unittest.mock.patch.dict(os.environ, {**kept, **personal}):
            out = cedsens.cloud_environment()
        self.assertEqual(out, kept)

    def test4(self) -> None:
        """
        Test that the sensor sends the deployment without the commit's
        author.
        """
        env = {
            "DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod",
            "DAGSTER_CLOUD_GIT_AUTHOR_EMAIL": "someone@example.com",
        }
        recorder = _Recorder()
        with unittest.mock.patch.dict(os.environ, env):
            cedsens.convalesce_sensor(_Context(), emitter=recorder)
        self.assertEqual(
            recorder.sent[0]["payload"]["cloud_environment"],
            {"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod"},
        )
        self.assertNotIn("someone@example.com", repr(recorder.sent[0]))


# #############################################################################
# Test_local_path1
# #############################################################################


class Test_local_path1(unittest.TestCase):
    """
    Test that where an IO manager put a value crosses when it is in a store
    and stays behind when it is on a local disk.
    """

    def _metadata(self, path: Any) -> Dict[str, Any]:
        out, _ = cedsens.redact_metadata(
            {"metadata": {"path": path, "dagster/row_count": 5}}, "event_log"
        )
        kept: Dict[str, Any] = out["metadata"]
        return kept

    def test1(self) -> None:
        """
        Test that the URI of a path in an object store crosses as it is, as
        Dagster dumps a path value and as a bare text.
        """
        for path in (
            {"path": "s3://my-bucket/orders", "fspath": "s3://my-bucket/orders"},
            "gs://my-bucket/orders",
        ):
            self.assertEqual(
                self._metadata(path), {"path": path, "dagster/row_count": 5}
            )

    def test2(self) -> None:
        """
        Test that a path on a local disk is withheld and counted, whether
        absolute, relative, a `file://` URI or not text at all.
        """
        for path in (
            {"path": "/home/someone/storage/result", "fspath": "/home/someone"},
            "/home/someone/storage/result",
            "storage/result",
            "file:///home/someone/storage/result",
            {"path": None},
            {},
            17,
        ):
            self.assertEqual(
                self._metadata(path),
                {"dagster/row_count": 5, "redacted": True, "count": 1},
            )

    def test3(self) -> None:
        """
        Test that the switch sends a local path as it was written.
        """
        env = {"CONVALESCE_DAGSTER_SEND_LOCAL_PATHS": "true"}
        with unittest.mock.patch.dict(os.environ, env):
            kept = self._metadata("/data/orders.parquet")
        self.assertEqual(
            kept, {"path": "/data/orders.parquet", "dagster/row_count": 5}
        )


# #############################################################################
# Test_internal_tags1
# #############################################################################


class Test_internal_tags1(unittest.TestCase):
    """
    Test that the tags Dagster keeps for its own machinery stay behind.
    """

    def test1(self) -> None:
        """
        Test that the code server's host and socket do not cross, that the
        tags a run was launched with do, and that the removal is declared.
        """
        context = _Context(
            run={
                "job_name": "nightly",
                "run_id": "abc",
                "tags": {
                    ".dagster/grpc_info": '{"host": "host-17", "socket": "/tmp/x"}',
                    ".dagster/run_worker": "worker-3",
                    ".dagster/scheduled_execution_time": "2026-01-01T00:00:00",
                    ".dagster/repository": "__repository__@my_location",
                    "dagster/sensor_name": "kick",
                    "team": "data",
                },
            }
        )
        recorder = _Recorder()
        cedsens.convalesce_sensor(context, emitter=recorder)
        sent = recorder.sent[0]
        self.assertEqual(
            sent["payload"]["dagster_run"]["tags"],
            {
                ".dagster/scheduled_execution_time": "2026-01-01T00:00:00",
                ".dagster/repository": "__repository__@my_location",
                "dagster/sensor_name": "kick",
                "team": "data",
            },
        )
        self.assertNotIn("host-17", repr(sent["payload"]))
        self.assertEqual(
            sent["excluded"],
            [
                {
                    "path": "dagster_run.tags..dagster/grpc_info",
                    "reason": "internal tag not sent",
                },
                {
                    "path": "dagster_run.tags..dagster/run_worker",
                    "reason": "internal tag not sent",
                },
            ],
        )

    def test_who_launched_it_and_which_agent_ran_it_stay_behind(self) -> None:
        """
        Test that the launching user's login, the agent and its process do
        not cross, nor the agent's environment repeated in the run's origin.
        """
        context = _Context(
            run={
                "job_name": "nightly",
                "run_id": "abc",
                "tags": {
                    "user": "someone@example.com",
                    "process/pid": "4242",
                    "dagster/agent_id": "agent-1",
                    "dagster/agent_label": "team-agent",
                    "dagster/code_location": "my_location",
                },
                "job_code_origin": {
                    "repository_origin": {
                        "container_context": {
                            "env_vars": [
                                "WAREHOUSE_PASSWORD=hunter2",
                                "REGION=eu",
                            ],
                            "k8s": {"namespace": "data"},
                        }
                    }
                },
            }
        )
        recorder = _Recorder()
        cedsens.convalesce_sensor(context, emitter=recorder)
        sent = recorder.sent[0]
        run = sent["payload"]["dagster_run"]
        self.assertEqual(run["tags"], {"dagster/code_location": "my_location"})
        context_sent = run["job_code_origin"]["repository_origin"][
            "container_context"
        ]
        self.assertEqual(context_sent, {"k8s": {"namespace": "data"}})
        text = repr(sent["payload"])
        for kept_back in ("someone@example.com", "hunter2", "agent-1", "4242"):
            self.assertNotIn(kept_back, text)
        reasons = {entry["path"]: entry["reason"] for entry in sent["excluded"]}
        self.assertEqual(
            reasons["dagster_run.tags.user"], "internal tag not sent"
        )
        self.assertEqual(
            reasons[
                "dagster_run.job_code_origin.repository_origin"
                ".container_context.env_vars"
            ],
            "environment not sent",
        )

    def test2(self) -> None:
        """
        Test that a run without tags crosses with nothing declared left out.
        """
        recorder = _Recorder()
        cedsens.convalesce_sensor(_Context(), emitter=recorder)
        self.assertEqual(recorder.sent[0]["excluded"], [])


# #############################################################################
# Test_plain_metadata1
# #############################################################################


class Test_plain_metadata1(unittest.TestCase):
    """
    Test that the numbers an author attached cross under their own names,
    and everything that can hold the data itself stays redacted.
    """

    def test1(self) -> None:
        """
        Test that a number, a flag and a timestamp cross as they are, as a
        Dagster value class dumps them and as a bare value; that text, a
        table, markdown, JSON, a path and a link do not; and that the rest
        are counted into the marker.
        """
        plain = {
            "rows_rejected": {"value": 12},
            "mean_amount": {"value": 41.5},
            "within_threshold": {"value": True},
            "loaded_at": {"value": 1790383261.0},
            "bare_count": 7,
        }
        withheld = {
            "preview": {"md_str": "| alice@x.com |"},
            "sample": {"records": [{"email": "alice@x.com"}], "schema": {}},
            "note": {"text": "alice@x.com"},
            "bare_text": "alice@x.com",
            "config": {"data": {"owner": "alice@x.com"}},
            "export": {"path": "/exports/alice"},
            "dashboard": {"url": "https://example.com/alice"},
            "wrapped_text": {"value": "alice@x.com"},
            "not_finite": {"value": float("inf")},
            "empty": {"value": None},
        }
        out, excluded = cedsens.redact_metadata(
            {"materialization": {"metadata": {**plain, **withheld}}}, "event_log"
        )
        self.assertEqual(
            out["materialization"]["metadata"],
            {**plain, "redacted": True, "count": len(withheld)},
        )
        self.assertNotIn("alice", str(out))
        self.assertEqual(
            [entry["path"] for entry in excluded],
            ["event_log.materialization.metadata"],
        )

    def test2(self) -> None:
        """
        Test that the switch restores what crossed before: the allowed
        entries, and the marker for every other one.
        """
        metadata = {"dagster/row_count": {"value": 5}, "rows_rejected": 12}
        env = {"CUSTOMER_CONVALESCE_DAGSTER_SEND_METADATA": "false"}
        with unittest.mock.patch.dict(os.environ, env, clear=False):
            out, _ = cedsens.redact_metadata({"metadata": metadata}, "")
        self.assertEqual(
            out["metadata"],
            {"dagster/row_count": {"value": 5}, "redacted": True, "count": 1},
        )

    def test3(self) -> None:
        """
        Test that only so many of an author's entries cross from one
        mapping, and that a name too long to be one, a name that is not
        text, and the marker's own names never do.
        """
        metadata: Dict[Any, Any] = {
            f"m{i:03}": i for i in range(cedsens.MAX_PLAIN_VALUES + 5)
        }
        metadata["n" * (cedsens.MAX_PLAIN_NAME_CHARS + 1)] = 1
        metadata[7] = 1
        out, _ = cedsens.redact_metadata({"metadata": metadata}, "")
        kept = out["metadata"]
        self.assertEqual(kept["count"], 7)
        self.assertEqual(
            sorted(name for name in kept if name not in ("redacted", "count")),
            [f"m{i:03}" for i in range(cedsens.MAX_PLAIN_VALUES)],
        )
        # An author's own `count` is never taken for the marker's.
        out, _ = cedsens.redact_metadata(
            {"metadata": {"count": 9, "x": "y"}}, ""
        )
        self.assertEqual(out["metadata"], {"redacted": True, "count": 2})

    def test4(self) -> None:
        """
        Test that what an observation, an asset check and a `Failure` carry
        is treated the same way, where the event log holds it and where the
        event that ended the run repeats the failure.
        """

        def failure() -> Dict[str, Any]:
            return {"user_failure_data": {"metadata": _author_metadata()}}

        class Instance(_Instance):
            """An instance whose event log holds one event of each kind."""

            def get_records_for_run(
                self, run_id: str, of_type: Any = None
            ) -> types.SimpleNamespace:
                """The event log, the shape `EventLogConnection` has."""
                self.asked.append(run_id)
                self.of_type = of_type
                data = [
                    {"asset_observation": {"metadata": _author_metadata()}},
                    {"check_name": "fresh", "metadata": _author_metadata()},
                    failure(),
                ]
                return types.SimpleNamespace(
                    records=[
                        {
                            "event_log_entry": {
                                "dagster_event": {"event_specific_data": item}
                            }
                        }
                        for item in data
                    ]
                )

        class Context(_ReachableContext):
            """A context whose run ended on that failure."""

            @property
            def dagster_event(self) -> Dict[str, Any]:
                return {
                    "event_specific_data": {
                        "first_step_failure_event": {
                            "event_specific_data": failure()
                        }
                    }
                }

        fake_module = types.SimpleNamespace(
            DagsterEventType=_fake_dagster_event_type()
        )
        recorder = _Recorder()
        with unittest.mock.patch.dict(sys.modules, {"dagster": fake_module}):
            cedsens.convalesce_sensor(Context(Instance()), emitter=recorder)
        payload = recorder.sent[0]["payload"]
        expected = {"rows_seen": {"value": 3}, "redacted": True, "count": 1}
        log = [
            entry["event_log_entry"]["dagster_event"]["event_specific_data"]
            for entry in payload["event_log"]
        ]
        self.assertEqual(log[0]["asset_observation"]["metadata"], expected)
        self.assertEqual(log[1]["metadata"], expected)
        self.assertEqual(log[2]["user_failure_data"]["metadata"], expected)
        ended = payload["dagster_event"]["event_specific_data"]
        self.assertEqual(
            ended["first_step_failure_event"]["event_specific_data"][
                "user_failure_data"
            ]["metadata"],
            expected,
        )
        self.assertNotIn("alice@x.com", str(payload))


def _author_metadata() -> Dict[str, Any]:
    """
    What an author might attach: one number, one line of text.

    :return: a fresh mapping, since redaction is checked on each copy
    """
    return {"rows_seen": {"value": 3}, "note": {"text": "alice@x.com"}}


# #############################################################################
# Test_dagster_url1
# #############################################################################


class Test_dagster_url1(unittest.TestCase):
    """
    Test that the deployment's own UI address crosses with every run.
    """

    def test1(self) -> None:
        """
        Test that the address is sent as set, without a trailing slash, and
        read under the prefix a platform puts on a setting.
        """
        recorder = _Recorder()
        env = {
            "CUSTOMER_CONVALESCE_DAGSTER_URL": " https://dagster.example.com/ "
        }
        with unittest.mock.patch.dict(os.environ, env, clear=False):
            cedsens.convalesce_sensor(_Context(), emitter=recorder)
        self.assertEqual(
            recorder.sent[0]["payload"]["dagster_url"],
            "https://dagster.example.com",
        )

    def test2(self) -> None:
        """
        Test that nothing is sent when the address is not set.
        """
        recorder = _Recorder()
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            cedsens.convalesce_sensor(_Context(), emitter=recorder)
        self.assertNotIn("dagster_url", recorder.sent[0]["payload"])


# #############################################################################
# Test_declared_lineage1
# #############################################################################


class Test_declared_lineage1(unittest.TestCase):
    """
    Test the `lineage=` keyword: the datasets an author declares for an op.
    """

    def test1(self) -> None:
        """
        Test that a mapping crosses as each op's sorted references, whether
        a side is one reference, a list or a set, and that an op with
        nothing declared is left out.
        """
        urn = "urn:cvl:dataset:(urn:cvl:dataPlatform:snowflake,my_db.raw.orders,PROD)"
        recorder = _Recorder()
        cedsens.convalesce_sensor(
            _Context(),
            emitter=recorder,
            lineage={
                "load_orders": {
                    "inputs": {urn, "s3://my-bucket/exports/orders"},
                    "outputs": urn,
                },
                "tidy": {"inputs": [], "outputs": None},
            },
        )
        self.assertEqual(
            recorder.sent[0]["payload"]["lineage"],
            {
                "load_orders": {
                    "inputs": ["s3://my-bucket/exports/orders", urn],
                    "outputs": [urn],
                }
            },
        )

    def test2(self) -> None:
        """
        Test that a function is called with the context, and that what it
        returns may hold objects with `inputs` and `outputs` of their own.
        """
        seen: List[Any] = []

        def extract(context: Any) -> Dict[str, Any]:
            seen.append(context)
            sides = types.SimpleNamespace(inputs=[], outputs=["s3://b/out"])
            return {"export": sides}

        context = _Context()
        recorder = _Recorder()
        cedsens.convalesce_sensor(context, emitter=recorder, lineage=extract)
        self.assertEqual(seen, [context])
        self.assertEqual(
            recorder.sent[0]["payload"]["lineage"],
            {"export": {"outputs": ["s3://b/out"]}},
        )

    def test3(self) -> None:
        """
        Test that a declaration that cannot be read costs the declaration
        and never the run's report.
        """

        def broken(context: Any) -> Dict[str, Any]:
            raise RuntimeError("no catalogue")

        for lineage in (broken, 7, {"load": {"inputs": 7}}):
            recorder = _Recorder()
            cedsens.convalesce_sensor(
                _Context(), emitter=recorder, lineage=lineage
            )
            payload = recorder.sent[0]["payload"]
            self.assertEqual(payload["dagster_run"]["job_name"], "nightly")
            self.assertNotIn("lineage", payload)

"""
Tests for forwarding the OpenLineage provider's events.

Run with `make test`.
"""

import json
import logging
import os
import shutil
import tempfile
import types
import unittest
import unittest.mock
from typing import Any, Dict, List, Optional

import convalesce_emit_airflow.openlineage as cealol

_LOG = logging.getLogger(__name__)

_OURS = json.dumps({"type": cealol.TRANSPORT_TYPE})


# #############################################################################
# _Recorder
# #############################################################################


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []
        self.flushes = 0

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Count the flush; nothing is queued, so nothing to send."""
        self.flushes += 1


def _run_event() -> Dict[str, Any]:
    """
    A RunEvent as OpenLineage's serde renders one, trimmed to what matters.

    :return: the event
    """
    return {
        "eventType": "COMPLETE",
        "eventTime": "2026-09-25T10:00:00.000000+00:00",
        "run": {"runId": "0192-run", "facets": {}},
        "job": {
            "namespace": "default",
            "name": "orders.load",
            "facets": {"sql": {"query": "INSERT INTO orders SELECT 1"}},
        },
        "inputs": [
            {
                "namespace": "postgres://etl:pa55@db:5432",
                "name": "shop.public.raw_orders",
                "facets": {},
            }
        ],
        "outputs": [],
        "producer": "https://github.com/apache/airflow/tree/providers",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
    }


# #############################################################################
# Test_transport1
# #############################################################################


class Test_transport1(unittest.TestCase):
    """
    Test that the transport forwards each event as an observation.
    """

    def test1(self) -> None:
        """
        Test that an event arrives as observation `openlineage`, whole,
        under `run_event`, and is flushed before `emit` returns.

        The provider emits task events from a process it forks and ends
        with `os._exit`; anything queued would never leave.
        """
        recorder = _Recorder()
        transport = cealol.ConvalesceTransport(
            cealol.ConvalesceConfig.from_dict({"type": "x"}), emitter=recorder
        )
        transport.emit(_run_event())
        self.assertEqual(len(recorder.sent), 1)
        sent = recorder.sent[0]
        self.assertEqual(sent["tool"], "airflow")
        self.assertEqual(sent["event"], "openlineage")
        run_event = sent["payload"]["run_event"]
        self.assertEqual(run_event["run"]["runId"], "0192-run")
        self.assertEqual(
            run_event["job"]["facets"]["sql"]["query"],
            "INSERT INTO orders SELECT 1",
        )
        self.assertEqual(recorder.flushes, 1)

    def test2(self) -> None:
        """
        Test that a password in a facet is masked and declared, and the
        rest of the value survives.
        """
        recorder = _Recorder()
        cealol.ConvalesceTransport(emitter=recorder).emit(_run_event())
        sent = recorder.sent[0]
        namespace = sent["payload"]["run_event"]["inputs"][0]["namespace"]
        self.assertEqual(namespace, "postgres://etl:***@db:5432")
        self.assertIn(
            {
                "path": "run_event.inputs[0].namespace",
                "reason": "credential masked",
            },
            sent["excluded"],
        )

    def test3(self) -> None:
        """
        Test that an event goes through OpenLineage's own serde when the
        client is installed.
        """

        class Serde:
            """Stands in for `openlineage.client.serde.Serde`."""

            @staticmethod
            def to_dict(event: Any) -> Dict[str, Any]:
                """Render the event the way the spec does."""
                return {"eventType": event.kind}

        serde = types.SimpleNamespace(Serde=Serde)
        recorder = _Recorder()
        with unittest.mock.patch.dict(
            "sys.modules", {"openlineage.client.serde": serde}
        ):
            cealol.ConvalesceTransport(emitter=recorder).emit(
                types.SimpleNamespace(kind="START")
            )
        self.assertEqual(
            recorder.sent[0]["payload"], {"run_event": {"eventType": "START"}}
        )

    def test4(self) -> None:
        """
        Test that a failing emitter is logged, never raised into the task.
        """

        class Broken(_Recorder):
            """An emitter whose send fails."""

            def emit(self, **kwargs: Any) -> None:
                raise RuntimeError("down")

        with self.assertLogs(cealol.__name__, level="WARNING"):
            cealol.ConvalesceTransport(emitter=Broken()).emit(_run_event())

    def test5(self) -> None:
        """
        Test that the transport declares a kind, which the provider's
        adapter names its metrics after, and its own config class.
        """
        self.assertEqual(cealol.ConvalesceTransport.kind, "convalesce")
        self.assertIs(
            cealol.ConvalesceTransport.config_class, cealol.ConvalesceConfig
        )
        self.assertEqual(
            cealol.TRANSPORT_TYPE,
            "convalesce_emit_airflow.openlineage.ConvalesceTransport",
        )


# #############################################################################
# Test_enable1
# #############################################################################


class Test_enable1(unittest.TestCase):
    """
    Test that the transport is switched on only where nobody chose.
    """

    def setUp(self) -> None:
        """
        Run from an empty directory with no home OpenLineage file, and a
        provider that is installed and has a listener.
        """
        self._dir = tempfile.mkdtemp()
        self._cwd = os.getcwd()
        os.chdir(self._dir)
        self.listener = object()
        patches: List[Any] = [
            unittest.mock.patch.object(
                cealol, "provider_installed", return_value=True
            ),
            unittest.mock.patch.object(
                cealol, "_provider_listener", return_value=self.listener
            ),
            unittest.mock.patch.dict(os.environ, {"HOME": self._dir}),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def tearDown(self) -> None:
        """Return to where the run started."""
        os.chdir(self._cwd)
        shutil.rmtree(self._dir)

    @staticmethod
    def _conf(**options: str) -> Any:
        """
        A reader for `[openlineage]` options.

        :param options: the options that are set
        :return: the reader
        """

        def read(option: str) -> Optional[str]:
            return options.get(option)

        return read

    def test1(self) -> None:
        """
        Test that with nothing configured, the transport is set and the
        provider's listener is handed back for this plugin to register.
        """
        env: Dict[str, str] = {}
        listeners = cealol.enable(env, self._conf())
        self.assertEqual(
            json.loads(env[cealol.TRANSPORT_ENV]),
            {"type": cealol.TRANSPORT_TYPE},
        )
        self.assertEqual(listeners, [self.listener])

    def test2(self) -> None:
        """
        Test that every way of configuring OpenLineage, or switching it
        off, leaves it alone.
        """
        cases = [
            ({}, {"transport": '{"type": "http", "url": "x"}'}),
            ({}, {"config_path": "/etc/ol.yml"}),
            ({}, {"disabled": "True"}),
            ({"OPENLINEAGE_URL": "http://marquez:5000"}, {}),
            ({"OPENLINEAGE_CONFIG": "/etc/ol.yml"}, {}),
            ({"OPENLINEAGE_DISABLED": "true"}, {}),
            ({"OPENLINEAGE__TRANSPORT__TYPE": "console"}, {}),
        ]
        for env, options in cases:
            with self.subTest(env=env, options=options):
                before = dict(env)
                self.assertEqual(cealol.enable(env, self._conf(**options)), [])
                self.assertEqual(env, before)

    def test3(self) -> None:
        """
        Test that an `openlineage.yml` where the client would find it
        counts as configured.
        """
        with open("openlineage.yml", "w", encoding="utf-8") as handle:
            handle.write("transport:\n  type: console\n")
        env: Dict[str, str] = {}
        self.assertEqual(cealol.enable(env, self._conf()), [])
        self.assertNotIn(cealol.TRANSPORT_ENV, env)

    def test4(self) -> None:
        """
        Test that `CONVALESCE_OPENLINEAGE=false`, or the emitter switched
        off, opts out.
        """
        for name in ("CONVALESCE_OPENLINEAGE", "CONVALESCE_ENABLED"):
            with self.subTest(name=name):
                env = {name: "false"}
                self.assertEqual(cealol.enable(env, self._conf()), [])
                self.assertNotIn(cealol.TRANSPORT_ENV, env)

    def test5(self) -> None:
        """
        Test that nothing happens where the provider is not installed.
        """
        env: Dict[str, str] = {}
        with unittest.mock.patch.object(
            cealol, "provider_installed", return_value=False
        ):
            self.assertEqual(cealol.enable(env, self._conf()), [])
        self.assertEqual(env, {})

    def test6(self) -> None:
        """
        Test that a process inheriting the transport this plugin set in its
        parent still registers the listener, rather than mistaking the
        setting for the user's.
        """
        env = {cealol.TRANSPORT_ENV: _OURS}
        listeners = cealol.enable(env, self._conf(transport=_OURS))
        self.assertEqual(listeners, [self.listener])

    def test7(self) -> None:
        """
        Test that the provider's cached configuration is forgotten, so a
        reader called before this plugin loaded sees the transport.
        """
        cleared: List[str] = []
        conf = types.ModuleType(cealol.PROVIDER_CONF)
        conf.transport = types.SimpleNamespace(  # type: ignore[attr-defined]
            cache_clear=lambda: cleared.append("transport")
        )
        conf.is_disabled = types.SimpleNamespace(  # type: ignore[attr-defined]
            cache_clear=lambda: cleared.append("is_disabled")
        )
        with unittest.mock.patch.dict(
            "sys.modules",
            {cealol.PROVIDER_CONF: conf},
        ):
            cealol.enable({}, self._conf())
        self.assertEqual(sorted(cleared), ["is_disabled", "transport"])

    def test8(self) -> None:
        """
        Test that a configuration reader that raises leaves Airflow able to
        start and OpenLineage untouched.
        """

        def read(option: str) -> Optional[str]:
            raise RuntimeError(option)

        env: Dict[str, str] = {}
        with self.assertLogs(cealol.__name__, level="WARNING"):
            self.assertEqual(cealol.enable(env, read), [])
        self.assertEqual(env, {})


# #############################################################################
# Test_hook_lineage_readers1
# #############################################################################


class Test_hook_lineage_readers1(unittest.TestCase):
    """
    Test that Airflow's hook lineage reader is found wherever it lives.
    """

    def test1(self) -> None:
        """
        Test that the first module carrying the reader wins, and a module
        that is missing is passed over.
        """
        reader = type("HookLineageReader", (), {})
        module = types.ModuleType("_convalesce_lineage")
        module.HookLineageReader = reader  # type: ignore[attr-defined]
        with unittest.mock.patch.dict(
            "sys.modules", {"_convalesce_lineage": module}
        ):
            with unittest.mock.patch.object(
                cealol,
                "_HOOK_LINEAGE_READER_MODULES",
                ("_convalesce_absent_lineage", "_convalesce_lineage"),
            ):
                self.assertEqual(cealol.hook_lineage_readers(), [reader])

    def test2(self) -> None:
        """
        Test that an Airflow older than 2.10, which has no reader, gets none.
        """
        with unittest.mock.patch.object(
            cealol,
            "_HOOK_LINEAGE_READER_MODULES",
            ("_convalesce_absent_lineage",),
        ):
            self.assertEqual(cealol.hook_lineage_readers(), [])

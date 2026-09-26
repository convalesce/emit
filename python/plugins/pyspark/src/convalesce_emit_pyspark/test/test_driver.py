"""
Tests for the driver failure hooks.

Run with `make test`.
"""

import logging
import os
import pathlib
import sys
import threading
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest import mock

import convalesce_emit_pyspark as cepyspar
import convalesce_emit_pyspark.driver as cepysdri

_LOG = logging.getLogger(__name__)

# Beside the package in the source tree; at site-packages' root once installed.
_PTH = next(
    path
    for path in (
        pathlib.Path(cepysdri.__file__).parent / "convalesce_emit_pyspark.pth",
        pathlib.Path(cepysdri.__file__).parent.parent
        / "convalesce_emit_pyspark.pth",
    )
    if path.is_file()
).read_text()


class _Recorder:
    """Stands in for an emitter, remembering what it was given."""

    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []
        self.flushes = 0

    def emit(self, **kwargs: Any) -> None:
        """Record one observation."""
        self.sent.append(kwargs)

    def flush(self) -> None:
        """Count the flush the hook must make before the driver exits."""
        self.flushes += 1


class _Broken:
    """An emitter that fails every call."""

    def emit(self, **_kwargs: Any) -> None:
        """Fail."""
        raise RuntimeError("emitter down")

    def flush(self) -> None:
        """Fail."""
        raise RuntimeError("emitter down")


class _Context:
    """Stands in for PySpark's active SparkContext."""

    appName = "customer_features_broken"

    @property
    def applicationId(self) -> str:  # pylint: disable=invalid-name
        """What PySpark reads off the JVM."""
        return "local-1790387078840"


def _pyspark(context: Optional[Any]) -> types.ModuleType:
    """
    A stand-in `pyspark` module.

    :param context: its active SparkContext, or None for no session
    :return: the module
    """
    module = types.ModuleType("pyspark")
    spark_context = type("SparkContext", (), {"_active_spark_context": context})
    setattr(module, "SparkContext", spark_context)
    setattr(module, "__version__", "3.5.3")
    return module


def _raised(exc: BaseException) -> BaseException:
    """
    The exception, raised and caught so it carries a traceback.

    :param exc: the exception to raise
    :return: it, with its traceback
    """
    try:
        raise exc
    except BaseException as caught:  # pylint: disable=broad-exception-caught
        return caught
    raise AssertionError("unreachable")


class _HookTestCase(unittest.TestCase):
    """Every test runs with its own hooks, a PySpark driver and argv."""

    def setUp(self) -> None:
        super().setUp()
        self.previous = mock.Mock()
        self.previous_threading = mock.Mock()
        self.recorder = _Recorder()
        patches: List[Any] = [
            mock.patch.object(sys, "excepthook", self.previous),
            mock.patch.object(threading, "excepthook", self.previous_threading),
            mock.patch.object(sys, "exit", sys.exit),
            mock.patch.object(sys, "argv", ["/opt/jobs/revenue.py", "broken"]),
            mock.patch.dict(sys.modules, {"pyspark": _pyspark(_Context())}),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(cepysdri.uninstall)

    def install(self, emitter: Any = None) -> bool:
        """Install the hooks, sending to this test's recorder by default."""
        return cepyspar.install(emitter=emitter or self.recorder)

    def fail_main(self, exc: BaseException) -> BaseException:
        """Hand an uncaught exception to the main-thread hook."""
        exc = _raised(exc)
        sys.excepthook(type(exc), exc, exc.__traceback__)
        return exc


# #############################################################################
# Test_excepthook
# #############################################################################


class Test_excepthook1(_HookTestCase):
    """An uncaught exception in the main thread."""

    def test_sends_one_driver_failure(self) -> None:
        """Sends one driver failure."""
        self.assertTrue(self.install())
        self.fail_main(ValueError("relation analytics.x does not exist"))
        self.assertEqual(len(self.recorder.sent), 1)
        sent = self.recorder.sent[0]
        self.assertEqual(sent["tool"], "spark")
        self.assertEqual(sent["event"], "driver_failure")
        self.assertEqual(sent["tool_version"], "3.5.3")
        payload = sent["payload"]
        self.assertEqual(payload["application_id"], "local-1790387078840")
        self.assertEqual(payload["application_name"], "customer_features_broken")
        self.assertEqual(payload["argv"], ["/opt/jobs/revenue.py", "broken"])
        self.assertEqual(payload["pyspark_version"], "3.5.3")
        self.assertTrue(payload["python_version"])
        detail = payload["error_detail"]
        self.assertEqual(detail["type"], "builtins.ValueError")
        self.assertEqual(
            detail["message"], "relation analytics.x does not exist"
        )
        self.assertIn("ValueError", detail["traceback"])
        self.assertEqual(self.recorder.flushes, 1)

    def test_chains_the_previous_hook(self) -> None:
        """Chains the previous hook."""
        self.install()
        exc = self.fail_main(KeyError("x"))
        self.previous.assert_called_once_with(KeyError, exc, exc.__traceback__)

    def test_a_previous_hook_that_raises_does_not_escape(self) -> None:
        """A previous hook that raises does not escape."""
        self.previous.side_effect = RuntimeError("hook broke")
        self.install()
        with mock.patch.object(sys, "__excepthook__") as fallback:
            self.fail_main(KeyError("x"))
        self.assertEqual(len(self.recorder.sent), 1)
        fallback.assert_called_once()

    def test_sends_once_per_process(self) -> None:
        """Sends once per process."""
        self.install()
        self.fail_main(ValueError("first"))
        self.fail_main(ValueError("second"))
        self.assertEqual(len(self.recorder.sent), 1)
        self.assertEqual(self.previous.call_count, 2)

    def test_install_twice_hooks_once(self) -> None:
        """Install twice hooks once."""
        self.install()
        self.install(_Recorder())
        self.fail_main(ValueError("x"))
        self.assertEqual(len(self.recorder.sent), 1)
        self.previous.assert_called_once()

    def test_uninstall_restores_the_hooks(self) -> None:
        """Uninstall restores the hooks."""
        self.install()
        cepysdri.uninstall()
        self.assertIs(sys.excepthook, self.previous)
        self.assertIs(threading.excepthook, self.previous_threading)


# #############################################################################
# Test_threading_excepthook
# #############################################################################


class Test_threading_excepthook1(_HookTestCase):
    """An uncaught exception in another thread."""

    def test_sends_and_chains(self) -> None:
        """Sends and chains."""
        self.install()

        def _work() -> None:
            raise ValueError("in a thread")

        thread = threading.Thread(target=_work)
        thread.start()
        thread.join()
        self.assertEqual(len(self.recorder.sent), 1)
        detail = self.recorder.sent[0]["payload"]["error_detail"]
        self.assertEqual(detail["message"], "in a thread")
        self.previous_threading.assert_called_once()

    def test_a_system_exit_in_a_thread_is_not_a_failure(self) -> None:
        """A system exit in a thread is not a failure."""
        self.install()
        args = types.SimpleNamespace(
            exc_type=SystemExit, exc_value=SystemExit(1), exc_traceback=None
        )
        threading.excepthook(args)  # type: ignore[arg-type]
        self.assertEqual(self.recorder.sent, [])
        self.previous_threading.assert_called_once_with(args)


# #############################################################################
# Test_exit
# #############################################################################


class Test_exit1(_HookTestCase):
    """`sys.exit()` from the driver."""

    def test_a_non_zero_exit_is_reported_at_interpreter_exit(self) -> None:
        """A non zero exit is reported at interpreter exit."""
        self.install()
        with self.assertRaises(SystemExit) as raised:
            sys.exit(3)
        self.assertEqual(raised.exception.code, 3)
        self.assertEqual(self.recorder.sent, [])
        hook = cepysdri._INSTALLED  # pylint: disable=protected-access
        assert hook is not None
        hook.at_exit()
        payload = self.recorder.sent[0]["payload"]
        self.assertEqual(payload["error_detail"]["type"], "builtins.SystemExit")
        self.assertEqual(payload["error_detail"]["message"], "3")
        self.assertEqual(payload["application_id"], "local-1790387078840")

    def test_a_zero_exit_is_not(self) -> None:
        """A zero exit is not."""
        self.install()
        for code in (0, None):
            with self.assertRaises(SystemExit):
                sys.exit(code)
        hook = cepysdri._INSTALLED  # pylint: disable=protected-access
        assert hook is not None
        hook.at_exit()
        self.assertEqual(self.recorder.sent, [])


# #############################################################################
# Test_context
# #############################################################################


class Test_context1(_HookTestCase):
    """What the driver says about its application."""

    def test_no_session_sends_nulls(self) -> None:
        """No session sends nulls."""
        with mock.patch.dict(sys.modules, {"pyspark": _pyspark(None)}):
            self.install()
            self.fail_main(ValueError("before the session"))
        payload = self.recorder.sent[0]["payload"]
        self.assertIsNone(payload["application_id"])
        self.assertIsNone(payload["application_name"])

    def test_a_context_that_cannot_be_read_sends_nulls(self) -> None:
        """A context that cannot be read sends nulls."""
        context = mock.Mock()
        type(context).applicationId = mock.PropertyMock(
            side_effect=RuntimeError("gateway gone")
        )
        context.appName = None
        with mock.patch.dict(sys.modules, {"pyspark": _pyspark(context)}):
            self.install()
            self.fail_main(ValueError("x"))
        payload = self.recorder.sent[0]["payload"]
        self.assertIsNone(payload["application_id"])
        self.assertIsNone(payload["application_name"])

    def test_not_a_pyspark_process_sends_nothing(self) -> None:
        """Not a pyspark process sends nothing."""
        with mock.patch.dict(sys.modules):
            del sys.modules["pyspark"]
            self.install()
            self.fail_main(ValueError("x"))
        self.assertEqual(self.recorder.sent, [])
        self.previous.assert_called_once()

    def test_an_executor_worker_sends_nothing(self) -> None:
        """An executor worker sends nothing."""
        main = types.ModuleType("__main__")
        setattr(main, "__spec__", types.SimpleNamespace(name="pyspark.daemon"))
        with mock.patch.dict(sys.modules, {"__main__": main}):
            self.install()
            self.fail_main(ValueError("x"))
        self.assertEqual(self.recorder.sent, [])

    def test_credentials_in_argv_are_masked(self) -> None:
        """Credentials in argv are masked."""
        argv = [
            "job.py",
            "--password",
            "hunter2",
            "--url=postgresql://etl:s3cret@db/shop",
            "--table",
            "orders",
        ]
        with mock.patch.object(sys, "argv", argv):
            self.install()
            self.fail_main(ValueError("x"))
        sent = self.recorder.sent[0]
        self.assertEqual(
            sent["payload"]["argv"],
            [
                "job.py",
                "--password",
                "***",
                "--url=postgresql://etl:***@db/shop",
                "--table",
                "orders",
            ],
        )
        self.assertEqual(
            sorted(e["path"] for e in sent["excluded"]), ["argv[2]", "argv[3]"]
        )


# #############################################################################
# Test_never_raises
# #############################################################################


class Test_never_raises1(_HookTestCase):
    """Reporting must never become the driver's problem."""

    def test_a_failing_emitter_is_swallowed(self) -> None:
        """A failing emitter is swallowed."""
        self.install(_Broken())
        self.fail_main(ValueError("x"))
        self.previous.assert_called_once()

    def test_a_failing_report_is_swallowed(self) -> None:
        """A failing report is swallowed."""
        self.install()
        with mock.patch.object(
            cepysdri.cemit, "error_detail", side_effect=RuntimeError("boom")
        ):
            self.fail_main(ValueError("x"))
        self.previous.assert_called_once()


# #############################################################################
# Test_config
# #############################################################################


class Test_config1(_HookTestCase):
    """Without an emitter, the environment decides."""

    def test_missing_config_hooks_nothing(self) -> None:
        """Missing config hooks nothing."""
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(cepyspar.install())
        self.assertIs(sys.excepthook, self.previous)
        self.assertIs(threading.excepthook, self.previous_threading)

    def test_disabled_hooks_nothing(self) -> None:
        """Disabled hooks nothing."""
        env = {"CONVALESCE_INGEST_KEY": "k", "CONVALESCE_ENABLED": "false"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertFalse(cepyspar.install())
        self.assertIs(sys.excepthook, self.previous)

    def test_configured_sends_through_the_environment(self) -> None:
        """Configured sends through the environment."""
        env = {"CONVALESCE_INGEST_KEY": "k", "CONVALESCE_DRY_RUN": "true"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(cepyspar.install())
        with mock.patch.object(cepysdri.cemit, "send_one") as send_one:
            self.fail_main(ValueError("x"))
        emitter = send_one.call_args.kwargs["emitter"]
        self.assertTrue(emitter.config.dry_run)


# #############################################################################
# Test_pth
# #############################################################################


class Test_pth1(unittest.TestCase):
    """The `.pth` line site-packages runs at interpreter start."""

    def _run(self, env: Dict[str, str], install: Any) -> None:
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch.object(cepyspar, "install", install),
        ):
            exec(_PTH, {})  # pylint: disable=exec-used

    def test_off_by_default(self) -> None:
        """Off by default."""
        install = mock.Mock()
        self._run({}, install)
        install.assert_not_called()

    def test_on_when_asked(self) -> None:
        """On when asked."""
        install = mock.Mock()
        self._run({"CONVALESCE_PYSPARK_DRIVER_HOOK": "true"}, install)
        install.assert_called_once_with()

    def test_a_failing_install_does_not_escape(self) -> None:
        """A failing install does not escape."""
        install = mock.Mock(side_effect=RuntimeError("boom"))
        self._run({"CONVALESCE_PYSPARK_DRIVER_HOOK": "true"}, install)
        install.assert_called_once_with()

    def test_is_one_line_starting_with_import(self) -> None:
        """Is one line starting with import."""
        # site-packages only executes a `.pth` line that starts `import`.
        lines = [line for line in _PTH.splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("import "))

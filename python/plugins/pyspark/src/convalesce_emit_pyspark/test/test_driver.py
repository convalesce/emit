"""
Tests for the driver hooks.

Run with `make test`.
"""

import hashlib
import logging
import os
import pathlib
import shutil
import sys
import tempfile
import threading
import types
import unittest
from typing import Any, Dict, List, Optional
from unittest import mock

import convalesce_emit_pyspark as cepyspar
import convalesce_emit_pyspark.driver as cepysdri

# The recording emitter mirrors the Airflow OpenLineage test's own.
# pylint: disable=duplicate-code

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

    def test_an_exit_while_handling_an_error_reports_that_error(self) -> None:
        """An exit while handling an error reports that error."""
        self.install()
        with self.assertRaises(SystemExit):
            try:
                raise LookupError("table orders is not there")
            except LookupError:
                # As a platform's launcher does with the script it ran.
                sys.exit(1)
        hook = cepysdri._INSTALLED  # pylint: disable=protected-access
        assert hook is not None
        hook.at_exit()
        detail = self.recorder.sent[0]["payload"]["error_detail"]
        self.assertEqual(detail["type"], "builtins.LookupError")
        self.assertEqual(detail["message"], "table orders is not there")
        self.assertIn("raise LookupError", detail["traceback"])

    def test_a_zero_exit_is_not(self) -> None:
        """A zero exit is not."""
        self.install()
        for code in (0, None):
            with self.assertRaises(SystemExit):
                sys.exit(code)
        hook = cepysdri._INSTALLED  # pylint: disable=protected-access
        assert hook is not None
        hook.at_exit()
        events = [sent["event"] for sent in self.recorder.sent]
        self.assertNotIn("driver_failure", events)


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

    def test_a_yarn_container_names_the_application_before_any_session(
        self,
    ) -> None:
        """A yarn container names the application before any session."""
        self.install()
        container = "container_e12_1791409434978_0014_01_000001"
        with (
            mock.patch.dict(sys.modules, {"pyspark": _pyspark(None)}),
            mock.patch.dict(os.environ, {"CONTAINER_ID": container}),
        ):
            self.fail_main(RuntimeError("no session yet"))
        payload = self.recorder.sent[-1]["payload"]
        self.assertEqual(self.recorder.sent[-1]["event"], "driver_failure")
        self.assertEqual(
            payload["application_id"], "application_1791409434978_0014"
        )
        self.assertIsNone(payload["application_name"])

    def test_a_yarn_container_numbers_the_attempt(self) -> None:
        """A yarn container numbers the attempt."""
        self.install()
        container = "container_1791409434978_0014_02_000001"
        with mock.patch.dict(os.environ, {"CONTAINER_ID": container}):
            self.fail_main(RuntimeError("the second try failed too"))
            hook = cepysdri._INSTALLED  # pylint: disable=protected-access
            assert hook is not None
            hook.at_exit()
        failure, script = (sent["payload"] for sent in self.recorder.sent)
        self.assertEqual(failure["attempt"], 2)
        self.assertEqual(script["attempt"], 2)

    def test_off_yarn_and_databricks_neither_is_said(self) -> None:
        """Off yarn and databricks neither is said."""
        self.install()
        self.fail_main(ValueError("x"))
        payload = self.recorder.sent[0]["payload"]
        self.assertNotIn("attempt", payload)
        self.assertNotIn("databricks", payload)

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

    def test_a_yarn_driver_with_no_session_is_a_run_this_opens_and_closes(
        self,
    ) -> None:
        """A yarn driver with no session is a run this opens and closes."""
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        script = os.path.join(folder, "nightly.py")
        pathlib.Path(script).write_text("import pyspark\n", encoding="utf-8")
        main = types.ModuleType("__main__")
        setattr(main, "__file__", script)
        container = "container_1791409434978_0014_02_000001"
        with (
            mock.patch.dict(sys.modules, {"pyspark": _pyspark(None)}),
            mock.patch.dict(sys.modules, {"__main__": main}),
            mock.patch.object(sys, "argv", [script]),
            mock.patch.dict(os.environ, {"CONTAINER_ID": container}),
        ):
            self.install()
            hook = cepysdri._INSTALLED  # pylint: disable=protected-access
            assert hook is not None
            self.fail_main(RuntimeError("no session yet"))
            hook.at_exit()
            hook.at_exit()
        self.assertEqual(
            [sent["event"] for sent in self.recorder.sent],
            [
                "SparkListenerApplicationStart",
                "driver_failure",
                "driver_script",
                "SparkListenerApplicationEnd",
            ],
        )
        start, failure, described, end = (
            sent["payload"] for sent in self.recorder.sent
        )
        application = "application_1791409434978_0014"
        self.assertEqual(start["App ID"], application)
        self.assertEqual(start["App Name"], "nightly.py")
        self.assertEqual(start["attempt"], 2)
        self.assertLessEqual(start["Timestamp"], end["Timestamp"])
        self.assertEqual(failure["application_id"], application)
        self.assertEqual(failure["application_name"], "nightly.py")
        self.assertEqual(described["source"]["file"], script)
        self.assertEqual(end["App ID"], application)

    def test_a_yarn_driver_that_had_a_context_is_the_listeners(self) -> None:
        """A yarn driver that had a context is the listener's."""
        container = "container_1791409434978_0014_01_000001"
        gone = _pyspark(None)
        # PySpark keeps its gateway to the JVM after the context has gone.
        setattr(getattr(gone, "SparkContext"), "_gateway", object())
        for module in (_pyspark(_Context()), gone):
            with self.subTest(context=module is not gone):
                self.recorder.sent.clear()
                cepysdri.uninstall()
                with (
                    mock.patch.dict(sys.modules, {"pyspark": module}),
                    mock.patch.dict(os.environ, {"CONTAINER_ID": container}),
                ):
                    self.install()
                    self.fail_main(RuntimeError("after the session"))
                    # pylint: disable-next=protected-access
                    hook = cepysdri._INSTALLED
                    assert hook is not None
                    hook.at_exit()
                events = [sent["event"] for sent in self.recorder.sent]
                self.assertNotIn("SparkListenerApplicationStart", events)
                self.assertNotIn("SparkListenerApplicationEnd", events)
                self.assertIn("driver_failure", events)

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
# Test_script
# #############################################################################

_SCRIPT = """import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.read.jdbc("jdbc:postgresql://etl:s3cret@db/shop", sys.argv[1])
"""


class _Stoppable:
    """Stands in for a SparkContext the driver can stop."""

    appName = "daily_revenue"
    _active_spark_context: Optional["_Stoppable"] = None
    stopped = 0

    @property
    def applicationId(self) -> str:  # pylint: disable=invalid-name
        """What PySpark reads off the JVM, until the context stops."""
        if type(self)._active_spark_context is None:
            raise RuntimeError("stopped")
        return "local-1790387079999"

    def stop(self) -> None:
        """Stop, after which the context names no application."""
        type(self).stopped += 1
        type(self)._active_spark_context = None


class Test_script1(_HookTestCase):
    """What the driver ran, sent as it exits whether or not it failed."""

    def setUp(self) -> None:
        super().setUp()
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        self.script = os.path.join(folder, "revenue.py")
        pathlib.Path(self.script).write_text(_SCRIPT, encoding="utf-8")
        main = types.ModuleType("__main__")
        setattr(main, "__file__", self.script)
        patches: List[Any] = [
            mock.patch.dict(sys.modules, {"__main__": main}),
            mock.patch.object(sys, "argv", [self.script, "shop.orders"]),
            mock.patch.dict(os.environ, {}, clear=False),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        os.environ.pop("CONVALESCE_SEND_SOURCE", None)
        os.environ.pop("CONVALESCE_SEND_ARGUMENTS", None)

    def at_exit(self) -> None:
        """Run what the interpreter runs as it exits."""
        hook = cepysdri._INSTALLED  # pylint: disable=protected-access
        assert hook is not None
        hook.at_exit()

    def test_a_driver_that_succeeds_sends_its_script(self) -> None:
        """A driver that succeeds sends its script."""
        self.install()
        self.at_exit()
        self.assertEqual(len(self.recorder.sent), 1)
        sent = self.recorder.sent[0]
        self.assertEqual(sent["tool"], "spark")
        self.assertEqual(sent["event"], "driver_script")
        self.assertEqual(sent["tool_version"], "3.5.3")
        payload = sent["payload"]
        self.assertEqual(payload["application_id"], "local-1790387078840")
        self.assertEqual(payload["application_name"], "customer_features_broken")
        self.assertEqual(payload["argv"], [self.script, "shop.orders"])
        source = payload["source"]
        self.assertEqual(source["file"], self.script)
        self.assertEqual(source["language"], "python")
        self.assertFalse(source["truncated"])
        self.assertIn("spark.read.jdbc", source["text"])
        self.assertEqual(self.recorder.flushes, 1)

    def test_the_hash_is_of_the_file_and_the_text_is_masked(self) -> None:
        """The hash is of the file and the text is masked."""
        self.install()
        self.at_exit()
        sent = self.recorder.sent[0]
        source = sent["payload"]["source"]
        self.assertNotIn("s3cret", source["text"])
        self.assertIn("postgresql://etl:***@db/shop", source["text"])
        self.assertEqual(
            source["sha256"], hashlib.sha256(_SCRIPT.encode("utf-8")).hexdigest()
        )
        self.assertIn("source.text", [e["path"] for e in sent["excluded"]])

    def test_sends_once(self) -> None:
        """Sends once."""
        self.install()
        self.at_exit()
        self.at_exit()
        self.assertEqual(len(self.recorder.sent), 1)

    def test_a_failing_driver_sends_both(self) -> None:
        """A failing driver sends both."""
        self.install()
        self.fail_main(ValueError("x"))
        self.at_exit()
        self.assertEqual(
            [sent["event"] for sent in self.recorder.sent],
            ["driver_failure", "driver_script"],
        )

    def test_a_long_script_is_cut_from_the_end(self) -> None:
        """A long script is cut from the end."""
        text = "# head\n" + "x = 1\n" * 20_000
        pathlib.Path(self.script).write_text(text, encoding="utf-8")
        self.install()
        self.at_exit()
        source = self.recorder.sent[0]["payload"]["source"]
        self.assertTrue(source["truncated"])
        self.assertEqual(len(source["text"]), cepysdri.cesource.MAX_CHARS)
        self.assertTrue(source["text"].startswith("# head"))
        self.assertEqual(
            source["sha256"], hashlib.sha256(text.encode("utf-8")).hexdigest()
        )

    def test_source_can_be_switched_off(self) -> None:
        """Source can be switched off."""
        for value in ("false", "0", "no", "off"):
            with self.subTest(value=value):
                self.recorder.sent.clear()
                cepysdri.uninstall()
                os.environ["CONVALESCE_SEND_SOURCE"] = value
                self.install()
                self.at_exit()
                payload = self.recorder.sent[0]["payload"]
                self.assertNotIn("source", payload)
                self.assertEqual(payload["argv"], [self.script, "shop.orders"])

    def test_arguments_can_be_switched_off(self) -> None:
        """Arguments can be switched off."""
        os.environ["CONVALESCE_SEND_ARGUMENTS"] = "false"
        self.install()
        self.fail_main(ValueError("x"))
        self.at_exit()
        failure, script = (sent["payload"] for sent in self.recorder.sent)
        self.assertEqual(failure["argv"], [self.script])
        self.assertEqual(script["argv"], [self.script])
        self.assertIn("source", script)

    def test_arguments_can_be_switched_off_under_a_platforms_prefix(
        self,
    ) -> None:
        """Arguments can be switched off under a platform's prefix."""
        os.environ.pop("CONVALESCE_SEND_ARGUMENTS", None)
        with mock.patch.dict(
            os.environ, {"CUSTOMER_CONVALESCE_SEND_ARGUMENTS": "false"}
        ):
            self.install()
            self.fail_main(ValueError("x"))
        self.assertEqual(self.recorder.sent[0]["payload"]["argv"], [self.script])

    def test_a_script_a_launcher_ran_is_the_source(self) -> None:
        """A script a launcher ran is the source."""
        launcher = os.path.join(os.path.dirname(self.script), "runscript.py")
        pathlib.Path(launcher).write_text(
            "# the platform's launcher\n", encoding="utf-8"
        )
        setattr(sys.modules["__main__"], "__file__", launcher)
        scope = {
            "__name__": "__main__",
            "__file__": self.script,
            "emitter": self.recorder,
        }
        exec(  # pylint: disable=exec-used
            compile(
                "import convalesce_emit_pyspark\n"
                "convalesce_emit_pyspark.install(emitter=emitter)\n",
                self.script,
                "exec",
            ),
            scope,
        )
        self.at_exit()
        self.assertEqual(
            self.recorder.sent[0]["payload"]["source"]["file"], self.script
        )

    def test_a_script_a_launcher_ran_is_learnt_from_what_it_imports(
        self,
    ) -> None:
        """A script a launcher ran is learnt from what it imports."""
        launcher = os.path.join(os.path.dirname(self.script), "runscript.py")
        pathlib.Path(launcher).write_text("# the launcher\n", encoding="utf-8")
        setattr(sys.modules["__main__"], "__file__", launcher)
        self.install()
        kind = cepysdri._Finder  # pylint: disable=protected-access
        finder = next(f for f in sys.meta_path if isinstance(f, kind))
        scope = {
            "__name__": "__main__",
            "__file__": self.script,
            "finder": finder,
        }
        exec(  # pylint: disable=exec-used
            compile("finder.find_spec('decimal', None)\n", self.script, "exec"),
            scope,
        )
        self.at_exit()
        self.assertEqual(
            self.recorder.sent[0]["payload"]["source"]["file"], self.script
        )

    def test_a_script_a_launcher_ran_is_learnt_from_the_error_it_caught(
        self,
    ) -> None:
        """A script a launcher ran is learnt from the error it caught."""
        launcher = os.path.join(os.path.dirname(self.script), "runscript.py")
        pathlib.Path(launcher).write_text("# the launcher\n", encoding="utf-8")
        setattr(sys.modules["__main__"], "__file__", launcher)
        self.install()
        scope = {"__name__": "__main__", "__file__": self.script}
        with self.assertRaises(SystemExit):
            try:
                exec(  # pylint: disable=exec-used
                    compile(
                        "raise LookupError('no table')\n", self.script, "exec"
                    ),
                    scope,
                )
            except LookupError:
                sys.exit(1)
        self.at_exit()
        events = {sent["event"]: sent["payload"] for sent in self.recorder.sent}
        self.assertEqual(events["driver_script"]["source"]["file"], self.script)
        self.assertEqual(
            events["driver_failure"]["error_detail"]["type"],
            "builtins.LookupError",
        )

    def install_at_start(self) -> None:
        """
        Install as the `.pth` does, before anything runs as `__main__`.

        A new thread's stack has no main program on it, as the interpreter's
        has not while it is still starting.
        """
        thread = threading.Thread(target=self.install)
        thread.start()
        thread.join()

    def launch(self, body: str) -> str:
        """
        Run this test's script the way AWS Glue's launcher runs a job.

        :param body: what the script does, after one import of its own
        :return: the launcher's file, which is the process's `__main__`
        """
        launcher = os.path.join(os.path.dirname(self.script), "runscript.py")
        pathlib.Path(launcher).write_text(
            "import runpy\n"
            "import sys\n"
            "try:\n"
            f"    runpy.run_path({self.script!r}, run_name='__main__')\n"
            "except Exception:\n"
            "    sys.exit(1)\n",
            encoding="utf-8",
        )
        pathlib.Path(self.script).write_text(
            "import sys\n"
            "sys.modules.pop('colorsys', None)\n"
            "import colorsys\n" + body,
            encoding="utf-8",
        )
        main = sys.modules["__main__"]
        setattr(main, "__file__", launcher)
        with mock.patch.object(sys, "argv", [launcher]):
            self.install_at_start()
            code = compile(
                pathlib.Path(launcher).read_text(encoding="utf-8"),
                launcher,
                "exec",
            )
            exec(code, main.__dict__)  # pylint: disable=exec-used
        return launcher

    def test_a_script_run_with_runpy_is_the_source_not_its_launcher(
        self,
    ) -> None:
        """A script run with runpy is the source, not its launcher."""
        launcher = self.launch("answer = 42\n")
        main_file = cepysdri._MAIN_FILE  # pylint: disable=protected-access
        self.assertEqual(main_file, launcher)
        self.at_exit()
        source = self.recorder.sent[0]["payload"]["source"]
        self.assertEqual(source["file"], self.script)
        self.assertIn("answer = 42", source["text"])

    def test_a_script_run_with_runpy_that_fails_is_the_source(self) -> None:
        """A script run with runpy that fails is the source."""
        with self.assertRaises(SystemExit):
            self.launch("raise LookupError('no table')\n")
        self.at_exit()
        events = {sent["event"]: sent["payload"] for sent in self.recorder.sent}
        self.assertEqual(events["driver_script"]["source"]["file"], self.script)
        self.assertEqual(
            events["driver_failure"]["error_detail"]["type"],
            "builtins.LookupError",
        )

    def test_the_main_file_is_the_outermost_main_program(self) -> None:
        """The main file is the outermost main program."""
        launcher = os.path.join(os.path.dirname(self.script), "runscript.py")
        pathlib.Path(launcher).write_text("# the launcher\n", encoding="utf-8")
        code = types.SimpleNamespace(co_filename="<frozen runpy>")
        outer = types.SimpleNamespace(
            f_globals={"__name__": "__main__", "__file__": launcher},
            f_code=code,
            f_back=None,
        )
        between = types.SimpleNamespace(
            f_globals={"__name__": "runpy"}, f_code=code, f_back=outer
        )
        inner = types.SimpleNamespace(
            f_globals={"__name__": "__main__", "__file__": self.script},
            f_code=code,
            f_back=between,
        )
        # pylint: disable=protected-access
        self.assertEqual(cepysdri._main_file(inner), launcher)
        # With nothing running as the main program, `__main__` says.
        self.assertEqual(cepysdri._main_file(between.f_back.f_back), self.script)

    def test_a_module_that_installs_is_not_taken_for_the_script(self) -> None:
        """A module that installs is not taken for the script."""
        self.install()
        self.at_exit()
        self.assertEqual(
            self.recorder.sent[0]["payload"]["source"]["file"], self.script
        )
        caller = cepysdri._CALLER_SCRIPT  # pylint: disable=protected-access
        self.assertIsNone(caller)

    def test_a_launcher_module_has_no_source(self) -> None:
        """A launcher module has no source."""
        main = sys.modules["__main__"]
        setattr(main, "__spec__", types.SimpleNamespace(name="kernel_launcher"))
        self.install()
        self.at_exit()
        self.assertNotIn("source", self.recorder.sent[0]["payload"])

    def test_a_script_that_cannot_be_read_has_no_source(self) -> None:
        """A script that cannot be read has no source."""
        os.remove(self.script)
        self.install()
        self.at_exit()
        self.assertNotIn("source", self.recorder.sent[0]["payload"])

    def test_no_application_sends_nothing(self) -> None:
        """No application sends nothing."""
        with mock.patch.dict(sys.modules, {"pyspark": _pyspark(None)}):
            self.install()
            self.at_exit()
        self.assertEqual(self.recorder.sent, [])

    def test_a_failing_emitter_is_swallowed(self) -> None:
        """A failing emitter is swallowed."""
        self.install(_Broken())
        self.at_exit()


# #############################################################################
# Test_stop
# #############################################################################


class Test_stop1(_HookTestCase):
    """A driver that stops its session before it exits."""

    # The stand-in keeps PySpark's own names, and the finder is the hook's.
    # pylint: disable=protected-access

    def setUp(self) -> None:
        super().setUp()
        self.context = _Stoppable()
        _Stoppable._active_spark_context = self.context
        _Stoppable.stopped = 0
        self.original_stop = _Stoppable.stop
        self.module = types.ModuleType("pyspark")
        setattr(self.module, "__version__", "3.5.3")
        patch = mock.patch.dict(sys.modules, {"pyspark": self.module})
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(setattr, _Stoppable, "stop", self.original_stop)

    def test_the_application_is_read_as_the_context_stops(self) -> None:
        """The application is read as the context stops."""
        setattr(self.module, "SparkContext", _Stoppable)
        self.install()
        self.context.stop()
        self.assertEqual(_Stoppable.stopped, 1)
        hook = cepysdri._INSTALLED  # pylint: disable=protected-access
        assert hook is not None
        hook.at_exit()
        payload = self.recorder.sent[0]["payload"]
        self.assertEqual(payload["application_id"], "local-1790387079999")
        self.assertEqual(payload["application_name"], "daily_revenue")

    def test_a_failure_after_the_stop_names_the_application(self) -> None:
        """A failure after the stop names the application."""
        setattr(self.module, "SparkContext", _Stoppable)
        self.install()
        self.context.stop()
        self.fail_main(ValueError("after the stop"))
        payload = self.recorder.sent[0]["payload"]
        self.assertEqual(payload["application_id"], "local-1790387079999")

    def test_hooked_before_pyspark_is_imported(self) -> None:
        """Hooked before pyspark is imported."""
        self.install()
        finder = sys.meta_path[0]
        self.assertIsInstance(finder, cepysdri._Finder)
        # Something else being imported changes nothing.
        self.assertIsNone(finder.find_spec("json.decoder", None))
        self.assertIn(finder, sys.meta_path)
        # PySpark's own imports, once its context class exists, wrap it.
        setattr(self.module, "SparkContext", _Stoppable)
        self.assertIsNone(finder.find_spec("pyspark.sql", None))
        # It stays a while longer, in case a launcher's script is yet to
        # import something, and leaves once it has looked enough.
        self.assertIn(finder, sys.meta_path)
        with mock.patch.object(cepysdri, "_MAX_LOOKS", 0):
            self.assertIsNone(finder.find_spec("json.encoder", None))
        self.assertNotIn(finder, sys.meta_path)
        self.context.stop()
        hook = cepysdri._INSTALLED  # pylint: disable=protected-access
        assert hook is not None
        self.assertEqual(hook.context()[0], "local-1790387079999")

    def test_uninstall_puts_stop_back(self) -> None:
        """Uninstall puts stop back."""
        setattr(self.module, "SparkContext", _Stoppable)
        self.install()
        self.assertIsNot(_Stoppable.stop, self.original_stop)
        cepysdri.uninstall()
        self.assertIs(_Stoppable.stop, self.original_stop)
        self.assertFalse(
            [f for f in sys.meta_path if isinstance(f, cepysdri._Finder)]
        )

    def test_a_context_that_cannot_be_read_still_stops(self) -> None:
        """A context that cannot be read still stops."""
        setattr(self.module, "SparkContext", _Stoppable)
        self.install()
        _Stoppable._active_spark_context = None
        self.context.stop()
        self.assertEqual(_Stoppable.stopped, 1)


# #############################################################################
# Test_databricks
# #############################################################################


class _Cluster(_Stoppable):
    """Stands in for the SparkContext of a Databricks classic cluster."""

    local = {
        "spark.databricks.job.runId": "456",
        "spark.databricks.job.id": "123",
        "spark.databricks.notebook.path": "/Repos/etl/orders_nightly",
    }
    settings = {
        "spark.databricks.job.runId": "1",
        "spark.databricks.clusterUsageTags.clusterId": "0921-133320-abcd1234",
        "spark.databricks.clusterUsageTags.clusterOwnerOrgId": "1234567890123456",
        "spark.databricks.workspaceUrl": "my-workspace.cloud.databricks.com",
    }

    def getLocalProperty(  # pylint: disable=invalid-name
        self, name: str
    ) -> Optional[str]:
        """What the thread running the job run's code was given."""
        if type(self)._active_spark_context is None:
            raise RuntimeError("stopped")
        return self.local.get(name)

    def getConf(self) -> Dict[str, str]:  # pylint: disable=invalid-name
        """The cluster's configuration."""
        return self.settings


class Test_databricks1(_HookTestCase):
    """A driver on a Databricks cluster says which job run it is for."""

    # pylint: disable=protected-access

    _SAID = {
        "job_run_id": "456",
        "job_id": "123",
        "notebook_path": "/Repos/etl/orders_nightly",
        "cluster_id": "0921-133320-abcd1234",
        "workspace_id": "1234567890123456",
        "workspace_url": "my-workspace.cloud.databricks.com",
    }

    def setUp(self) -> None:
        super().setUp()
        self.context = _Cluster()
        _Cluster._active_spark_context = self.context
        module = types.ModuleType("pyspark")
        setattr(module, "__version__", "3.5.3")
        setattr(module, "SparkContext", _Cluster)
        patch = mock.patch.dict(sys.modules, {"pyspark": module})
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(setattr, _Cluster, "stop", _Cluster.stop)

    def test_a_failure_names_the_job_run_and_where_it_ran(self) -> None:
        """A failure names the job run and where it ran."""
        self.install()
        self.fail_main(ValueError("no Spark job ran"))
        hook = cepysdri._INSTALLED
        assert hook is not None
        hook.at_exit()
        failure, script = (sent["payload"] for sent in self.recorder.sent)
        # The thread's own run id, not whatever the configuration holds.
        self.assertEqual(failure["databricks"], self._SAID)
        self.assertEqual(script["databricks"], self._SAID)

    def test_what_was_read_before_the_stop_is_kept(self) -> None:
        """What was read before the stop is kept."""
        self.install()
        self.context.stop()
        self.fail_main(ValueError("after the stop"))
        self.assertEqual(
            self.recorder.sent[0]["payload"]["databricks"], self._SAID
        )

    def test_what_was_read_as_the_driver_exited_is_kept(self) -> None:
        """What was read as the driver exited is kept."""
        self.install()
        with self.assertRaises(SystemExit):
            sys.exit(1)
        _Cluster._active_spark_context = None
        hook = cepysdri._INSTALLED
        assert hook is not None
        hook.at_exit()
        self.assertEqual(
            self.recorder.sent[0]["payload"]["databricks"], self._SAID
        )

    def test_a_session_that_cannot_be_asked_changes_nothing(self) -> None:
        """A session that cannot be asked changes nothing."""
        self.install()
        with mock.patch.object(
            cepysdri, "_databricks", side_effect=RuntimeError("gateway gone")
        ):
            self.fail_main(ValueError("x"))
        payload = self.recorder.sent[0]["payload"]
        self.assertNotIn("databricks", payload)
        self.assertEqual(payload["application_id"], "local-1790387079999")


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

    def test_on_when_asked_under_a_platforms_prefix(self) -> None:
        """On when asked under a platform's prefix."""
        install = mock.Mock()
        self._run({"CUSTOMER_CONVALESCE_PYSPARK_DRIVER_HOOK": "true"}, install)
        install.assert_called_once_with()

    def test_the_settings_own_name_wins(self) -> None:
        """The setting's own name wins."""
        install = mock.Mock()
        env = {
            "CONVALESCE_PYSPARK_DRIVER_HOOK": "false",
            "CUSTOMER_CONVALESCE_PYSPARK_DRIVER_HOOK": "true",
        }
        self._run(env, install)
        install.assert_not_called()

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


# #############################################################################
# Test_shell
# #############################################################################


class _Events:
    """Stands in for IPython's event registry."""

    def __init__(self) -> None:
        self.callbacks: Dict[str, List[Any]] = {}

    def register(self, name: str, callback: Any) -> None:
        """Keep a callback."""
        self.callbacks.setdefault(name, []).append(callback)

    def unregister(self, name: str, callback: Any) -> None:
        """Drop a callback."""
        self.callbacks[name].remove(callback)


class _Shell:
    """Stands in for the IPython shell a platform runs a driver inside."""

    def __init__(self) -> None:
        self.events = _Events()

    def run(self, error: Optional[BaseException] = None) -> None:
        """End a cell, as IPython does, with what it raised."""
        result = types.SimpleNamespace(
            error_in_exec=error, error_before_exec=None
        )
        for callback in list(self.events.callbacks.get("post_run_cell", [])):
            callback(result)


class _ConnectSession:
    """Stands in for a Spark Connect session."""

    session_id = "0c067419-ce47-4441-98b0-47ea5a2a3882"
    # What serverless answered when asked: the cluster, and no job run.
    settings = {
        "spark.app.name": "Databricks Shell",
        "spark.databricks.clusterUsageTags.clusterId": "0921-133320-abcd1234",
    }
    conf = types.SimpleNamespace(
        get=lambda key, default=None: _ConnectSession.read(key, default)
    )

    @classmethod
    def read(cls, key: str, default: Optional[str]) -> Optional[str]:
        """A setting, or what Spark Connect raises for one it will not say."""
        if key == "spark.databricks.job.runId":
            raise RuntimeError("[CONFIG_NOT_AVAILABLE]")
        return cls.settings.get(key, default)

    @classmethod
    def getActiveSession(  # pylint: disable=invalid-name
        cls,
    ) -> "_ConnectSession":
        """The session, as PySpark names it."""
        return cls()


class Test_shell1(Test_script1):
    """A driver a shell runs: nothing is uncaught, and the process lives on."""

    def setUp(self) -> None:
        super().setUp()
        self.shell = _Shell()
        ipython = types.ModuleType("IPython")
        setattr(ipython, "get_ipython", lambda: self.shell)
        # As Databricks runs a Python file task: the script is `sys.argv[0]`,
        # and the `__main__` its shell runs it in names no file.
        patch = mock.patch.dict(
            sys.modules,
            {"IPython": ipython, "__main__": types.ModuleType("__main__")},
        )
        patch.start()
        self.addCleanup(patch.stop)

    def notebook(self) -> str:
        """
        Make this a notebook's kernel: its own launcher, and no script.

        :return: the launcher's file
        """
        launcher = os.path.join(
            os.path.dirname(self.script), "db_ipykernel_launcher.py"
        )
        pathlib.Path(launcher).write_text("# the kernel\n", encoding="utf-8")
        setattr(sys.modules["__main__"], "__file__", launcher)
        patch = mock.patch.object(
            sys, "argv", [launcher, "-f", "/kernels/connection.json"]
        )
        patch.start()
        self.addCleanup(patch.stop)
        return launcher

    def over_connect(self) -> None:
        """Give the driver a Connect session and no context, as serverless does."""
        connect = types.ModuleType("pyspark.sql.connect.session")
        setattr(connect, "SparkSession", _ConnectSession)
        patch = mock.patch.dict(
            sys.modules,
            {"pyspark": _pyspark(None), "pyspark.sql.connect.session": connect},
        )
        patch.start()
        self.addCleanup(patch.stop)

    def test_a_cell_that_raises_is_reported_with_the_script(self) -> None:
        """A cell that raises is reported with the script."""
        self.install()
        self.shell.run(_raised(LookupError("table orders is not there")))
        events = [sent["event"] for sent in self.recorder.sent]
        self.assertEqual(events, ["driver_failure", "driver_script"])
        detail = self.recorder.sent[0]["payload"]["error_detail"]
        self.assertEqual(detail["type"], "builtins.LookupError")

    def test_a_session_over_connect_is_a_run_this_opens_and_closes(self) -> None:
        """A session over connect is a run this opens and closes."""
        self.over_connect()
        self.install()
        self.shell.run(_raised(LookupError("table orders is not there")))
        self.shell.run()
        events = [sent["event"] for sent in self.recorder.sent]
        self.assertEqual(
            events,
            [
                "SparkListenerApplicationStart",
                "driver_failure",
                "driver_script",
                "SparkListenerApplicationEnd",
            ],
        )
        start, failure = (
            self.recorder.sent[0]["payload"],
            self.recorder.sent[1]["payload"],
        )
        self.assertEqual(start["App ID"], _ConnectSession.session_id)
        self.assertEqual(start["App Name"], "revenue")
        self.assertEqual(failure["application_id"], _ConnectSession.session_id)
        # Only what the session would say of Databricks, on all three.
        cluster = {"cluster_id": "0921-133320-abcd1234"}
        for sent in self.recorder.sent[:3]:
            self.assertEqual(sent["payload"]["databricks"], cluster)
        self.assertNotIn("databricks", self.recorder.sent[3]["payload"])
        self.assertEqual(
            self.recorder.sent[3]["payload"]["App ID"], start["App ID"]
        )

    def test_a_launcher_module_has_no_source(self) -> None:
        """A kernel started as a module, with no script, sends no script."""
        main = sys.modules["__main__"]
        setattr(main, "__spec__", types.SimpleNamespace(name="kernel_launcher"))
        launcher = os.path.join(
            os.path.dirname(self.script), "ipykernel_launcher.py"
        )
        pathlib.Path(launcher).write_text("# the kernel\n", encoding="utf-8")
        with mock.patch.object(sys, "argv", [launcher]):
            self.install()
            self.at_exit()
        self.assertEqual(self.recorder.sent, [])

    def test_a_script_that_cannot_be_read_has_no_source(self) -> None:
        """With no file to name, a shell is a notebook and sends no script."""
        os.remove(self.script)
        self.install()
        self.at_exit()
        self.assertEqual(self.recorder.sent, [])

    def test_the_main_file_is_the_outermost_main_program(self) -> None:
        """In a shell only `__main__` says what the process was started with."""
        frame = types.SimpleNamespace(
            f_globals={"__name__": "__main__", "__file__": self.script},
            f_code=types.SimpleNamespace(co_filename=self.script),
            f_back=None,
        )
        # pylint: disable=protected-access
        # Not the script a platform put in `sys.argv[0]` and runs as a cell.
        self.assertIsNone(cepysdri._main_file(frame))
        launcher = self.notebook()
        self.assertEqual(cepysdri._main_file(frame), launcher)

    def test_a_launcher_of_any_name_is_not_the_script(self) -> None:
        """A launcher of any name is not the script."""
        launcher = os.path.join(os.path.dirname(self.script), "start_shell.py")
        pathlib.Path(launcher).write_text("# a kernel\n", encoding="utf-8")
        setattr(sys.modules["__main__"], "__file__", launcher)
        with mock.patch.object(sys, "argv", [launcher]):
            self.install()
            self.shell.run()
            self.at_exit()
        self.assertEqual(self.recorder.sent, [])

    def test_a_notebook_that_passes_sends_nothing(self) -> None:
        """A notebook that passes sends nothing."""
        self.notebook()
        self.over_connect()
        self.install()
        self.shell.run()
        self.shell.run()
        self.at_exit()
        self.assertEqual(self.recorder.sent, [])

    def test_a_notebook_cell_that_raises_is_a_run_with_no_script(self) -> None:
        """A notebook cell that raises is a run with no script."""
        self.notebook()
        self.over_connect()
        self.install()
        # The cell that installs passes, and ends nothing.
        self.shell.run()
        self.shell.run(_raised(LookupError("table orders is not there")))
        self.shell.run()
        self.at_exit()
        self.assertEqual(
            [sent["event"] for sent in self.recorder.sent],
            [
                "SparkListenerApplicationStart",
                "driver_failure",
                "SparkListenerApplicationEnd",
            ],
        )
        start, failure, _ = (sent["payload"] for sent in self.recorder.sent)
        # Named by the session, never after the kernel's launcher.
        self.assertEqual(start["App Name"], "Databricks Shell")
        self.assertEqual(failure["application_name"], "Databricks Shell")
        self.assertEqual(
            failure["error_detail"]["message"], "table orders is not there"
        )

    def test_a_notebook_is_named_by_its_path(self) -> None:
        """A notebook is named by its path."""
        self.notebook()
        self.over_connect()
        path = {"spark.databricks.notebook.path": "/Repos/etl/orders_nightly"}
        with mock.patch.dict(_ConnectSession.settings, path):
            self.install()
            self.shell.run(_raised(LookupError("table orders is not there")))
        start, failure, _ = (sent["payload"] for sent in self.recorder.sent)
        self.assertEqual(start["App Name"], "orders_nightly")
        self.assertEqual(failure["application_name"], "orders_nightly")
        self.assertEqual(
            failure["databricks"]["notebook_path"], "/Repos/etl/orders_nightly"
        )

    def test_a_notebook_beside_a_listener_reports_its_failure_only(
        self,
    ) -> None:
        """A notebook beside a listener reports its failure only."""
        self.notebook()
        self.install()
        self.shell.run()
        self.shell.run(_raised(LookupError("table orders is not there")))
        self.at_exit()
        events = [sent["event"] for sent in self.recorder.sent]
        self.assertEqual(events, ["driver_failure"])

    def test_a_listener_s_application_is_not_opened_again(self) -> None:
        """A listener s application is not opened again."""
        self.install()
        self.shell.run()
        events = [sent["event"] for sent in self.recorder.sent]
        self.assertEqual(events, ["driver_script"])

    def test_uninstall_leaves_the_shell(self) -> None:
        """Uninstall leaves the shell."""
        self.install()
        cepysdri.uninstall()
        self.assertEqual(self.shell.events.callbacks.get("post_run_cell"), [])

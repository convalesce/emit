"""
Report a PySpark driver: what it ran, and whether it failed in Python.

A driver can fail before Spark runs anything: it reads a table that is not
there, and the exception ends the Python process. The JVM still ends the
application normally -- its end event has no exit code and OpenLineage says
COMPLETE -- so nothing Spark itself sends says the run failed. This hooks
the driver's own interpreter instead and sends one `driver_failure`
observation for the application, which a receiver then marks failed.

Three ways out of a driver are seen: an uncaught exception in the main
thread (`sys.excepthook`), one in another thread (`threading.excepthook`),
and `sys.exit()` with a non-zero code (a wrapper around `sys.exit`, read at
interpreter exit). At most one observation is sent per process, whichever
comes first, and it is flushed synchronously before the process goes on to
exit. Nothing here raises into the driver or changes its exit code.

Whether it failed or not, a driver is also described once, as it exits, by
one `driver_script` observation: the arguments it was called with and the
text of the script it ran. A repository says what the script was meant to
be; what ran is the file on the machine that ran it. Arguments are sent
unless `CONVALESCE_SEND_ARGUMENTS` is false, and the text unless
`CONVALESCE_SEND_SOURCE` is.

Where the platform says which of its runs the driver is, both say so too:
on Databricks the job run, the task's run, the job, notebook, cluster and
workspace (`databricks`), and on YARN which try of the application it is
(`attempt`). Databricks says them in the session's configuration, and where
that will not answer (serverless) in the run's context, which the shell's
`dbutils` hands out: a fixed list of its attributes is read by name
(`_CONTEXT`), and nothing else of it is kept, logged or sent.

A driver usually stops its session before it exits, and a stopped session
no longer says which application it was. So `SparkContext.stop` is wrapped
to read the application's id and name first, and nothing else about it
changes.

Import as:

import convalesce_emit_pyspark.driver as cepysdri
"""

import atexit
import functools
import hashlib
import json
import logging
import os
import platform
import re
import sys
import threading
import time
import types
from typing import Any, Callable, Dict, List, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit.source as cesource
import convalesce_emit_pyspark._env as cepysenv

_LOG = logging.getLogger(__name__)

TOOL = "spark"
EVENT = "driver_failure"
SCRIPT_EVENT = "driver_script"
# Spark's own events for an application's start and end, which this sends
# itself only where no listener can (`_Hook.open`).
APPLICATION_START = "SparkListenerApplicationStart"
APPLICATION_END = "SparkListenerApplicationEnd"

_PYSPARK = "pyspark"
_CONNECT = "pyspark.sql.connect.session"
_IPYTHON = "IPython"
_CELL_EVENT = "post_run_cell"
# Set by Databricks on serverless compute.
_SERVERLESS_ENV = "IS_SERVERLESS"
# What YARN names a container, which carries the application it belongs to.
_YARN_CONTAINER_ENV = "CONTAINER_ID"
# How far up the stack an import is followed to the script that made it.
_MAX_FRAMES = 64
# How many imports are watched for the script before giving up.
_MAX_LOOKS = 2000
_YARN_CONTAINER = re.compile(r"^container_(?:e\d+_)?(\d+)_(\d+)_(\d+)_\d+$")
# What a notebook kernel's own launcher file is called, by its file name.
_KERNEL_LAUNCHER = re.compile(
    r"ipykernel|ipython[-_.\d]*$|ipython.*launcher|kernel[-_]?launcher",
    re.IGNORECASE,
)
_KERNEL_PACKAGES = frozenset({"ipykernel", "ipython"})
# What Databricks says of the job run a driver is for and of where it runs,
# by the field each is sent as. A job run's id is a Spark job's local
# property on a classic cluster; the rest is the cluster's configuration.
_DATABRICKS = {
    "job_run_id": "spark.databricks.job.runId",
    "job_id": "spark.databricks.job.id",
    "notebook_path": "spark.databricks.notebook.path",
    "cluster_id": "spark.databricks.clusterUsageTags.clusterId",
    "workspace_id": "spark.databricks.clusterUsageTags.clusterOwnerOrgId",
    "workspace_url": "spark.databricks.workspaceUrl",
}
# What the context of a Databricks run says of the same, by the field each is
# sent as and the attribute Databricks names it. `currentRunId` is the run of
# the task, which Databricks numbers again each time it retries the task;
# `multitaskParentRunId` is the job run the task belongs to.
_CONTEXT = {
    "job_run_id": "multitaskParentRunId",
    "task_run_id": "currentRunId",
    "job_id": "jobId",
    "job_name": "jobName",
    "notebook_path": "notebook_path",
    "cluster_id": "clusterId",
    "workspace_id": "orgId",
}
_CONTEXT_TASK_TYPE = "jobTaskType"
_NOTEBOOK_TASK = "notebook"
# What a session nothing names is called, so a run always has a name.
_NOTEBOOK_NAME = "notebook"
_SESSION_NAME = "pyspark"
_DBUTILS = "dbutils"
_ARGUMENTS_ENV = "CONVALESCE_SEND_ARGUMENTS"
_FALSY = frozenset({"0", "false", "no", "off"})

_Context = Tuple[Optional[str], Optional[str]]

_INSTALLED: Optional["_Hook"] = None
# The script that called `install()` as the main program, when not `__main__`'s file.
_CALLER_SCRIPT: Optional[str] = None
# The file the process itself was started with, read once as the hooks go in.
# A launcher that runs the job's script as `__main__` (AWS Glue's) swaps
# `sys.modules["__main__"]` and `sys.argv[0]` for the script's while it runs,
# so neither says which file is the launcher's by the time it is asked.
_MAIN_FILE: Optional[str] = None
_INSTALL_LOCK = threading.Lock()


# #############################################################################
# install
# #############################################################################


def install(emitter: Optional[cemit.EmitterLike] = None) -> bool:
    """
    Hook this interpreter so a failing driver is reported.

    Safe to call more than once: only the first call hooks anything. Does
    nothing when no emitter is given and the environment does not configure
    one (`CONVALESCE_INGEST_KEY` unset, or `CONVALESCE_ENABLED=false`).

    :param emitter: emitter to send through; built from the environment
        when a failure is reported, when not given
    :return: whether the hooks are in place
    """
    global _INSTALLED, _CALLER_SCRIPT, _MAIN_FILE
    try:
        with _INSTALL_LOCK:
            if _INSTALLED is not None:
                return True
            caller = sys._getframe(1)  # pylint: disable=protected-access
            _MAIN_FILE = _main_file(caller)
            _CALLER_SCRIPT = _caller_script(caller)
            config = None
            if emitter is None:
                config = _config()
                if config is None:
                    return False
            hook = _Hook(emitter, config)
            hook.attach()
            _INSTALLED = hook
            return True
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: could not hook the driver: %s", exc)
        return False


def uninstall() -> None:
    """
    Put back what `install` replaced, where nothing has replaced it since.

    :return: nothing
    """
    global _INSTALLED, _CALLER_SCRIPT, _MAIN_FILE
    with _INSTALL_LOCK:
        if _INSTALLED is not None:
            _INSTALLED.detach()
            _INSTALLED = None
        _CALLER_SCRIPT = None
        _MAIN_FILE = None


def _config() -> Optional[cemit.Config]:
    """
    The emit configuration, if the environment gives a usable one.

    :return: the configuration, or None when it is missing, invalid or off
    """
    try:
        config = cemit.Config.from_env()
        config.validate()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.debug("convalesce: driver hook not configured: %s", exc)
        return None
    return config if config.enabled else None


# #############################################################################
# _Hook
# #############################################################################


class _Hook:
    """
    The installed hooks, and what they replaced.

    :param emitter: emitter to send through, or None to build one
    :param config: configuration to build one from, when not given
    """

    def __init__(
        self,
        emitter: Optional[cemit.EmitterLike],
        config: Optional[cemit.Config],
    ) -> None:
        self._emitter = emitter
        self._config = config
        self._lock = threading.Lock()
        self._sent = False
        self._script_sent = False
        # The application a stopped context was, read as it stopped.
        self._stopped: _Context = (None, None)
        # Whether this process had a Spark context at any point: its JVM's
        # listener then reports the application, and this never does.
        self._had_context = False
        # What Databricks said of the run, kept from when it could be read.
        self._databricks: Dict[str, str] = {}
        # What the run's context said of it, once it has been asked.
        self._run_context: Optional[Dict[str, str]] = None
        self._context_class: Any = None
        self._original_stop: Any = None
        self._finder: Optional["_Finder"] = None
        self._exit_code: Any = None
        self._exit_context: _Context = (None, None)
        self._exit_cause: Optional[BaseException] = None
        self._shell: Any = None
        self._attached_ms = int(time.time() * 1000)
        # A Spark Connect session's run, which this opens and closes itself.
        self._opened = False
        self._closed = False
        self._previous_excepthook: Callable[..., Any] = sys.excepthook
        self._previous_threading: Callable[..., Any] = threading.excepthook
        self._original_exit: Callable[..., Any] = sys.exit

    def attach(self) -> None:
        """
        Put every hook in place.

        :return: nothing
        """
        self._previous_excepthook = sys.excepthook
        self._previous_threading = threading.excepthook
        self._original_exit = sys.exit
        sys.excepthook = self.excepthook
        threading.excepthook = self.threading_excepthook
        sys.exit = self.exit  # type: ignore[assignment]
        atexit.register(self.at_exit)
        self._attached_ms = int(time.time() * 1000)
        self.watch_cells()
        if not self.wrap_stop():
            # Hooked before the driver imported PySpark, as the `.pth` does.
            self._finder = _Finder(self)
            sys.meta_path.insert(0, self._finder)

    def detach(self) -> None:
        """
        Undo `attach`, leaving alone any hook someone set after it.

        :return: nothing
        """
        # pylint: disable=comparison-with-callable
        if sys.excepthook == self.excepthook:
            sys.excepthook = self._previous_excepthook
        if threading.excepthook == self.threading_excepthook:
            threading.excepthook = self._previous_threading
        if sys.exit == self.exit:
            sys.exit = self._original_exit
        atexit.unregister(self.at_exit)
        if self._shell is not None:
            try:
                self._shell.events.unregister(_CELL_EVENT, self.after_cell)
            except Exception as err:  # pylint: disable=broad-exception-caught
                _LOG.debug("convalesce: could not leave the shell: %s", err)
            self._shell = None
        if self._finder in sys.meta_path:
            sys.meta_path.remove(self._finder)
        self._finder = None
        if (
            self._context_class is not None
            and getattr(self._context_class.stop, "_convalesce", None) is self
        ):
            self._context_class.stop = self._original_stop
        self._context_class = None
        self._original_stop = None

    # -- the application ----------------------------------------------------

    def wrap_stop(self) -> bool:
        """
        Wrap `SparkContext.stop`, once PySpark has a context class to wrap.

        :return: whether it is wrapped
        """
        if self._context_class is not None:
            return True
        context_class = getattr(sys.modules.get(_PYSPARK), "SparkContext", None)
        original = getattr(context_class, "stop", None)
        if context_class is None or not callable(original):
            return False

        @functools.wraps(original)
        def stop(context: Any, *args: Any, **kwargs: Any) -> Any:
            self.remember(context)
            return original(context, *args, **kwargs)

        setattr(stop, "_convalesce", self)
        self._context_class = context_class
        self._original_stop = original
        context_class.stop = stop
        return True

    def remember(self, context: Any) -> None:
        """
        Keep the application a context is, while it can still say.

        :param context: the SparkContext about to stop
        :return: nothing
        """
        self._had_context = True
        try:
            app_id = _read(lambda: context.applicationId)
            if app_id is not None:
                self._stopped = (app_id, _read(lambda: context.appName))
            self.databricks()
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not read the application: %s", err)

    def context(self) -> _Context:
        """
        The driver's application: the running one, else the last stopped.

        :return: its id and name, or None for either
        """
        if _over_connect():
            return _connect_session(self.databricks())
        live = _spark_context()
        if live[0] is not None:
            self._had_context = True
            return live
        if self._stopped[0] is not None:
            return self._stopped
        application = _yarn_application()
        if application is None or not self.owns_yarn(application):
            return application, None
        # What `spark-submit` itself names an application nobody named.
        path = _script_path()
        return application, os.path.basename(path) if path else None

    def owns_yarn(self, app_id: str) -> bool:
        """
        Whether a YARN application is this helper's to report from start to
        end.

        A cluster-mode driver that ends before it starts a Spark session
        has an application, which YARN made, and no listener: the JVM beside
        it never built a Spark context. Nothing else will say the
        application started, and a receiver holds what is said of a run it
        was never told of.

        :param app_id: the application about to be reported
        :return: it is the YARN container's own, and this process never had
            a Spark context
        """
        return (
            app_id == _yarn_application()
            and not self._had_context
            and self._stopped[0] is None
            and not _context_made()
        )

    def databricks(self) -> Dict[str, str]:
        """
        What Databricks says of the job run this driver is for.

        Read while the session can still say, and kept: a stopped context
        answers nothing, and what it said before it stopped still holds.
        What the session will not say is filled from the run's context,
        which is asked once.

        :return: the fields of `_DATABRICKS` and `_CONTEXT` that are set;
            none off Databricks
        """
        try:
            self._databricks.update(_databricks())
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not read the job run: %s", err)
        try:
            if self._run_context is None:
                self._run_context = _run_context()
            for field, value in (self._run_context or {}).items():
                self._databricks.setdefault(field, value)
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not read the run's context: %s", err)
        return dict(self._databricks)

    def describe(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add to a payload which run of its platform this driver is.

        :param payload: what is about to be sent
        :return: it, with `databricks` where Databricks named the run or
            the cluster, and `attempt` where YARN numbered the try
        """
        found = self.databricks()
        if found:
            payload["databricks"] = found
        attempt = _yarn_attempt()
        if attempt is not None:
            payload["attempt"] = attempt
        return payload

    def watch_cells(self) -> None:
        """
        Hear of a failure from an IPython shell, where one runs the driver.

        A notebook, and a Python task on Databricks, run inside a shell that
        catches every exception itself: `sys.excepthook` never sees it and
        the process does not exit when the code does. The shell says so
        after each cell instead.

        :return: nothing
        """
        try:
            module = sys.modules.get(_IPYTHON)
            shell = module.get_ipython() if module is not None else None
            if shell is None:
                return
            shell.events.register(_CELL_EVENT, self.after_cell)
            self._shell = shell
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not watch the shell: %s", err)

    def after_cell(self, result: Any = None) -> None:
        """
        Report a cell that raised, and the script when the shell ran one.

        A shell that runs a script file runs it as its one cell, so the
        script and the run's end are sent as that cell ends. In a notebook
        there is no file and no last cell to wait for: a cell that passes
        says nothing and ends nothing, and only a failure is reported, with
        the run it ends.

        :param result: IPython's `ExecutionResult` for the cell
        :return: nothing
        """
        try:
            exc = getattr(result, "error_in_exec", None) or getattr(
                result, "error_before_exec", None
            )
            if exc is None and _script_path() is None:
                return
            context = self.context()
            if exc is not None:
                self.report(exc, context)
            self.report_script(context)
            self.close(context)
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the cell: %s", err)

    def open(self, context: _Context) -> None:
        """
        Say a run no listener reports has started, once.

        With Spark Connect the driver's Python has no JVM beside it, and a
        serverless platform takes no listener, so nothing else reports the
        application. The same holds for a YARN driver that never started a
        Spark session (`owns_yarn`). This sends Spark's own start event, as
        the listener would have, before the first thing said of the run.

        :param context: the application's id and name
        :return: nothing
        """
        app_id, app_name = context
        if app_id is None or not _is_driver():
            return
        if not _over_connect() and not self.owns_yarn(app_id):
            return
        with self._lock:
            if self._opened:
                return
            self._opened = True
        self._send(
            APPLICATION_START,
            self.describe(
                {
                    "App ID": app_id,
                    "App Name": app_name,
                    "Timestamp": self._attached_ms,
                }
            ),
            [],
        )

    def close(self, context: _Context) -> None:
        """
        Say the run `open` started has ended, once.

        :param context: the application's id and name
        :return: nothing
        """
        app_id, _ = context
        with self._lock:
            if not self._opened or self._closed:
                return
            self._closed = True
        self._send(
            APPLICATION_END,
            self.describe(
                {
                    "App ID": app_id,
                    "Timestamp": int(time.time() * 1000),
                }
            ),
            [],
        )

    # -- the hooks --------------------------------------------------------

    def excepthook(
        self,
        exc_type: type,
        exc: BaseException,
        tb: Optional[types.TracebackType],
    ) -> None:
        """
        Report an uncaught exception, then hand it to the previous hook.

        :param exc_type: the exception's class
        :param exc: the exception
        :param tb: its traceback
        :return: nothing
        """
        try:
            if exc is not None and exc.__traceback__ is None and tb is not None:
                exc = exc.with_traceback(tb)
            self.report(exc, self.context())
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the driver: %s", err)
        _chain(self._previous_excepthook, exc_type, exc, tb)

    def threading_excepthook(self, args: Any) -> None:
        """
        Report an exception uncaught in a thread, then hand it on.

        :param args: `threading.ExceptHookArgs`
        :return: nothing
        """
        try:
            exc = getattr(args, "exc_value", None)
            if exc is not None and not isinstance(exc, SystemExit):
                self.report(exc, self.context())
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the driver: %s", err)
        _chain(self._previous_threading, args)

    def exit(self, code: Any = None) -> None:
        """
        Stand in for `sys.exit`: remember a failing code, then exit.

        The application is read now, while the driver still holds it; by
        the time the interpreter exits it may have been stopped.

        :param code: the exit status, as `sys.exit` takes it
        :return: never; raises `SystemExit` as `sys.exit` does
        """
        try:
            if (
                _failing(code)
                and threading.current_thread() is threading.main_thread()
            ):
                self._exit_code = code
                self._exit_context = self.context()
                self.databricks()
                # A launcher that runs the script for the platform (AWS
                # Glue's does) catches its exception and exits non-zero.
                # The exception being handled then is why the driver failed.
                handled = sys.exc_info()[1]
                _note_script_of(handled)
                self._exit_cause = (
                    handled if isinstance(handled, Exception) else None
                )
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not read the exit: %s", err)
        self._original_exit(code)

    def at_exit(self) -> None:
        """
        Report a non-zero `sys.exit()`, then what the driver ran.

        :return: nothing
        """
        try:
            if self._exit_code is not None:
                self.report(
                    self._exit_cause or SystemExit(self._exit_code),
                    self._exit_context,
                )
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the driver: %s", err)
        try:
            self.report_script(self.context())
            self.close(self.context())
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the script: %s", err)

    # -- sending ------------------------------------------------------------

    def report(self, exc: BaseException, context: _Context) -> None:
        """
        Send the one `driver_failure` observation this process sends.

        Only from a PySpark driver: a process that never imported PySpark is
        not a Spark application, and one PySpark itself runs as `__main__`
        (an executor's Python worker) is not the driver.

        A notebook's command line is its kernel's (the launcher, and the
        file the kernel connects through), not anything its author wrote,
        so a notebook sends no `argv`.

        :param exc: what ended the driver
        :param context: the application's id and name, where known
        :return: nothing
        """
        if not _is_driver():
            return
        with self._lock:
            if self._sent:
                return
            self._sent = True
        app_id, app_name = context
        self.open(context)
        payload: Dict[str, Any] = {
            "application_id": app_id,
            "application_name": app_name,
            "error_detail": cemit.error_detail(exc),
            "python_version": platform.python_version(),
            "pyspark_version": _pyspark_version(),
        }
        excluded: List[Dict[str, str]] = []
        if not _in_notebook():
            payload["argv"], excluded = _argv()
        self._send(EVENT, self.describe(payload), excluded)

    def report_script(self, context: _Context) -> None:
        """
        Send the one `driver_script` observation this process sends.

        Only for a driver whose application is known: the script and its
        arguments describe a run, and without the application there is no
        run to describe. A notebook has no script, and its kernel's
        arguments are not the job's, so there nothing is sent.

        :param context: the application's id and name, where known
        :return: nothing
        """
        app_id, app_name = context
        if app_id is None or not _is_driver():
            return
        if _in_notebook():
            return
        with self._lock:
            if self._script_sent:
                return
            self._script_sent = True
        self.open(context)
        argv, excluded = _argv()
        payload: Dict[str, Any] = {
            "application_id": app_id,
            "application_name": app_name,
            "argv": argv,
            "python_version": platform.python_version(),
            "pyspark_version": _pyspark_version(),
        }
        source, masked = _script_source()
        if source is not None:
            payload["source"] = source
            excluded.extend(masked)
        self._send(SCRIPT_EVENT, self.describe(payload), excluded)

    def _send(
        self, event: str, payload: Dict[str, Any], excluded: List[Dict[str, str]]
    ) -> None:
        """
        Send one observation and flush it before the driver goes on.

        :param event: which observation
        :param payload: what it carries
        :param excluded: what was masked in it, by path and reason
        :return: nothing
        """
        emitter = self._emitter
        if emitter is None and self._config is not None:
            emitter = cemit.Emitter(self._config)
        cemit.send_one(
            tool=TOOL,
            event=event,
            payload=payload,
            emitter=emitter,
            tool_version=_pyspark_version(),
            excluded=excluded,
        )


# #############################################################################
# _Finder
# #############################################################################


class _Finder:
    """
    Watches the driver's imports until PySpark's context can be wrapped.

    The `.pth` hooks the interpreter before the driver's script has
    imported anything. This sits on `sys.meta_path`, finds nothing, and at
    each PySpark import asks the hook to wrap `SparkContext.stop`. It also
    learns the script a launcher runs as the main program, from the first
    thing that script imports. With both done it takes itself off the path.

    :param hook: the installed hooks
    """

    def __init__(self, hook: _Hook) -> None:
        self._hook = hook
        self._wrapped = False
        self._looks = 0

    def find_spec(self, name: str, path: Any = None, target: Any = None) -> None:
        """
        Find nothing; only notice that PySpark is being imported.

        :param name: the module being imported
        :param path: where its package looks, unused
        :param target: the module being reloaded, unused
        :return: None, always, so the real finders import it
        """
        del path, target
        try:
            if _CALLER_SCRIPT is None and self._looks < _MAX_LOOKS:
                # A launcher may have imported PySpark already, so the
                # script is looked for at whatever it imports first.
                self._looks += 1
                importer = sys._getframe(1)  # pylint: disable=protected-access
                _note_script(importer)
            if name.startswith(f"{_PYSPARK}."):
                self._wrapped = self._wrapped or self._hook.wrap_stop()
            if self._wrapped and (
                _CALLER_SCRIPT is not None or self._looks >= _MAX_LOOKS
            ):
                sys.meta_path.remove(self)
        except Exception:  # pylint: disable=broad-exception-caught
            pass


# #############################################################################
# Reading the driver
# #############################################################################


def _chain(hook: Callable[..., Any], *args: Any) -> None:
    """
    Call the hook this one replaced, never letting it raise through ours.

    :param hook: the previous hook
    :param args: what it is called with
    :return: nothing
    """
    try:
        hook(*args)
    except Exception:  # pylint: disable=broad-exception-caught
        if hook is not sys.__excepthook__ and len(args) == 3:
            sys.__excepthook__(*args)


def _failing(code: Any) -> bool:
    """
    Whether `sys.exit(code)` ends the process with a non-zero status.

    :param code: as `sys.exit` takes it
    :return: False for None and 0, True for anything else
    """
    return code is not None and code != 0


def _is_driver() -> bool:
    """
    Whether this process is a PySpark driver.

    :return: PySpark is imported, and `__main__` is not one of its modules
    """
    if _PYSPARK not in sys.modules:
        return False
    main = sys.modules.get("__main__")
    spec = getattr(main, "__spec__", None)
    name = str(getattr(spec, "name", "") or "")
    return not name.startswith(f"{_PYSPARK}.")


def _spark_context() -> _Context:
    """
    The running application's id and name, where there is one.

    Read off PySpark's active context without importing anything: a driver
    that never started one has no application to fail.

    :return: `spark.app.id` and `spark.app.name`, or None for either
    """
    module = sys.modules.get(_PYSPARK)
    context_class = getattr(module, "SparkContext", None)
    context = getattr(context_class, "_active_spark_context", None)
    if context is None:
        return None, None
    return (
        _read(lambda: context.applicationId),
        _read(lambda: context.appName),
    )


def _context_made() -> bool:
    """
    Whether this process has, or began to build, a Spark context.

    PySpark keeps the gateway to its JVM once a context was asked for, even
    after the context stops, so a process that only imported PySpark shows
    neither.

    :return: a context is active, or a gateway to a JVM was opened
    """
    context_class = getattr(sys.modules.get(_PYSPARK), "SparkContext", None)
    return (
        getattr(context_class, "_active_spark_context", None) is not None
        or getattr(context_class, "_gateway", None) is not None
    )


def _yarn_application() -> Optional[str]:
    """
    The YARN application this driver is, read from its container's name.

    A cluster-mode driver that fails before it starts a Spark session has
    no context to ask, yet YARN already made its application.

    :return: `application_<cluster>_<n>`, or None outside a YARN container
    """
    match = _YARN_CONTAINER.match(os.environ.get(_YARN_CONTAINER_ENV, ""))
    return f"application_{match.group(1)}_{match.group(2)}" if match else None


def _yarn_attempt() -> Optional[int]:
    """
    Which try of its YARN application this driver is.

    YARN runs a failed cluster-mode driver again, under the same
    application id, and every try sends what the one before it did. The
    container's name numbers the try.

    :return: the attempt, from 1, or None outside a YARN container
    """
    match = _YARN_CONTAINER.match(os.environ.get(_YARN_CONTAINER_ENV, ""))
    return int(match.group(3)) if match else None


def _databricks() -> Dict[str, str]:
    """
    What the driver's session says of Databricks, importing nothing.

    Over Spark Connect the session's configuration is all there is to ask,
    and a serverless platform does not answer for every name. With a
    context, a job run's id is a local property of the thread its code
    runs on, and the cluster's own settings are its configuration.

    :return: the fields of `_DATABRICKS` that are set
    """
    session = _connect() if _over_connect() else None
    module = sys.modules.get(_PYSPARK)
    context_class = getattr(module, "SparkContext", None)
    context = getattr(context_class, "_active_spark_context", None)
    found: Dict[str, str] = {}
    for field, name in _DATABRICKS.items():
        value = _setting(session, context, name)
        if value:
            found[field] = value
    return found


def _setting(session: Any, context: Any, name: str) -> Optional[str]:
    """
    One Spark setting of the running driver.

    :param session: its Spark Connect session, when it runs over one
    :param context: else its Spark context, when it has one
    :param name: the setting
    :return: its value, or None when it is unset or cannot be read
    """
    if session is not None:
        return _read(lambda: session.conf.get(name, None))
    if context is None:
        return None
    return _read(lambda: context.getLocalProperty(name)) or _read(
        lambda: context.getConf().get(name)
    )


def _over_connect() -> bool:
    """
    Whether this driver talks to Spark over Spark Connect, with no JVM of
    its own and so no application a listener could have reported.

    A serverless platform can keep a local context in the process that runs
    the shell, shared by every run it serves and reported by nothing: there
    the Connect session is the run, whatever context is beside it.

    :return: a Connect session was made, and no Spark context of the
        application's own
    """
    if _connect() is None:
        return False
    if os.environ.get(_SERVERLESS_ENV, "").strip().lower() == "true":
        return True
    module = sys.modules.get(_PYSPARK)
    context_class = getattr(module, "SparkContext", None)
    return getattr(context_class, "_active_spark_context", None) is None


def _connect() -> Any:
    """
    The Spark Connect session this driver made, without importing anything.

    :return: the session, or None
    """
    session_class = getattr(sys.modules.get(_CONNECT), "SparkSession", None)
    if session_class is None:
        return None
    for name in ("getActiveSession", "getDefaultSession"):
        try:
            session = getattr(session_class, name)()
        except Exception:  # pylint: disable=broad-exception-caught
            session = None
        if session is not None:
            return session
    return getattr(session_class, "_default_session", None)


def _connect_session(said: Dict[str, str]) -> _Context:
    """
    A Spark Connect session as the application it stands for.

    The session's id is the only identity the client holds. Its name is the
    script's file name, which says what ran. A notebook has no script: it
    is named by the last part of its path where Databricks says it, else
    by the Databricks job it is a task of, else by the session's
    `spark.app.name` where the server says one. A session none of those
    names is called `notebook` inside a shell and `pyspark` outside one: a
    run with no name is a run a receiver cannot file. It is never named
    after the kernel's own launcher.

    :param said: what Databricks said of the run (`_Hook.databricks`)
    :return: the session's id and a name, or None for both
    """
    session = _connect()
    session_id = (
        _read(lambda: session.session_id) if session is not None else None
    )
    if session_id is None:
        return None, None
    path = _script_path()
    if path:
        return session_id, os.path.splitext(os.path.basename(path))[0]
    notebook = said.get("notebook_path") or ""
    name = (
        notebook.rstrip("/").rsplit("/", 1)[-1]
        or said.get("job_name")
        or _read(lambda: session.conf.get("spark.app.name"))
        or (_NOTEBOOK_NAME if _in_shell() else _SESSION_NAME)
    )
    return session_id, name


def _dbutils() -> Any:
    """
    The `dbutils` Databricks gives the code a shell runs, importing nothing.

    Databricks puts it among the shell's own names for a notebook and for a
    Python file task alike.

    :return: it, or None where there is no shell or the shell has none
    """
    module = sys.modules.get(_IPYTHON)
    shell = module.get_ipython() if module is not None else None
    names = getattr(shell, "user_ns", None)
    return names.get(_DBUTILS) if isinstance(names, dict) else None


def _run_context() -> Optional[Dict[str, str]]:
    """
    What the context of a Databricks run says of the run, by name.

    The context is what `dbutils` hands a notebook that asks which run it
    is. As JSON it also carries the run's own credentials for the
    workspace, so it is never kept, logged or sent: the attributes listed
    in `_CONTEXT` are taken out of it by name, here, and the rest is let
    go. A Python file task's `notebook_path` is its script, which the
    script's own observation already names, so only a notebook's is kept.

    :return: the fields of `_CONTEXT` that are set; nothing where the
        context would not answer; None where there is no `dbutils` to ask,
        which may be there the next time
    """
    dbutils = _dbutils()
    if dbutils is None:
        return None
    try:
        context = (
            dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        )
        attributes = _json_object(context.safeToJson()).get("attributes")
    except Exception:  # pylint: disable=broad-exception-caught
        return {}
    if not isinstance(attributes, dict):
        return {}
    named = {
        name: str(attributes[name])
        for name in (*_CONTEXT.values(), _CONTEXT_TASK_TYPE)
        if isinstance(attributes.get(name), (str, int))
        and attributes[name] != ""
    }
    found = {
        field: named[name] for field, name in _CONTEXT.items() if name in named
    }
    if "task_run_id" in found:
        found.setdefault("job_run_id", found["task_run_id"])
    if named.get(_CONTEXT_TASK_TYPE, _NOTEBOOK_TASK) != _NOTEBOOK_TASK:
        found.pop("notebook_path", None)
    return found


def _json_object(text: Any) -> Dict[str, Any]:
    """
    A JSON object, read from text.

    :param text: the text
    :return: the object, or an empty one when the text is not one
    """
    value = json.loads(str(text))
    return value if isinstance(value, dict) else {}


def _read(getter: Callable[[], Any]) -> Optional[str]:
    """
    One attribute of the Spark context, as text.

    :param getter: reads it; may call into the JVM, and may fail
    :return: the value, or None when it is unset or could not be read
    """
    try:
        value = getter()
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    return str(value) if value is not None else None


def _pyspark_version() -> Optional[str]:
    """
    The driver's PySpark version.

    :return: `pyspark.__version__`, or None
    """
    return cemit.version_of(_PYSPARK) if _PYSPARK in sys.modules else None


def _argv() -> Tuple[List[str], List[Dict[str, str]]]:
    """
    The driver's script and arguments, with credentials masked.

    `redact_secrets` masks `--password=...` and a password in a URI; a
    secret-named flag followed by its value (`--password hunter2`) has the
    value masked here, since only the flag says what it is.

    :return: the arguments, and what was masked, by path and reason; the
        script alone when `CONVALESCE_SEND_ARGUMENTS` is false
    """
    args = [str(arg) for arg in sys.argv]
    if not _arguments_enabled():
        args = args[:1]
    redacted, excluded = cemit.redact_secrets(args, path="argv")
    out: List[str] = list(redacted)
    for i in range(1, len(out)):
        flag = args[i - 1]
        if not flag.startswith("-") or "=" in flag:
            continue
        _, hit = cemit.redact_secrets({flag.lstrip("-"): out[i]})
        if hit:
            out[i] = "***"
            excluded.append({"path": f"argv[{i}]", "reason": "secret redacted"})
    return out, excluded


def _arguments_enabled() -> bool:
    """
    Whether what the driver was called with may be sent.

    :return: False only when `CONVALESCE_SEND_ARGUMENTS` says so
    """
    return (cepysenv.read(_ARGUMENTS_ENV) or "").strip().lower() not in _FALSY


def _frame_file(frame: Any) -> Optional[str]:
    """
    The file whose code a frame runs as the main program.

    :param frame: a frame of the running interpreter
    :return: its file, or None when it does not run as `__main__` or runs
        no file that exists
    """
    scope = getattr(frame, "f_globals", None) or {}
    if scope.get("__name__") != "__main__":
        return None
    path = scope.get("__file__") or getattr(frame.f_code, "co_filename", None)
    if not path or not os.path.isfile(str(path)):
        return None
    return os.path.abspath(str(path))


def _main_file(frame: Any) -> Optional[str]:
    """
    The file this process was started with, asked as the hooks go in.

    The outermost code running as `__main__` is the process's own: a
    launcher (AWS Glue's `runscript.py`) is below the script it runs, and
    while that script runs `sys.modules["__main__"]` is the script's. When
    the `.pth` hooks the interpreter nothing runs as `__main__` yet, and
    `__main__` and `sys.argv[0]` still say what the process was started
    with. Inside an IPython shell the stack and `sys.argv` are the
    platform's to arrange, so only `__main__` is asked.

    :param frame: where `install()` was called from
    :return: that file, or None when the process was started with none
    """
    found: Optional[str] = None
    shell = _in_shell()
    while frame is not None and not shell:
        found = _frame_file(frame) or found
        frame = frame.f_back
    if found is not None:
        return found
    path = getattr(sys.modules.get("__main__"), "__file__", None)
    if not path and not shell:
        path = sys.argv[0] if sys.argv else None
    if not path or not os.path.isfile(str(path)):
        return None
    return os.path.abspath(str(path))


def _in_notebook() -> bool:
    """
    Whether this driver is a notebook: a shell runs it, and runs no script.

    :return: it is
    """
    return _in_shell() and _script_path() is None


def _in_shell() -> bool:
    """
    Whether an IPython shell runs this driver's code, importing nothing.

    :return: IPython is imported and has a shell
    """
    try:
        module = sys.modules.get(_IPYTHON)
        return module is not None and module.get_ipython() is not None
    except Exception:  # pylint: disable=broad-exception-caught
        return False


def _kernel_launcher(path: str) -> bool:
    """
    Whether a file is a notebook kernel's own launcher, by what it is called.

    :param path: a file
    :return: it is named as IPython's and ipykernel's launchers are
        (Databricks' is `db_ipykernel_launcher.py`), or is in their package
    """
    folder = os.path.basename(os.path.dirname(path)).lower()
    stem = os.path.splitext(os.path.basename(path))[0]
    return folder in _KERNEL_PACKAGES or bool(_KERNEL_LAUNCHER.search(stem))


def _caller_script(frame: Any) -> Optional[str]:
    """
    The script that called `install()`, when a launcher runs it as the main
    program.

    A platform's launcher (AWS Glue's `runscript.py`) is the process's
    `__main__` and runs the job's script under that name. The script is then
    the file whose code runs as `__main__` and is not the launcher's own.

    :param frame: the frame `install()` was called from
    :return: that file, or None when the caller is not such a script
    """
    path = _frame_file(frame)
    if path is None or path == _MAIN_FILE or _kernel_launcher(path):
        return None
    return path


def _note_script(frame: Any, depth: int = _MAX_FRAMES) -> None:
    """
    Learn the script from whoever is importing something, when a launcher
    runs it as the main program and nothing called `install()`.

    The `.pth` hooks the interpreter before any script runs, so there is no
    caller to ask. The innermost code running under the name `__main__`,
    from a file that is not the one the process was started with, is the
    job's script. The file is compared with what was remembered as the
    hooks went in (`_MAIN_FILE`): while a launcher's script runs,
    `sys.modules["__main__"]` is the script's own module.

    :param frame: where to start looking, innermost first
    :param depth: how many frames outward to look
    :return: nothing
    """
    global _CALLER_SCRIPT
    if _CALLER_SCRIPT is not None:
        return
    seen = 0
    while frame is not None and seen < depth:
        found = _caller_script(frame)
        if found is not None:
            _CALLER_SCRIPT = found
            return
        frame, seen = frame.f_back, seen + 1


def _note_script_of(exc: Optional[BaseException]) -> None:
    """
    Learn the script from the traceback of the error a launcher caught.

    :param exc: the exception being handled as the launcher exits
    :return: nothing
    """
    global _CALLER_SCRIPT
    if _CALLER_SCRIPT is not None:
        return
    tb = getattr(exc, "__traceback__", None)
    found: Optional[str] = None
    seen = 0
    while tb is not None and seen < _MAX_FRAMES:
        # A traceback runs outermost first, and the script is the innermost
        # code that runs as `__main__`.
        found = _caller_script(tb.tb_frame) or found
        tb, seen = tb.tb_next, seen + 1
    _CALLER_SCRIPT = found


def _script_path() -> Optional[str]:
    """
    The file this driver is running.

    A driver started as `python -m package` is a launcher, such as a
    notebook kernel, and its file is not the job. A script that a launcher
    ran as the main program is. Inside an IPython shell the kernel's own
    launcher is never the job: there is a script only where the platform
    put one in `sys.argv[0]`, as Databricks does for a Python file task,
    and a notebook has none.

    :return: the script's path, or None when there is no such file
    """
    if _CALLER_SCRIPT is not None:
        return _CALLER_SCRIPT
    if _in_shell():
        path = sys.argv[0] if sys.argv else None
        if not path or not os.path.isfile(str(path)):
            return None
        path = os.path.abspath(str(path))
        return None if path == _MAIN_FILE or _kernel_launcher(path) else path
    main = sys.modules.get("__main__")
    if getattr(main, "__spec__", None) is not None:
        return None
    path = getattr(main, "__file__", None) or (sys.argv[0] if sys.argv else None)
    if not path or not os.path.isfile(str(path)):
        return None
    return os.path.abspath(str(path))


def _script_source() -> Tuple[Optional[Dict[str, Any]], List[Dict[str, str]]]:
    """
    The text of the script this driver is running, with credentials masked.

    The hash is of the file as it is on this machine, whole and unmasked,
    so it can be compared with the same file anywhere else.

    :return: `{"file", "text", "language", "sha256", "truncated"}` and what
        was masked in the text, by path and reason; None and nothing when
        `CONVALESCE_SEND_SOURCE` is false or the script cannot be read
    """
    if not cesource.enabled():
        return None, []
    path = _script_path()
    if path is None:
        return None, []
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError:
        return None, []
    text = raw.decode("utf-8", errors="replace")
    masked, excluded = cemit.redact_secrets(
        text[: cesource.MAX_CHARS], path="source.text"
    )
    source = {
        "file": path,
        "text": masked,
        "language": "python",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "truncated": len(text) > cesource.MAX_CHARS,
    }
    return source, excluded

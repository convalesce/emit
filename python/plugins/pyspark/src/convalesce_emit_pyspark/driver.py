"""
Report a PySpark driver that fails in Python.

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

Import as:

import convalesce_emit_pyspark.driver as cepysdri
"""

import atexit
import logging
import platform
import sys
import threading
import types
from typing import Any, Callable, Dict, List, Optional, Tuple

import convalesce_emit as cemit

_LOG = logging.getLogger(__name__)

TOOL = "spark"
EVENT = "driver_failure"

_PYSPARK = "pyspark"

_Context = Tuple[Optional[str], Optional[str]]

_INSTALLED: Optional["_Hook"] = None
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
    global _INSTALLED
    try:
        with _INSTALL_LOCK:
            if _INSTALLED is not None:
                return True
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
    global _INSTALLED
    with _INSTALL_LOCK:
        if _INSTALLED is not None:
            _INSTALLED.detach()
            _INSTALLED = None


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
        self._exit_code: Any = None
        self._exit_context: _Context = (None, None)
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
            self.report(exc, _spark_context())
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
                self.report(exc, _spark_context())
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
                self._exit_context = _spark_context()
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not read the exit: %s", err)
        self._original_exit(code)

    def at_exit(self) -> None:
        """
        Report a non-zero `sys.exit()`, once the interpreter is exiting.

        :return: nothing
        """
        try:
            if self._exit_code is not None:
                self.report(SystemExit(self._exit_code), self._exit_context)
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the driver: %s", err)

    # -- sending ------------------------------------------------------------

    def report(self, exc: BaseException, context: _Context) -> None:
        """
        Send the one `driver_failure` observation this process sends.

        Only from a PySpark driver: a process that never imported PySpark is
        not a Spark application, and one PySpark itself runs as `__main__`
        (an executor's Python worker) is not the driver.

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
        argv, excluded = _argv()
        payload: Dict[str, Any] = {
            "application_id": app_id,
            "application_name": app_name,
            "error_detail": cemit.error_detail(exc),
            "argv": argv,
            "python_version": platform.python_version(),
            "pyspark_version": _pyspark_version(),
        }
        emitter = self._emitter
        if emitter is None and self._config is not None:
            emitter = cemit.Emitter(self._config)
        cemit.send_one(
            tool=TOOL,
            event=EVENT,
            payload=payload,
            emitter=emitter,
            tool_version=payload["pyspark_version"],
            excluded=excluded,
        )


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

    :return: the arguments, and what was masked, by path and reason
    """
    args = [str(arg) for arg in sys.argv]
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

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
import logging
import os
import platform
import sys
import threading
import types
from typing import Any, Callable, Dict, List, Optional, Tuple

import convalesce_emit as cemit
import convalesce_emit.source as cesource

_LOG = logging.getLogger(__name__)

TOOL = "spark"
EVENT = "driver_failure"
SCRIPT_EVENT = "driver_script"

_PYSPARK = "pyspark"
_ARGUMENTS_ENV = "CONVALESCE_SEND_ARGUMENTS"
_FALSY = frozenset({"0", "false", "no", "off"})

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
        self._script_sent = False
        # The application a stopped context was, read as it stopped.
        self._stopped: _Context = (None, None)
        self._context_class: Any = None
        self._original_stop: Any = None
        self._finder: Optional["_Finder"] = None
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
        try:
            app_id = _read(lambda: context.applicationId)
            if app_id is not None:
                self._stopped = (app_id, _read(lambda: context.appName))
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not read the application: %s", err)

    def context(self) -> _Context:
        """
        The driver's application: the running one, else the last stopped.

        :return: its id and name, or None for either
        """
        live = _spark_context()
        return live if live[0] is not None else self._stopped

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
                self.report(SystemExit(self._exit_code), self._exit_context)
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the driver: %s", err)
        try:
            self.report_script(self.context())
        except Exception as err:  # pylint: disable=broad-exception-caught
            _LOG.debug("convalesce: could not report the script: %s", err)

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
        self._send(EVENT, payload, excluded)

    def report_script(self, context: _Context) -> None:
        """
        Send the one `driver_script` observation this process sends.

        Only for a driver whose application is known: the script and its
        arguments describe a run, and without the application there is no
        run to describe.

        :param context: the application's id and name, where known
        :return: nothing
        """
        app_id, app_name = context
        if app_id is None or not _is_driver():
            return
        with self._lock:
            if self._script_sent:
                return
            self._script_sent = True
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
        self._send(SCRIPT_EVENT, payload, excluded)

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
            tool_version=payload["pyspark_version"],
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
    each PySpark import asks the hook to wrap `SparkContext.stop`; once that
    succeeds it takes itself off the path.

    :param hook: the installed hooks
    """

    def __init__(self, hook: _Hook) -> None:
        self._hook = hook

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
            if name.startswith(f"{_PYSPARK}.") and self._hook.wrap_stop():
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
    return os.environ.get(_ARGUMENTS_ENV, "").strip().lower() not in _FALSY


def _script_path() -> Optional[str]:
    """
    The file this driver is running.

    A driver started as `python -m package` is a launcher, such as a
    notebook kernel, and its file is not the job.

    :return: the script's path, or None when there is no such file
    """
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

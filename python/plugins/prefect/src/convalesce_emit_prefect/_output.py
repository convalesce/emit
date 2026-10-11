"""
The last lines a failed flow run or task run logged.

Prefect hands everything a run says to two loggers, `prefect.flow_runs`
and `prefect.task_runs`: what the run's author logs through
`get_run_logger()`, what the run prints with `log_prints` on, and what the
engine says of the run itself. Each record carries the ids of the run it
belongs to, which is what tells one task's lines from its neighbour's in a
process that runs them side by side.

They are held as they pass, by a filter and not a handler. Prefect sets
its logging up from a configuration, and doing so takes every handler off
the loggers it names; it runs when `prefect` is imported, which is after
this package for any flow that imports the two in alphabetical order. A
filter is left where it was put. This one keeps the record and lets it
through unchanged.

A run that fails or crashes sends the end of its lines as `output_tail`;
see `convalesce_emit.output`. On unless `CONVALESCE_SEND_OUTPUT` is false.

Import as:

import convalesce_emit_prefect._output as ceout
"""

import logging
from typing import Any, Dict, List, Optional

import convalesce_emit.output as ceoutput
import convalesce_emit_prefect._mask as cemask

_LOG = cemask.logger(__name__)

LOGGERS = ("prefect.flow_runs", "prefect.task_runs")
# A flow's tasks share one ring, so it holds more than one unit's worth:
# enough that a failed task still has its lines after its neighbours have
# said theirs.
MAX_RECORDS = 2000
# The ids a record of a run carries, the narrower first: a task's record
# names its flow run as well, and belongs to the task.
_RUN_IDS = ("task_run_id", "flow_run_id")


# #############################################################################
# _Runs
# #############################################################################


class _Runs(ceoutput.Ring):
    """
    A ring that knows which run each record belongs to.
    """

    def scope(self, record: logging.LogRecord) -> Any:
        """
        The run a record belongs to.

        :param record: what was logged
        :return: the run's id as text; None for a record of no run
        """
        for name in _RUN_IDS:
            run_id = getattr(record, name, None)
            if run_id:
                return str(run_id)
        return None


# #############################################################################
# _Feed
# #############################################################################


class _Feed(logging.Filter):
    """
    Hand each record to the ring, and let it through.

    :param ring: where the records are held
    """

    def __init__(self, ring: _Runs) -> None:
        super().__init__()
        self.ring = ring

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Keep the record.

        :param record: the record on its way to Prefect's handlers
        :return: True, always: nothing is dropped
        """
        self.ring.emit(record)
        return True


_RING = _Runs(capacity=MAX_RECORDS)


def watch() -> None:
    """
    Start holding what runs in this process log.

    Called once, as the package is imported; a second call changes nothing.

    :return: nothing
    """
    try:
        if not ceoutput.enabled():
            return
        for name in LOGGERS:
            logger = logging.getLogger(name)
            if not any(isinstance(each, _Feed) for each in logger.filters):
                logger.addFilter(_Feed(_RING))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not watch output: %s", type(exc).__name__
        )


def unwatch() -> None:
    """
    Stop holding what runs log, and forget what was held.

    :return: nothing
    """
    try:
        for name in LOGGERS:
            logger = logging.getLogger(name)
            for each in list(logger.filters):
                if isinstance(each, _Feed):
                    logger.removeFilter(each)
        _RING.clear()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not stop watching output: %s",
            type(exc).__name__,
        )


def lines_of(run: Any) -> List[str]:
    """
    What one run logged, oldest first.

    :param run: the task run or flow run a hook was given
    :return: each record's text; none for a run that logged nothing, or
        whose lines its neighbours have pushed out
    """
    run_id = getattr(run, "id", None)
    if run_id is None:
        return []
    return _RING.lines(str(run_id))


def add(payload: Dict[str, Any], run: Optional[Any]) -> List[Dict[str, str]]:
    """
    Put a failed run's last lines on the event it is sent with.

    :param payload: what is about to be sent, changed in place
    :param run: the task run or flow run that failed
    :return: what was masked or cut from the lines, by path and reason
    """
    try:
        return ceoutput.add(payload, lines_of(run))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _LOG.warning(
            "convalesce: could not read the output: %s", type(exc).__name__
        )
        return []

"""
Loggers that keep a Prefect Cloud account and workspace out of what is logged.

A Prefect Cloud API URL names the account and the workspace by id, and the
client's own errors quote the URL they failed on. A log line is read by
whoever runs the flow and shipped wherever that process's logs go, so every
line this package writes passes through here first.

Import as:

import convalesce_emit_prefect._mask as cemask
"""

import logging
import re

MASK = "***"
# The two path segments of a Cloud URL that are ids, wherever the URL sits in
# a line: bare, quoted inside an error, or followed by a longer path.
_CLOUD_IDS = re.compile(r"\b(accounts|workspaces)/[^/\s'\"?#]+")


def mask(text: str) -> str:
    """
    Replace the account and workspace ids of any Cloud URL in a text.

    :param text: a log line, an error, a URL
    :return: the same text, each id replaced by `MASK`
    """
    return _CLOUD_IDS.sub(rf"\1/{MASK}", text)


class _MaskIds(logging.Filter):
    """
    Rewrite each record's message with the ids masked, before any handler
    formats it.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Mask the record in place.

        :param record: the record about to be handled
        :return: True, always: nothing is dropped
        """
        try:
            record.msg = mask(record.getMessage())
            record.args = None
        except Exception:  # pylint: disable=broad-exception-caught
            # A message that will not format is the handler's to report.
            pass
        return True


def logger(name: str) -> logging.Logger:
    """
    The logger of one of this package's modules, masking what it writes.

    :param name: the module's `__name__`
    :return: its logger, with the mask on it once
    """
    log = logging.getLogger(name)
    if not any(isinstance(each, _MaskIds) for each in log.filters):
        log.addFilter(_MaskIds())
    return log

"""
Forward Prefect flow and task run state to Convalesce.

Import as:

import convalesce_emit_prefect as ceprefec
"""

import logging

import convalesce_emit_prefect._capture as cecap
import convalesce_emit_prefect._settings as ceset
from convalesce_emit_prefect._lineage import launched, lineage
from convalesce_emit_prefect._version import __version__
from convalesce_emit_prefect.hooks import emit_flow_run, emit_task_run

_LOG = logging.getLogger(__name__)

# The hooks are functions a flow's author attaches; nothing calls into this
# package before a task ends. The SQL a task runs has to be noted while it
# runs, so the watching starts here, where the author's own import of the
# hooks lands.
cecap.watch()
# The same holds for a Variable a run reads: it is noted as it is read, or
# not at all.
ceset.watch_variables()

__all__ = [
    "__version__",
    "emit_flow_run",
    "emit_task_run",
    "launched",
    "lineage",
]

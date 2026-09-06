"""Fires a Prefect state hook.

The hook is passed by reference rather than wrapped, because Prefect 2 calls
these by keyword and Prefect 3 positionally; letting Prefect choose is the
whole point.
"""

import prefect
from prefect import flow

import convalesce_emit_prefect as cepref

sent = []


class _Recorder:
    def emit(self, **kwargs):
        sent.append(kwargs)

    def flush(self):
        pass


@flow
def nightly():
    return 1


cepref.emit_flow_run(nightly, {"id": "abc"}, {"type": "COMPLETED"}, emitter=_Recorder())

payload = sent[0]["payload"]
assert payload["flow"].get("name") == "nightly", payload["flow"]
# A Prefect Flow reaches its task runner, then that runner's logger, then
# every logger in the process. One flow run once produced 256 MB.
import json

size = len(json.dumps(payload, default=str))
assert size < 100_000, f"payload was {size} bytes"

print(f"PROBE_OK prefect {prefect.__version__} ({size} bytes)")

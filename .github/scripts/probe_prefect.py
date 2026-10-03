"""Fires a Prefect state hook.

The hook is passed by reference rather than wrapped, because Prefect 2 calls
these by keyword and Prefect 3 positionally; letting Prefect choose is the
whole point.
"""

import prefect
from prefect import flow, task

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

# A task that starts a run elsewhere, as a customer writes it: the hooks are
# Prefect's to call, keyword on 2.x and positional on 3.x, and the result is
# read from the state as each version holds it in memory.
GLUE_RUN = "jr_" + "ab" * 32
task_sent = []


class _TaskRecorder:
    def emit(self, **kwargs):
        task_sent.append(kwargs["payload"])

    def flush(self):
        pass


def _on_task_end(task, task_run, state):
    cepref.emit_task_run(task, task_run, state, emitter=_TaskRecorder())


@task(on_completion=[_on_task_end])
def start_glue_job():
    cepref.launched("glue", GLUE_RUN, job="probe_daily_agg")
    return GLUE_RUN


@task(on_completion=[_on_task_end])
def summary():
    return {"rows": 1}


@flow
def launches():
    start_glue_job()
    summary()


launches()
by_task = {p["task"]["name"]: p for p in task_sent}
started = by_task["start_glue_job"]
assert started.get("result_text") == GLUE_RUN, sorted(started)
assert started.get("launched") == [
    {"platform": "glue", "run_id": GLUE_RUN, "job": "probe_daily_agg"}
], started.get("launched")
assert "result_text" not in by_task["summary"], by_task["summary"].get("result_text")

print(f"PROBE_OK prefect {prefect.__version__} ({size} bytes)")

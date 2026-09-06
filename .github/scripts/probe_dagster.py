"""Runs a real Dagster job and forwards its run record."""

import dagster

import convalesce_emit_dagster as cedag

sent = []


class _Recorder:
    def emit(self, **kwargs):
        sent.append(kwargs)

    def flush(self):
        pass


@dagster.op
def load():
    return 1


@dagster.job
def nightly():
    load()


result = nightly.execute_in_process()
cedag.convalesce_sensor(context=result.dagster_run, emitter=_Recorder())

payload = sent[0]["payload"]["context"]
# A DagsterRun is a namedtuple. Serialised as a plain tuple it arrives as an
# anonymous array with every field name lost, which has happened.
assert isinstance(payload, dict), f"the run arrived as {type(payload).__name__}"
assert payload.get("job_name") == "nightly", payload.get("job_name")

print(f"PROBE_OK dagster {dagster.__version__}")

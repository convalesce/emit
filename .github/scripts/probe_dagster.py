"""Runs a real Dagster job and forwards its run through a real run-status context."""

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


instance = dagster.DagsterInstance.ephemeral()
result = nightly.execute_in_process(instance=instance)
success = [e for e in result.all_events if e.event_type_value in ("PIPELINE_SUCCESS", "RUN_SUCCESS")]
assert success, [e.event_type_value for e in result.all_events]

# The context a run-status sensor really receives, built the way Dagster's
# own docs say to test one. Passing a bare DagsterRun instead once hid that
# the context itself forwards as nothing but its repr.
context = dagster.build_run_status_sensor_context(
    sensor_name="convalesce_success",
    dagster_event=success[0],
    dagster_instance=instance,
    dagster_run=result.dagster_run,
)
cedag.convalesce_sensor(context=context, emitter=_Recorder())

payload = sent[0]["payload"]
run = payload["dagster_run"]
# A DagsterRun is a namedtuple. Serialised as a plain tuple it arrives as an
# anonymous array with every field name lost, which has happened.
assert isinstance(run, dict), f"the run arrived as {type(run).__name__}"
assert run.get("job_name") == "nightly", run.get("job_name")
assert isinstance(payload["dagster_event"], dict), payload["dagster_event"]
assert payload["sensor_name"] == "convalesce_success"

print(f"PROBE_OK dagster {dagster.__version__}")

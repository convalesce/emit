"""The example workflow: a flow that works and one that does not, with the
hooks a customer attaches."""

from prefect import flow, task

from convalesce_emit_prefect import emit_flow_run, emit_task_run


@task(on_completion=[emit_task_run], on_failure=[emit_task_run])
def extract():
    return 42


@task(on_completion=[emit_task_run], on_failure=[emit_task_run])
def load(rows):
    raise RuntimeError(f"load failed on purpose after extracting {rows} rows")


@flow(name="nightly_ok", on_completion=[emit_flow_run], on_failure=[emit_flow_run])
def nightly_ok():
    return extract()


@flow(name="nightly_broken", on_completion=[emit_flow_run], on_failure=[emit_flow_run])
def nightly_broken():
    load(extract())


if __name__ == "__main__":
    nightly_ok()
    try:
        nightly_broken()
    except Exception as exc:  # the flow is meant to fail
        print("nightly_broken failed as intended:", exc)

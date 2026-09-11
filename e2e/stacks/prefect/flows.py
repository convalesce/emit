"""
The example workflow: a flow that works and one that does not, with the
hooks a customer attaches.

Run as a script inside the runner container, against the server.
"""

from prefect import flow, task

from convalesce_emit_prefect import emit_flow_run, emit_task_run


@task(on_completion=[emit_task_run], on_failure=[emit_task_run])
def extract() -> int:
    """Return a row count."""
    return 42


@task(on_completion=[emit_task_run], on_failure=[emit_task_run])
def load(rows: int) -> None:
    """Fail, so the failure path is exercised."""
    raise RuntimeError(f"load failed on purpose after extracting {rows} rows")


@flow(
    name="nightly_ok",
    on_completion=[emit_flow_run],
    on_failure=[emit_flow_run],
)
def nightly_ok() -> None:
    """The flow that succeeds."""
    extract()


@flow(
    name="nightly_broken",
    on_completion=[emit_flow_run],
    on_failure=[emit_flow_run],
)
def nightly_broken() -> None:
    """The flow that fails."""
    load(extract())


def main() -> None:
    """
    Run both flows; the second is meant to raise.

    :return: nothing
    """
    nightly_ok()
    try:
        nightly_broken()
    except RuntimeError as exc:
        print("nightly_broken failed as intended:", exc)


if __name__ == "__main__":
    main()

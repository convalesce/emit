# convalesce-emit-prefect

Forward Prefect flow and task run state to Convalesce, unchanged.

## Install

```sh
pip install convalesce-emit-prefect
```

## Wire it up

The package exports plain hook functions. Attach them to a flow or task
and Prefect calls them, by keyword on 2.x and positionally on 3.x:

```python
from prefect import flow, task
from convalesce_emit_prefect import emit_flow_run, emit_task_run


@task(on_completion=[emit_task_run], on_failure=[emit_task_run])
def extract():
    ...


@flow(on_completion=[emit_flow_run], on_failure=[emit_flow_run])
def nightly():
    extract()
```

Set `CONVALESCE_INGEST_KEY` where the flow runs.

Both functions also serve `on_running`, and `emit_flow_run` serves
`on_crashed` and `on_cancellation`. Wire those too: a flow that crashes or
is cancelled never fires `on_failure`, so without them its run never ends.

## Declare lineage

Prefect knows which task fed which, not which tables a task read or wrote.
Say so inside the task, and its task-run hook sends it:

```python
from convalesce_emit_prefect import lineage


@task(on_completion=[emit_task_run], on_failure=[emit_task_run])
def load():
    lineage(
        inputs=[{"platform": "postgres", "name": "shop.public.orders"}],
        outputs=["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t,PROD)"],
    )
```

Each dataset is a urn, or a `platform` and `name` with an optional `env`
(`PROD` by default).

On Prefect 3, a task's own assets need none of this: the task-run hook of
an `@materialize` task sends the assets it writes and the `asset_deps` it
reads, and the receiver follows assets handed on by upstream task runs the
way Prefect does.

A failed task's hook also sends the exception and its traceback, when
Prefect holds it in memory; a result persisted to storage is not read back.

## Supported

Prefect 2.20 and 3.x, on Python 3.9 and later. Verified on real installs:
see [version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

Each event also reads the Prefect API directly for the flow record, the run
graph, the full task-run list and, on Prefect Cloud, the workspace -- state a
hook's own arguments never carry. This is on by default and needs no
configuration beyond `CONVALESCE_INGEST_KEY`; set
`CONVALESCE_PREFECT_API_READS=false` to turn it off if this process's
Prefect API is locked down, and still get everything the hook's own
arguments already carry.

A flow's parameters are what launched the run typed, and can be customer
data. Only their names and types are sent (`{"since": "<str>"}`), for the
run and for the flow's defaults, and the payload says so in `excluded`. Set
`CONVALESCE_PREFECT_SEND_PARAMETERS=true` to send the values; anything named
like a credential is still redacted.

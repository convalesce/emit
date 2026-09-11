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

## Supported

Prefect 2.20 and 3.x, on Python 3.9 and later. Verified on real installs:
see [version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

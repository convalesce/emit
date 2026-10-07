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

## Name a run the task started

A task that starts a run in another tool, a Glue job say, can name it, and
its task-run hook sends it as `launched` so the two runs are linked by id:

```python
from convalesce_emit_prefect import launched


@task(on_completion=[emit_task_run], on_failure=[emit_task_run])
def aggregate():
    run_id = glue.start_job_run(JobName="lake_daily_agg")["JobRunId"]
    launched("glue", run_id, job="lake_daily_agg")
```

A task that simply returns the id needs none of this: a completed task's
result is sent as `result_text` when Prefect holds it in memory and it is a
`str`, `int`, `float` or `bool`, or a list of at most ten of them, cut to
200 characters. Anything else it returns is never sent. Set
`CONVALESCE_PREFECT_SEND_RESULT=false` to send no result at all.

## Supported

Prefect 2.20 and 3.x, on Python 3.9 and later. Verified on real installs:
see [version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

Each event also reads the Prefect API directly for the flow record, the run
graph, the full task-run list, the entrypoint of the deployment that started
the run and, on Prefect Cloud, the workspace -- state a hook's own arguments
never carry. This is on by default and needs no
configuration beyond `CONVALESCE_INGEST_KEY`; set
`CONVALESCE_PREFECT_API_READS=false` to turn it off if this process's
Prefect API is locked down, and still get everything the hook's own
arguments already carry.

## What a run says it ran

Each event also carries what the flow or task was, read from the process
that ran it:

- **Source.** The text of the flow's or task's function, with a hash of it
  and of its file, as `flow.source` and `task.source`. What ran is whatever
  was deployed, which is not always what the repository holds.
- **Arguments.** A flow run's parameter values, and the values each task was
  called with, as `arguments`: an argument that was another task's result is
  the value that task returned. Values are kept small. A list or mapping
  keeps its first 50 items, text its first 2,000 characters, a data frame or
  array is named by its type and shape and never read, and every cut is
  listed in the event's `excluded`. Anything named like a credential is
  redacted, as is a password inside a URL.
- **SQL.** The statements a task sent to a database, as `sql_capture`, so
  the tables and columns it read and wrote can be worked out. Only the
  statement text is kept, never the values bound to it. Tasks running side
  by side, in threads or on one event loop, each get their own statements;
  a statement the flow sends outside any task goes with the flow run. Seen
  through SQLAlchemy and through psycopg2, psycopg, Snowflake, MySQL,
  Redshift, Trino and DuckDB drivers, as each is imported.
- **Entrypoint.** For a run a deployment started, the deployment's name,
  entrypoint and path, as `api_deployment`.

Importing `convalesce_emit_prefect` is all it takes; the hooks above do the
rest. Import it before the flow opens its database connections.

All of it is on by default. Each part has a switch, set to `false`:

| Setting | Turns off |
| --- | --- |
| `CONVALESCE_SEND_SOURCE` | the source text |
| `CONVALESCE_SEND_ARGUMENTS` | task arguments and flow parameter values; parameter names and types still cross, as `{"since": "<str>"}` |
| `CONVALESCE_SQL_CAPTURE` | the SQL statements |

`CONVALESCE_PREFECT_SEND_PARAMETERS`, where it is already set, still decides
for flow parameter values alone: `false` withholds them and `true` sends
them, whatever `CONVALESCE_SEND_ARGUMENTS` says.

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
        outputs=["urn:cvl:dataset:(urn:cvl:dataPlatform:snowflake,db.s.t,PROD)"],
    )
```

Each dataset is a urn, or a `platform` and `name` with an optional `env`
(`PROD` by default) and an optional `platform_instance`, for a platform the
catalogue holds more than one instance of.

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

## Settings

Set `CONVALESCE_SEND_SETTINGS=true` where your flows run and the event a
flow run or a task run ends with, on completion, failure, a crash or a
cancellation, also carries the settings of its process: its environment
variables, and each Prefect Variable the process read with `Variable.get`
or `Variable.aget`. It is off by default.

A Variable is noted as your code reads it, from the moment your flow
imports `convalesce_emit_prefect`. A task run's event carries the Variables
read so far, and the flow run's last event carries them all.

A setting whose name or value reads as a credential, and any value longer
than 300 characters, is sent as a `fingerprint`: a keyed hash, sixteen hex
characters long, made in your process. It says that the value changed
between two runs and nothing of what the value is. Every other setting is
sent as its `value`.

The hash is keyed with `CONVALESCE_FINGERPRINT_KEY` where you set one, and
with a key derived from your ingest key otherwise. The fingerprint key is
only ever read in your process and is never sent. `keyed_by` identifies the
key the hashes were made under, so hashes are compared only between runs
that used the same one.

Set `CONVALESCE_SETTINGS_SKIP` to a comma-separated list of names to keep
those settings out in either form, such as
`CONVALESCE_SETTINGS_SKIP=HOSTNAME,INTERNAL_REGION`.

The event then holds:

```json
{
  "settings": {
    "items": [
      {"kind": "environment", "name": "API_KEY", "fingerprint": "5d1c0a9e7b3f2468"},
      {"kind": "environment", "name": "WAREHOUSE", "value": "analytics"},
      {"kind": "variable", "name": "region", "value": "eu"}
    ],
    "keyed_by": "9f3b6c1d2e4a5b70"
  }
}
```

## Supported

Prefect 2.20 and 3.x, on Python 3.9 and later. Verified on real installs:
see [version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

Each event also reads the Prefect API directly for the flow record, the run
graph, the full task-run list, the entrypoint of the deployment that started
the run and, on Prefect Cloud, the workspace: state a hook's own arguments
never carry. This is on by default and needs no configuration beyond
`CONVALESCE_INGEST_KEY`; set `CONVALESCE_PREFECT_API_READS=false` to turn it
off if this process's Prefect API is locked down, and still get everything
the hook's own arguments already carry.

- **Workspace.** The account and workspace are read from the API URL
  Prefect is configured with, whether a profile (`prefect cloud login`) or
  `PREFECT_API_URL` holds it. The workspace's name is read from Prefect
  Cloud once per process, with the API key Prefect is already configured
  with, and waited on for three seconds at most.
- **Run graph.** Read from `graph-v2`, which Prefect 2.20 and every 3.x
  server serve, and sent as `api_flow_run_graph_v2`: each task run and
  subflow run of the flow run, with its parents. Artifacts are left out. A
  server that does not answer for it is asked for the older graph, sent as
  `api_flow_run_graph`.
- **Task runs.** Prefect 3 reports a task run to the API after the fact,
  so the API can list a run's task runs a few seconds late. The hook that
  ends a flow run (completion, failure, crash or cancellation) waits for
  the list to catch up: until every task run in the graph, and every task
  run this process saw, is listed in a state that ended it. It reads again
  after 0.25, 0.5, 1, 1.5 and 1.5 seconds and stops at five seconds in
  total, counting the reads themselves, then sends what it has. Task hooks
  and `on_running` never wait.

Left out of every event: who started a run beyond `created_by.type` (the
user's id and handle stay behind), and where a persisted result is stored.
The plugin's own log lines mask the account and workspace ids of a Prefect
Cloud URL.

## What a run says it ran

Each event also carries what the flow or task was, read from the process
that ran it:

- **Source.** The text of the flow's or task's function, with a hash of it
  and of its file, as `flow.source` and `task.source`. What ran is whatever
  was deployed, which is not always what the repository holds.
- **Arguments.** A flow run's parameter values, and the values each task was
  called with, as `arguments`: an argument that was another task's result is
  the value that task returned, and so is each result of a mapped task. A
  result that is not in memory yet is sent as its task run's id. Values are
  kept small. A list or mapping
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

# convalesce-emit-airflow

Forward Airflow run events to Convalesce, unchanged.

## Install

```sh
pip install convalesce-emit-airflow
```

That is the whole integration. The package registers a listener through
Airflow's `airflow.plugins` entry point, so nothing in a DAG changes.
Set `CONVALESCE_INGEST_KEY` on the worker and every task instance and
dag run event is forwarded as it happens.

## Supported

Airflow 2.5 through 3.0 in this one release, on Python 3.9 and later.
The listener generates its hooks from the hookspecs of the Airflow it
runs in, so a release that adds or removes a hook argument cannot break
it. Airflow passes a task's failure message to listeners from 2.10.

Verified on real installs: see
[version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## OpenLineage

Where Airflow's OpenLineage provider
(`apache-airflow-providers-openlineage`) is installed and OpenLineage is not
configured, the plugin points the provider at its own transport, and each
lineage event the provider builds (tables read and written, parsed SQL,
columns) is forwarded as observation `openlineage`. It does this by setting
`AIRFLOW__OPENLINEAGE__TRANSPORT` when the plugin loads.

On Airflow 2.10 and later it also registers Airflow's hook lineage reader, so
files and tables a hook touches from inside a task (S3, GCS, object storage,
SQL run through a hook) reach the same events.

To link a Spark job an Airflow task submits to that task, set
`AIRFLOW__OPENLINEAGE__SPARK_INJECT_PARENT_JOB_INFO=true`. The provider then
names the task as the Spark job's parent; it changes the job's Spark
configuration, so it is left for you to switch on.

It never replaces a setup you made: any of `[openlineage] transport`,
`[openlineage] config_path`, `[openlineage] disabled`, `OPENLINEAGE_URL`,
`OPENLINEAGE_CONFIG`, `OPENLINEAGE_DISABLED`, `OPENLINEAGE__TRANSPORT__*` or an
`openlineage.yml` leaves OpenLineage as it was. To use a transport of your
own and forward to Convalesce too, name this one in it:

```json
{"type": "convalesce_emit_airflow.openlineage.ConvalesceTransport"}
```

Set `CONVALESCE_OPENLINEAGE=false` to switch this off.

## Choose which DAGs are reported

Every DAG is reported unless you say otherwise. Two settings choose by DAG
id, each a comma-separated list of shell-style patterns (`*` for any run of
characters, `?` for one, `[abc]` for one of a set), matched against the
whole id with its case kept:

- `CONVALESCE_AIRFLOW_DAG_DENY`: a DAG whose id matches is left out.
- `CONVALESCE_AIRFLOW_DAG_ALLOW`: when set, only a DAG whose id matches is
  reported.

A DAG that matches both is left out.

```sh
CONVALESCE_AIRFLOW_DAG_ALLOW="orders_*,billing"
CONVALESCE_AIRFLOW_DAG_DENY="orders_scratch"
```

Nothing is sent for a DAG that is left out: its task and dag run events, the
asset events its tasks raise, and the OpenLineage events the provider builds
for it. Set them on the scheduler and on every worker.

## Connections

For each connection a task names, the plugin sends its type, host, port and
schema, and these names from its `extra` when they are set: `database`,
`schema`, `warehouse`, `role`, `catalog`, `project` and `dataset`. They say
which database a table named without one belongs to. Each value passes
through the same redaction as the rest of the event.

The same is sent for the connection of each database hook a task builds in
its own code, such as a `PostgresHook` inside a `@task`, and each statement
that hook ran carries the connection's id as `conn_id`.

## Settings

Set `CONVALESCE_SEND_SETTINGS=true` on your workers and each task's success
or failed event also carries the settings the task ran with: the environment
variables of its process and the Airflow Variables it read. It is off by
default.

A Variable is noted as the task reads it, in a template such as
`{{ var.value.my_var }}` or from the task's own code. A Variable the task
depends on without reading it through Airflow can be named in
`CONVALESCE_SETTINGS_VARIABLES`, a comma-separated list of exact names, up
to 50. Each is read once when the task ends.

A setting whose name or value reads as a credential, and any value longer
than 300 characters, is sent as a `fingerprint`: a keyed hash, sixteen hex
characters long, made on your worker. It says that the value changed
between two runs and nothing of what the value is. Every other setting is
sent as its `value`.

The hash is keyed with `CONVALESCE_FINGERPRINT_KEY` where you set one, and
with a key derived from your ingest key otherwise. The fingerprint key is
only ever read on your worker and is never sent. `keyed_by` identifies the
key the hashes were made under, so hashes are compared only between runs
that used the same one.

Set `CONVALESCE_SETTINGS_SKIP` to a comma-separated list of names to keep
those settings out in either form.

The event then holds:

```json
{
  "settings": {
    "items": [
      {"kind": "environment", "name": "DB_PASSWORD", "fingerprint": "5d1c0a9e7b3f2468"},
      {"kind": "environment", "name": "STAGE", "value": "prod"},
      {"kind": "variable", "name": "region", "value": "eu-west-1"}
    ],
    "keyed_by": "9f3b6c1d2e4a5b70"
  }
}
```

With `CONVALESCE_SEND_ARGUMENTS=false`, an operator's templated fields
(`bash_command`, `sql`, `env` and whatever else it declares) are withheld
along with its arguments, since a rendered template holds the values it
read.

## Configure

`CONVALESCE_INGEST_KEY`, `CONVALESCE_ENDPOINT`, `CONVALESCE_DRY_RUN` and the
rest are read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

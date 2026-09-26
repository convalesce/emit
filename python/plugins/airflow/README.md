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

## Configure

`CONVALESCE_INGEST_KEY`, `CONVALESCE_ENDPOINT`, `CONVALESCE_DRY_RUN` and the
rest are read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

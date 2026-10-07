# convalesce-emit-dagster

Forward Dagster run status to Convalesce, unchanged.

## Install

```sh
pip install convalesce-emit-dagster
```

## Wire it up

The package exports a sensor body rather than a decorated sensor, because
Dagster's decorator signatures move between versions and this package
does not depend on Dagster. Apply the decorator in your own definitions:

```python
from dagster import DagsterRunStatus, Definitions, run_status_sensor
from convalesce_emit_dagster import convalesce_sensor


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def convalesce_on_success(context):
    convalesce_sensor(context)


@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def convalesce_on_failure(context):
    convalesce_sensor(context)


@run_status_sensor(run_status=DagsterRunStatus.CANCELED)
def convalesce_on_canceled(context):
    convalesce_sensor(context)


defs = Definitions(
    sensors=[convalesce_on_success, convalesce_on_failure, convalesce_on_canceled]
)
```

Each fires with the run, the event and the sensor name, as Dagster
produced them. A cancelled run is recorded as skipped. Set
`CONVALESCE_INGEST_KEY` where the daemon runs.

## Links to the Dagster UI

Set `CONVALESCE_DAGSTER_URL` where your sensor runs to the address you open
the Dagster UI at, such as `https://dagster.example.com`, or
`https://my-org.dagster.cloud` on Dagster+. Each job, op, run and step in
Convalesce then links to its page in Dagster. On Dagster+ the link goes
through the deployment the run ran in, a branch deployment included.

## What each step ran

With the run, the sensor sends one entry per executed step:

- the source of the op or asset function behind it, read from your
  definitions;
- the SQL statements it ran through SQLAlchemy or a database driver, as
  text, with the dialect and database and without bound values;
- the configuration it was resolved with.

The statements and the configuration are noted in the process that runs
the step and written to the run's event log as one engine event per step,
which the sensor reads back. This starts when your definitions import
`convalesce_emit_dagster`, on the multiprocess and the in-process
executor alike. Secrets are masked before anything is written or sent.

Each is on by default and has its own switch, set where your steps and
your sensor run: `CONVALESCE_SEND_SOURCE`, `CONVALESCE_SQL_CAPTURE` and
`CONVALESCE_SEND_ARGUMENTS` (`false` turns one off).

## What an asset's author attached

Metadata on a materialisation, an observation, an asset check and a
`Failure` is sent in two parts:

- the entries that describe a table, as they are: row counts, the table
  name and URI, the column schema and column lineage, the code version and
  the SQL an asset ran;
- every other entry whose value is a number, a flag or a timestamp, under
  its own name: `MetadataValue.int`, `.float`, `.bool` and `.timestamp`,
  or the bare value. Up to 50 per event, with names up to 128 characters.

Text, markdown, JSON, tables, paths and links are replaced by a count of
what was withheld. Set `CONVALESCE_DAGSTER_SEND_METADATA=false` where your
sensor runs to send the first part only.

## Declare what an op reads and writes

An asset names its own table. For an op whose datasets appear nowhere in
its code or metadata, pass them to the sensor as `lineage`, keyed by op
name:

```python
LINEAGE = {
    "load_orders": {
        "inputs": ["s3://my-bucket/exports/orders"],
        "outputs": [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
        ],
    },
}


@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def convalesce_on_success(context):
    convalesce_sensor(context, lineage=LINEAGE)
```

A dataset is named by its urn, as in the `convalesce.inputs` and
`convalesce.outputs` metadata of an op's inputs and outputs, or by the URI
of a path in an object store. `lineage` may also be a function of the
sensor's context that returns such a mapping, for a declaration that
depends on the run.

## Supported

Dagster 1.7 and later, on Python 3.9 and later. Verified on real installs:
see [version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

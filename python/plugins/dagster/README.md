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


defs = Definitions(sensors=[convalesce_on_success, convalesce_on_failure])
```

Each fires with the run, the event and the sensor name, as Dagster
produced them. Set `CONVALESCE_INGEST_KEY` where the daemon runs.

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

## Supported

Dagster 1.7 and later, on Python 3.9 and later. Verified on real installs:
see [version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

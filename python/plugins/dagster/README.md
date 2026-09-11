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

## Supported

Dagster 1.7 and later, on Python 3.9 and later. Verified on real installs:
see [version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

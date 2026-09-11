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

## Configure

`CONVALESCE_INGEST_KEY`, `CONVALESCE_ENDPOINT`, `CONVALESCE_DRY_RUN` and the
rest are read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

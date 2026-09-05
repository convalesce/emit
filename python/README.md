# convalesce-emit (Python)

Forwards a data tool's own output to Convalesce, unchanged. Airflow, Dagster, Prefect and Great
Expectations.

See the [repository README](../README.md) for what these do and why, and
[docs/version-support.md](../docs/version-support.md) for the versions each was verified against.

## Install

```sh
pip install convalesce-emit-airflow   # or -dagster, -prefect, -gx
```

## Develop

```sh
make lint
make test
```

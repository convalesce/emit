# convalesce-emit

Forwards a data tool's own output to Convalesce, unchanged. This is the
transport the tool plugins share: Airflow, Dagster, Prefect and Great
Expectations each install it as their one dependency.

It has **no dependencies of its own**. Transport is `urllib` from the
standard library, so it installs into an existing pipeline environment
without touching a resolution that already works.

## Install

```sh
pip install convalesce-emit-airflow   # or -dagster, -prefect, -gx
```

## Configure

Set these on the worker; no code changes are needed.

| Variable | Default | |
| --- | --- | --- |
| `CONVALESCE_INGEST_KEY` | none | required unless dry-running |
| `CONVALESCE_ENDPOINT` | `https://api.convalesce.io` | |
| `CONVALESCE_DRY_RUN` | `false` | build envelopes, log them, send nothing |
| `CONVALESCE_ENABLED` | `true` | set `false` to switch off entirely |
| `CONVALESCE_BATCH_SIZE` | `50` | |
| `CONVALESCE_MAX_RETRIES` | `3` | |
| `CONVALESCE_TIMEOUT` | `10` | seconds |

Nothing names an account: the ingest key carries that.

## Links

- [Repository](https://github.com/convalesce/emit), with what these
  plugins do and why.
- [Versions verified](https://github.com/convalesce/emit/blob/main/docs/version-support.md),
  for each tool, on real installs.
- [Develop](https://github.com/convalesce/emit/blob/main/python/README.md):
  `make lint`, `make test`.

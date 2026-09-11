# convalesce-emit

[![ci](https://github.com/convalesce/emit/actions/workflows/ci.yml/badge.svg)](https://github.com/convalesce/emit/actions/workflows/ci.yml)
[![e2e](https://github.com/convalesce/emit/actions/workflows/e2e.yml/badge.svg)](https://github.com/convalesce/emit/actions/workflows/e2e.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE)

Client libraries that forward a data tool's own output to Convalesce, unchanged.

| | |
| --- | --- |
| [`python/`](python) | Airflow, Dagster, Prefect, Great Expectations |
| [`java/`](java) | Spark |

Both speak the same envelope and read the same environment variables, so one receiver and one set
of docs cover both.

## What they do

Each one connects and sends. It does not parse, map, or resolve anything:
whatever the tool handed the callback goes across as-is, and every bit of interpretation happens after it arrives. That is
the point. Improving how a payload is understood never requires anyone to
upgrade a package inside their pipeline.

## Install

```sh
pip install convalesce-emit-airflow   # or -dagster, -prefect, -gx
```

For Spark, two lines of config rather than a pip install. See [`java/`](java).

Each plugin pulls in `convalesce-emit`, which has **no dependencies of its
own**: transport is `urllib` from the standard library. It installs into an
existing Airflow or Dagster environment without touching a resolution that
already works.

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

Try it against a real pipeline before pointing it at an account:
`CONVALESCE_DRY_RUN=true`.

## What gets sent

```json
{ "envelope_version": 1, "observation_id": "…", "emitted_at": "…",
  "tool": "airflow", "event": "task_instance_failed", "tool_version": "2.9.1",
  "client_version": "0.1.0",
  "payload": { "…the tool's own output, untouched…" } }
```

Only the fields around `payload` are ours, and they exist so the receiver knows
what it is holding. Nothing names an account: the ingest key carries that, so
an observation cannot claim to be from an account it is not.

## Great Expectations and row values

A GX validation result carries sample failing values (`partial_unexpected_list`
and its siblings) which are real rows from your table. Those are **replaced by
their counts before anything leaves the process**, because Convalesce reads
table shapes, run outcomes, row counts and lineage, not the rows themselves.

Set `CONVALESCE_GX_SEND_SAMPLES=true` to send them anyway, knowingly.

## Tool versions

One package per tool, no per-version build. Verified against real installs:

| Tool | Verified |
| --- | --- |
| Airflow | 2.5 - 2.11, 3.0 |
| Dagster | 1.7 - 1.13 |
| Prefect | 2.20, 3.1, 3.8 |
| Great Expectations | 0.17, 0.18, 1.22 |
| Spark | 3.3, 3.5, 4.0 |
| Python | 3.9+ |

One release per tool covers its whole row: Airflow 2.5 to 3.0 in a single artifact, Spark on
Java 8 and 11 clusters, Great Expectations 0.x and 1.x behind one import.

Each of those versions also runs for real in Docker, on its own scheduler, daemon or server,
with an example workflow and a stand-in endpoint: see [`e2e/`](e2e).

Nothing here reads a field off a tool's object, so a renamed attribute is the
receiver's problem rather than a reason to ship a second package. Where a
tool's own plugin API changed incompatibly -- Great Expectations between 0.x
and 1.x -- there are two classes and the installed version picks one.

`docs/version-support.md` has the details, including three Airflow bugs that
only registering with a real Airflow would have found.

## Failure

Nothing here raises into your pipeline. If our endpoint is down, the
observation is logged and dropped. A DAG must not go red because we had a bad
minute: we are watching your pipeline, not standing in it.

## Contributing

Plugins for more tools are the most useful contribution, and the open
[`tool-plugin` issues](https://github.com/convalesce/emit/issues?q=is%3Aissue+is%3Aopen+label%3Atool-plugin)
list the ones with a known push mechanism. [CONTRIBUTING.md](CONTRIBUTING.md)
has the gates, the style and the recipe for a new plugin.

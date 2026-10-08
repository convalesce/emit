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
A PySpark driver that fails in Python before Spark runs anything still ends
its application as a success; `convalesce-emit-pyspark` reports it. See
[`python/plugins/pyspark`](python/plugins/pyspark).

Each plugin pulls in `convalesce-emit`, which has **no dependencies of its
own**: transport is `urllib` from the standard library. It installs into an
existing Airflow or Dagster environment without touching a resolution that
already works.

## Configure

Set these on the worker; no code changes are needed.

| Variable | Default | |
| --- | --- | --- |
| `CONVALESCE_INGEST_KEY` | none | required unless dry-running |
| `CONVALESCE_ENDPOINT` | `https://api.convalesce.io` | for a self-hosted collect, its GMS URL ending in `/openapi`, e.g. `http://localhost:8080/openapi` |
| `CONVALESCE_DRY_RUN` | `false` | build envelopes, log them, send nothing |
| `CONVALESCE_ENABLED` | `true` | set `false` to switch off entirely |
| `CONVALESCE_BATCH_SIZE` | `50` | observations per request; at most 50, the receiver's limit |
| `CONVALESCE_MAX_BODY_BYTES` | `1000000` | a batch is sent before the body would pass this; at most 5000000 |
| `CONVALESCE_MAX_RETRIES` | `3` | |
| `CONVALESCE_TIMEOUT` | `10` | seconds |
| `CONVALESCE_SPOOL_DIR` | `<temp dir>/convalesce-emit-spool` | undelivered batches wait here and are sent after the next send that succeeds; refused ones are kept under `rejected/` |
| `CONVALESCE_SPOOL_MAX_BYTES` | `1000000000` | the waiting batches stop growing past this, with an error in the log |

Try it against a real pipeline before pointing it at an account:
`CONVALESCE_DRY_RUN=true`.

Each plugin adds a few settings of its own. Its README has the detail:

| Variable | Plugin | Default | |
| --- | --- | --- | --- |
| `CONVALESCE_AIRFLOW_DAG_ALLOW` | [Airflow](python/plugins/airflow) | every DAG | comma-separated shell-style patterns; when set, only a DAG whose id matches is reported |
| `CONVALESCE_AIRFLOW_DAG_DENY` | [Airflow](python/plugins/airflow) | none | a DAG whose id matches is left out, also when it matches the allow list |
| `CONVALESCE_DAGSTER_URL` | [Dagster](python/plugins/dagster) | none | the address the Dagster UI is opened at, so each job, op, run and step links to its page |
| `CONVALESCE_DAGSTER_SEND_METADATA` | [Dagster](python/plugins/dagster) | `true` | `false` sends only the metadata entries that describe a table |
| `CONVALESCE_DAGSTER_SEND_LOCAL_PATHS` | [Dagster](python/plugins/dagster) | `false` | `true` also sends `path` metadata that names a local file |
| `CONVALESCE_GX_SEND_SAMPLES` | [Great Expectations](python/plugins/gx) | `true` | `false` replaces failing sample values and observed values by their counts |
| `CONVALESCE_PYSPARK_DRIVER_HOOK` | [PySpark](python/plugins/pyspark) | `false` | `true` turns the driver hook on with no code change |

AWS Glue hands a job only the variables whose names start with `CUSTOMER_`,
so every setting is also read with that in front. Give them in the job
parameter `--customer-driver-env-vars`, separated by commas:
`CUSTOMER_CONVALESCE_INGEST_KEY=...,CUSTOMER_CONVALESCE_ENDPOINT=...`. A
setting given under both names is read from its own.

## Check the connection

Run this where the tool runs, with the same environment it has:

```sh
convalesce-emit check                      # or: python -m convalesce_emit check
java -jar convalesce-emit-core-0.2.1.jar   # on a Spark driver's host
```

It sends one empty batch with the configured key and says whether the key was
accepted, refused, or never reached us. Once it passes, the console shows the
key as heard from.

## What gets sent

```json
{ "envelope_version": 1, "observation_id": "…", "emitted_at": "…",
  "tool": "airflow", "event": "task_instance_failed", "tool_version": "2.9.1",
  "client_version": "0.1.1",
  "payload": { "…the tool's own output, untouched…" } }
```

Only the fields around `payload` are ours, and they exist so the receiver knows
what it is holding. Nothing names an account: the ingest key carries that, so
an observation cannot claim to be from an account it is not.

Beside the tool's own event, by default:

- **Airflow**: for each connection a task names, its type, host, port and
  schema, and the `database`, `schema`, `warehouse`, `role`, `catalog`,
  `project` and `dataset` of its `extra` when set, which say which database a
  table named without one belongs to. Every DAG, unless
  `CONVALESCE_AIRFLOW_DAG_ALLOW` or `CONVALESCE_AIRFLOW_DAG_DENY` chooses.
- **Dagster**: the metadata entries that describe a table, and every other
  entry whose value is a number, a flag or a timestamp. Text, markdown, JSON,
  tables, paths and links are replaced by a count.
  `CONVALESCE_DAGSTER_SEND_METADATA=false` sends the first part only.
- **Great Expectations**: the failing values a check sampled and the values
  it observed in a column. See below.
- **Prefect**: on Prefect Cloud, the workspace's name.
- **PySpark**: with the driver hook on, the script the driver ran, its
  arguments, and the Python error if it failed.

Secrets are masked before anything leaves the process.

## Great Expectations and row values

A GX validation result holds some of the values of the table it checked: the
failing values a check sampled (`partial_unexpected_list` and its siblings,
20 for a check by default), what a fuller result format adds, and the values
a check observed in a column. They are **sent by default**, because they are
what tells an investigation which rows broke a check. The rows that passed,
and the frame that was validated, stay where they are.

Set `CONVALESCE_GX_SEND_SAMPLES=false` where the checkpoint runs to send none
of them: each value or list of values is then replaced by how many there
were. Counts, percentages and numeric statistics are sent either way. See
[`python/plugins/gx`](python/plugins/gx).

## Tool versions

One package per tool, no per-version build. Verified against real installs:

| Tool | Verified |
| --- | --- |
| Airflow | 2.5 - 2.11, 3.0 - 3.2 |
| Dagster | 1.7 - 1.13 |
| Prefect | 2.20, 3.1, 3.4, 3.8 |
| Great Expectations | 0.17, 0.18, 1.22, 1.23 |
| Spark | 3.3, 3.4, 3.5, 4.0 |
| Python | 3.9+ |

One release per tool covers its whole row: Airflow 2.5 to 3.2 in a single artifact, Spark on
Java 8 and 11 clusters, Great Expectations 0.x and 1.x behind one import.

Each of those versions also runs for real in Docker, on its own scheduler, daemon or server,
with an example workflow, against a real receiver: see
[`collect/e2e-observe/`](../collect/e2e-observe) in the sibling `collect` checkout, which is
where the integration tests live.

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

## Releasing

[docs/releasing.md](docs/releasing.md) has the steps. The two registries a
release lands in, for whoever is cutting one:

- **PyPI**: [log in](https://pypi.org/account/login/), then
  [trusted publishers](https://pypi.org/manage/account/publishing/) lists the
  package each workflow environment may upload, and
  [your projects](https://pypi.org/manage/projects/) lists what is published.
- **Maven Central**: [log in](https://central.sonatype.com/account), then
  [deployments](https://central.sonatype.com/publishing/deployments) holds the
  jars a release uploaded. They stay in staging until someone presses
  Publish there, and take a while to appear on
  [repo1](https://repo1.maven.org/maven2/io/convalesce/) afterwards.

## Contributing

Plugins for more tools are the most useful contribution, and the open
[`tool-plugin` issues](https://github.com/convalesce/emit/issues?q=is%3Aissue+is%3Aopen+label%3Atool-plugin)
list the ones with a known push mechanism. [CONTRIBUTING.md](CONTRIBUTING.md)
has the gates, the style and the recipe for a new plugin.

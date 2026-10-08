# convalesce-emit-pyspark

Report a PySpark driver to Convalesce: the script it ran, what it was called
with, and whether it failed in Python.

A driver can fail before Spark runs anything, say by reading a table that is
not there. Spark still ends the application normally, so the listener that
`convalesce-emit-spark` installs reports a success. This package hooks the
driver's own Python and sends one `driver_failure` observation, which marks
the application's run failed, with the Python exception and its traceback.

Whether it failed or not, it also sends one `driver_script` observation as
the driver exits: the script's text and the arguments it was given. What ran
is the file on the machine that ran it, which a repository can only suggest.

## Install

Into the Python the driver runs on:

```sh
pip install convalesce-emit-pyspark
```

## Turn it on

With no code change, set this where `spark-submit` runs:

```sh
export CONVALESCE_PYSPARK_DRIVER_HOOK=true
```

The package ships a `.pth` file, which Python runs at start-up. It reads that
one variable and does nothing else unless it is `true`, so installing the
package changes nothing until you set it.

Or in the driver, before anything can fail:

```python
import convalesce_emit_pyspark

convalesce_emit_pyspark.install()
```

Either way, set `CONVALESCE_INGEST_KEY` too; without it nothing is hooked.
Run it beside `convalesce-emit-spark`, which reports the application itself.

## What is reported

### `driver_script`, once per driver

Sent as the interpreter exits, after a run that succeeded and after one that
failed, for a driver that started a Spark application:

- the application's id and name
- `argv`: the script and its arguments, with credentials masked
  (`--password hunter2` and `--url=postgresql://etl:hunter2@db/shop` both
  lose the password)
- `source`: the script's path, its text with credentials masked the same
  way, and the sha256 of the file as it is on the driver's machine. Text
  over 60,000 characters is cut from the end and marked `truncated`; the
  hash is always of the whole file
- the Python and PySpark versions

Two settings, both on unless set to `false`, `0`, `no` or `off`:

| Variable | Off means |
| --- | --- |
| `CONVALESCE_SEND_SOURCE` | no `source` |
| `CONVALESCE_SEND_ARGUMENTS` | `argv` holds the script's path only, here and in `driver_failure` |

### `driver_failure`, at most once per driver

For whichever comes first:

- an exception nobody caught, in the main thread or another thread
- `sys.exit(n)` with `n` not zero, sent as a `SystemExit`

It carries the application's id and name, the exception (type, message,
traceback, and its cause), `argv` as above, and the Python and PySpark
versions.

### Which run of the platform, on both

- `databricks`, on Databricks: the job run's id, the job's id, the notebook's
  path, the cluster's id, and the workspace's id and URL, each one the
  session says. A serverless session says the cluster and not the job run.
  The job run's id puts a failure on the right job run of a cluster that
  several job runs share.
- `attempt`, in a YARN container: which try of the application the driver
  is, from 1. YARN runs a failed cluster-mode driver again under the same
  application id, and each try reports.

Both observations are sent before the driver exits, which delays that exit by at most
`CONVALESCE_TIMEOUT` per attempt. The hook never raises and never changes
the exit code; the exception still prints as it would have.

A driver usually stops its `SparkSession` before it exits, and a stopped
session no longer names its application. The hook wraps `SparkContext.stop`
to read the application's id and name first, and changes nothing else about
it.

Limits:

- `raise SystemExit(n)` is not seen, only `sys.exit(n)`
- a `sys.exit(n)` whose `SystemExit` the driver catches and swallows is
  still reported
- `source` is the script the driver was started with (`spark-submit job.py`),
  not the modules it imports. Where a platform's launcher runs the script
  (AWS Glue's `runscript.py`), `source` is the script and never the launcher

### Notebooks

A notebook reports a failure. The cell that raises sends `driver_failure`,
and on Spark Connect (Databricks serverless) the run's start before it and
its end after it. A notebook has no script, so it sends no `driver_script`,
and a cell that passes sends nothing and ends nothing: a notebook that
never fails sends nothing.

On Spark Connect the run is named by the last part of the notebook's path
where the session says it, else by the session's `spark.app.name`. A Python
file task is named by its script, and sends `driver_script` as it always
did.

### A driver with no Spark session

A cluster-mode driver on YARN can fail before it starts a Spark session.
YARN has made the application and no listener is running to report it, so
the hook reports the whole run: `SparkListenerApplicationStart` (named by
the script's file name, timed from when the hook went in), `driver_failure`,
`driver_script`, then `SparkListenerApplicationEnd`, each with `attempt`.
Once a driver has asked for a Spark context the listener reports the
application and the hook sends only its own two observations.

### The ingest key

The key is sent in the `Authorization` header only. Its value is masked
(`***`) anywhere else it turns up, such as a script with the key written
into it, and the observation's `excluded` says so
(`{"path": "$", "reason": "ingest key masked"}`). A key under 8 characters
is too short to look for.

## Supported

PySpark 3.3 to 4.0, on Python 3.9 and later.

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

On AWS Glue, give every setting with `CUSTOMER_` in front, in the job
parameter `--customer-driver-env-vars`:
`CUSTOMER_CONVALESCE_INGEST_KEY=...,CUSTOMER_CONVALESCE_PYSPARK_DRIVER_HOOK=true`.
Glue installs `--additional-python-modules` where Python runs the `.pth`
file, so that last one turns the hook on with no code change.
`driver_script` carries the job's script either way, whether the variable
or a call to `install()` turned the hook on.

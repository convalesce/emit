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

Both are sent before the driver exits, which delays that exit by at most
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
  not the modules it imports

## Supported

PySpark 3.3 to 4.0, on Python 3.9 and later.

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

On AWS Glue, give every setting with `CUSTOMER_` in front, in the job
parameter `--customer-driver-env-vars`:
`CUSTOMER_CONVALESCE_INGEST_KEY=...,CUSTOMER_CONVALESCE_PYSPARK_DRIVER_HOOK=true`.
Glue installs `--additional-python-modules` where Python runs the `.pth`
file, so that last one turns the hook on with no code change. Call
`install()` from the job's script instead to have `driver_script` carry
that script.

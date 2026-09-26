# convalesce-emit-pyspark

Report a PySpark driver that fails in Python to Convalesce.

A driver can fail before Spark runs anything, say by reading a table that is
not there. Spark still ends the application normally, so the listener that
`convalesce-emit-spark` installs reports a success. This package hooks the
driver's own Python and sends one `driver_failure` observation, which marks
the application's run failed, with the Python exception and its traceback.

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

At most one observation per driver, for whichever comes first:

- an exception nobody caught, in the main thread or another thread
- `sys.exit(n)` with `n` not zero, sent as a `SystemExit`

Each carries the application's id and name, the exception (type, message,
traceback, and its cause), the script and its arguments with credentials
masked, and the Python and PySpark versions. It is sent before the driver
exits, which delays that exit by at most `CONVALESCE_TIMEOUT` per attempt.
The hook never raises and never changes the exit code; the exception still
prints as it would have.

Limits:

- `raise SystemExit(n)` is not seen, only `sys.exit(n)`
- a `sys.exit(n)` whose `SystemExit` the driver catches and swallows is
  still reported
- a driver that stops its `SparkSession` before the exception escapes (in a
  `finally`, say) is reported without the application's id and name, and
  cannot be matched to its run

## Supported

PySpark 3.x, on Python 3.9 and later.

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

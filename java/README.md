# convalesce-emit (Java)

Forwards Spark's own listener events to Convalesce, unchanged.

## Install

Two lines of Spark config:

```
spark.jars.packages   io.convalesce:convalesce-emit-spark:0.1.0
spark.extraListeners  io.convalesce.emit.spark.ConvalesceSparkListener
```

Or on the command line:

```sh
spark-submit \
  --packages io.convalesce:convalesce-emit-spark:0.1.0 \
  --conf spark.extraListeners=io.convalesce.emit.spark.ConvalesceSparkListener \
  your_job.py
```

Configure it with the same environment variables the Python client uses:
`CONVALESCE_INGEST_KEY`, `CONVALESCE_ENDPOINT`,
`CONVALESCE_DRY_RUN`, `CONVALESCE_ENABLED`, `CONVALESCE_BATCH_SIZE`,
`CONVALESCE_MAX_RETRIES`, `CONVALESCE_TIMEOUT`.

## What it sends

Whatever Spark's own `JsonProtocol` produced for the event, wrapped in the same envelope the
Python client uses. Nothing here reads a field off an event.

## Why it is small

Spark already knows how to render its own events -- `JsonProtocol` is what writes the event log --
so this forwards a string Spark produced rather than walking an object graph. A listener that
mapped events into some other shape would have to be upgraded in your cluster every time that
shape changed. This one does not.

## Constraints, and why

- **No dependencies.** This jar loads into someone else's Spark driver; anything it brought could
  collide with what that cluster already runs. That is why the JSON is written by hand and the
  transport is `HttpURLConnection`.
- **Java 8 bytecode.** Every Spark 3.x cluster can load it, including those still on Java 8 or 11:
  EMR 6.x, Databricks 15.4 LTS and below, and older Glue.
- **One jar for both Scala builds.** The only Spark API it touches takes and returns plain Java
  types, so there is no `_2.12` / `_2.13` split.
- **Nothing escapes into the job.** Every callback wraps its body; a failure to emit is logged and
  dropped. Verified: with the endpoint unreachable, `spark-submit` still exits 0.

## Build

```sh
./gradlew build          # compiles, formats, tests
./gradlew legacyTest     # the Spark 3.3 path, against real 3.3 jars
```

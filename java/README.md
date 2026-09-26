# convalesce-emit (Java)

Forwards Spark's own listener events to Convalesce, unchanged.

## Install

Two lines of Spark config:

```
spark.jars.packages   io.convalesce:convalesce-emit-spark:0.1.1
spark.extraListeners  io.convalesce.emit.spark.ConvalesceSparkListener
```

Or on the command line:

```sh
spark-submit \
  --packages io.convalesce:convalesce-emit-spark:0.1.1 \
  --conf spark.extraListeners=io.convalesce.emit.spark.ConvalesceSparkListener \
  your_job.py
```

Configure it with the same environment variables the Python client uses:
`CONVALESCE_INGEST_KEY`, `CONVALESCE_ENDPOINT`,
`CONVALESCE_DRY_RUN`, `CONVALESCE_ENABLED`, `CONVALESCE_BATCH_SIZE`,
`CONVALESCE_MAX_RETRIES`, `CONVALESCE_TIMEOUT`,
`CONVALESCE_MAX_BODY_BYTES`, `CONVALESCE_SPOOL_DIR`,
`CONVALESCE_SPOOL_MAX_BYTES`. `CONVALESCE_FLUSH_INTERVAL` (seconds, default
`5`) sends a part batch in the background, and the JVM's shutdown sends what
is left.

One setting is Spark's own: `CONVALESCE_SPARK_EVENTS`. By default the
listener forwards what describes a run, which is the application, the jobs
and the SQL executions starting and ending. Task and stage events describe
the inside of a job, one per task, so they are sent only when asked for:
`all` for everything Spark offers, or a comma-separated list of event class
simple names to add to the default (`QueryStartedEvent`, `CreateTableEvent`).
The only events `all` leaves out are the deprecated `*Blacklisted` twins of
the `*Excluded` ones; `emit-spark/src/test/resources/spark-events.yml` lists
them, and a test fails when a new Spark adds an event in neither place. A
failed task or stage is always sent, because it carries the reason the job
died, and an event Spark cannot render still arrives, carrying its type and
why it could not be rendered.

## What it sends

Whatever Spark's own `JsonProtocol` produced for the event, wrapped in the same envelope the
Python client uses, with the application's id stamped on it. Nothing else here reads or writes a
field of an event: Spark names the application on the start event and in a job's properties and
nowhere else, so without it a job end or an application end says nothing about which driver it
came from.

## Exact lineage, through OpenLineage

Spark's own events never carry the logical plan, so they cannot say exactly which tables and
columns a job read and wrote. OpenLineage-Spark walks that plan inside the driver. Add it, and
point its transport at this jar:

```sh
spark-submit \
  --packages io.convalesce:convalesce-emit-spark:0.1.4,io.openlineage:openlineage-spark_2.12:1.53.0 \
  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener,io.convalesce.emit.spark.ConvalesceSparkListener \
  --conf spark.openlineage.transport.type=convalesce \
  your_job.py
```

Use `openlineage-spark_2.13` on a Scala 2.13 Spark. Each OpenLineage `RunEvent` arrives as the
observation `openlineage`, payload `{"run_event": <the RunEvent>}`, rendered by OpenLineage's own
serialiser. It goes through the same emitter as the listener's events, so the same
`CONVALESCE_*` variables configure it; the transport takes no settings of its own. Datasets,
schema, column lineage, output statistics, parent runs and Delta or Iceberg versions all come
from OpenLineage-Spark itself. An event over `CONVALESCE_MAX_BODY_BYTES` is sent without its
`spark.logicalPlan` facet, the one part a receiver can do without.

The listener does not register OpenLineage for you: its listener reads its own Spark settings at
construction, and starting it from inside another listener would skip them. Without
openlineage-spark on the classpath nothing here changes; the transport is only loaded when
OpenLineage asks for it.

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
./gradlew build            # compiles, formats, tests
./gradlew legacyTest       # the Spark 3.3 path, against real 3.3 jars
./gradlew openLineageTest  # the OpenLineage transport, run by `test` too
```

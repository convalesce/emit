# convalesce-emit (Java)

Forwards Spark's own listener events to Convalesce, unchanged, and with them the exact tables and
columns each job read and wrote.

## Install

Two lines of Spark config:

```
spark.jars.packages   io.convalesce:convalesce-emit-spark_2.12:0.2.1
spark.extraListeners  io.convalesce.emit.spark.ConvalesceSparkListener
```

Or on the command line:

```sh
spark-submit \
  --packages io.convalesce:convalesce-emit-spark_2.12:0.2.1 \
  --conf spark.extraListeners=io.convalesce.emit.spark.ConvalesceSparkListener \
  your_job.py
```

The suffix is your Spark's Scala build: `_2.12` for Spark 3.3, 3.4 and 3.5 as they are usually
distributed, `_2.13` for Spark 4.0 and for a Spark 3 built with Scala 2.13. `spark-submit --version`
prints it.

Configure it with the same environment variables the Python client uses:
`CONVALESCE_INGEST_KEY`, `CONVALESCE_ENDPOINT`,
`CONVALESCE_DRY_RUN`, `CONVALESCE_ENABLED`, `CONVALESCE_BATCH_SIZE`,
`CONVALESCE_MAX_RETRIES`, `CONVALESCE_TIMEOUT`,
`CONVALESCE_MAX_BODY_BYTES`, `CONVALESCE_SPOOL_DIR`,
`CONVALESCE_SPOOL_MAX_BYTES`. `CONVALESCE_FLUSH_INTERVAL` (seconds, default
`5`) sends a part batch in the background, and the JVM's shutdown sends what
is left. With `CONVALESCE_DRY_RUN=true` each observation is logged as one
`convalesce dry-run: {...}` line; what is flushed as the JVM shuts down,
such as the application's end for a driver that failed with its session
still open, is written straight to stderr, in the same form.

AWS Glue hands a job only the variables whose names start with `CUSTOMER_`,
so every setting here is also read with that in front. Give them in the job
parameter `--customer-driver-env-vars`, separated by commas:
`CUSTOMER_CONVALESCE_INGEST_KEY=...,CUSTOMER_CONVALESCE_ENDPOINT=...`. A
setting given under both names is read from its own.

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

## Tables and columns

Spark's own events never carry the logical plan, so they cannot say exactly which tables and
columns a job read and wrote. [OpenLineage-Spark](https://openlineage.io/docs/integrations/spark/)
walks that plan inside the driver. The `_2.12` and `_2.13` packages name it as a dependency, so
`--packages` brings it with them, and the listener starts it: there is no second package to add,
no second listener to name and no OpenLineage setting to write.

Each OpenLineage `RunEvent` arrives as the observation `openlineage`, payload
`{"run_event": <the RunEvent>}`, rendered by OpenLineage's own serialiser. It goes through the same
emitter as the listener's events, so the same `CONVALESCE_*` variables configure it. Datasets,
schema, column lineage, output statistics, parent runs and Delta or Iceberg versions all come from
OpenLineage-Spark itself. An event over `CONVALESCE_MAX_BODY_BYTES` is sent without its
`spark.logicalPlan` facet, the one part a receiver can do without.

What the listener sets, on the driver's own configuration and only where the job has not:

| Setting | Value | Why |
| --- | --- | --- |
| `spark.openlineage.transport.type` | `convalesce` | sends each event through this jar |
| `spark.openlineage.columnLineage.datasetLineageEnabled` | `true` | the columns a join, filter or grouping read are reported once for the table |
| `spark.openlineage.capturedProperties` | `spark.master,spark.app.name,spark.glue.JOB_NAME,spark.glue.JOB_RUN_ID` | on AWS Glue, names the job and the run whatever the script calls its application |
| `spark.openlineage.facets.custom_environment_variables` | `[AWS_DEFAULT_REGION;GLUE_VERSION;GLUE_COMMAND_CRITERIA;GLUE_PYTHON_VERSION;]` | on AWS Glue only: the region and the Glue version the run used |

Every other `spark.openlineage.*` setting is yours and is kept: `namespace`, `appName`, the parent
job settings an orchestrator adds.

The driver's log says in one line when a job will send no table or column lineage and why: a
warning when OpenLineage-Spark is not on the classpath, a note when the job runs its own.

Column lineage follows the plan. A column computed by a Python UDF is traced to the columns passed
to the UDF. Code that leaves the plan, an RDD `map` or a `collect()` whose rows are written back,
is reported with the tables it read and wrote.

### A job that already runs OpenLineage

It is left exactly as it is. The listener starts nothing and sets nothing when the job names
`io.openlineage.spark.agent.OpenLineageSparkListener` in `spark.extraListeners`, sets any
`spark.openlineage.transport.*`, `spark.openlineage.url`, `spark.openlineage.host` or
`spark.openlineage.disabled`, sets `OPENLINEAGE_URL`, `OPENLINEAGE_CONFIG`,
`OPENLINEAGE_DISABLED` or an `OPENLINEAGE__TRANSPORT*` variable, or has an `openlineage.yml` in
the working directory or `~/.openlineage`.

To send that job's events here as well as to your own backend, add this transport to it:
`spark.openlineage.transport.type=convalesce`, or one entry of a `composite` transport.

### Switching it off

`CONVALESCE_OPENLINEAGE=false` on the driver: the listener forwards Spark's own events and starts
nothing else.

### Adding jars by hand

`--jars`, a cluster library folder and a job's own fat jar resolve no dependencies. Add three
jars: `convalesce-emit-spark`, `convalesce-emit-core` and `openlineage-spark_2.12` (or `_2.13`),
version `1.53.0`, the one the packages above name. The listener starts OpenLineage the same way
once it is on the classpath. `io.convalesce:convalesce-emit-spark`, with no suffix, is the same
jar with only `convalesce-emit-core` behind it.

### Credentials

A job start carries the job's whole Spark configuration, and OpenLineage copies the settings named
in `spark.openlineage.capturedProperties`. Both are redacted before anything leaves the driver, by
Spark's own rule: a value is replaced with `*********(redacted)` when its key or the value itself
matches `spark.redaction.regex`.

Convalesce's own key is held to more than that rule:

- A setting whose name holds `ingest_key`, `ingest.key` or `ingestkey`, in any case, is redacted
  whatever `spark.redaction.regex` is set to. That covers
  `spark.yarn.appMasterEnv.CONVALESCE_INGEST_KEY` and `spark.executorEnv.CONVALESCE_INGEST_KEY`.
- A value that lists `NAME=value` entries has the value of each entry with such a name, or a name
  the rule matches, replaced with `***` and the rest kept. On AWS Glue
  `spark.glue.customer-driver-env-vars` is sent as
  `CUSTOMER_CONVALESCE_ENDPOINT=...,CUSTOMER_CONVALESCE_INGEST_KEY=***`. The observation's
  `excluded` names the setting, as
  `{"path": "Properties.spark.glue.customer-driver-env-vars", "reason": "ingest key masked"}`.
- The key's exact value is masked (`***`) anywhere else in an observation, and that observation's
  `excluded` says `{"path": "$", "reason": "ingest key masked"}`. The key travels in the
  `Authorization` header only. A key under 8 characters is too short to look for.

## Settings

Off by default. With `CONVALESCE_SEND_SETTINGS=true` on the driver, the application's end also
carries the driver's environment variables, so a receiver holding them for the last good run and
for this one can say which changed.

An ordinary variable is sent as its value. One whose name or value reads as a credential, or whose
value is over 300 characters, is sent only as a keyed hash made in the driver: an HMAC-SHA-256 of
its kind, name and value, cut to 16 hex characters. The hash says that a secret changed and nothing of
what it is.

```json
"settings": {
  "items": [
    {"kind": "environment", "name": "DB_PASSWORD", "fingerprint": "7ee700b0447bb731"},
    {"kind": "environment", "name": "TZ", "value": "Europe/London"}
  ],
  "keyed_by": "2b4957b70d863694"
}
```

- The hash is keyed with `CONVALESCE_FINGERPRINT_KEY` where you set one, and with a key derived
  from the ingest key where you do not. `CONVALESCE_FINGERPRINT_KEY` is never sent. `keyed_by` is
  a hash of the key itself: two runs' hashes compare only where it is the same.
- With neither key, each secret is left out and the observation's `excluded` says
  `{"path": "settings", "reason": "no key to fingerprint 3 settings with"}`.
- `CONVALESCE_SETTINGS_SKIP` lists names that are sent in neither form, separated by commas:
  `CONVALESCE_SETTINGS_SKIP=INTERNAL_HOST,BUILD_USER`.
- `CONVALESCE_INGEST_KEY`, `CONVALESCE_API_KEY` and `CONVALESCE_FINGERPRINT_KEY` are sent in
  neither form, under their own names or with `CUSTOMER_` in front.
- At most 500 names are sent, in name order, and `excluded` says
  `{"path": "settings.environment", "reason": "limited to 500 names"}` when there were more.

The field is added after Spark's redaction has run on the event, and its hashes are the same ones
the Python client makes for the same key, name and value.

## Why it is small

Spark already knows how to render its own events -- `JsonProtocol` is what writes the event log --
so this forwards a string Spark produced rather than walking an object graph. A listener that
mapped events into some other shape would have to be upgraded in your cluster every time that
shape changed. This one does not.

## Constraints, and why

- **No dependencies of its own.** This jar loads into someone else's Spark driver; anything it
  brought could collide with what that cluster already runs. That is why the JSON is written by
  hand and the transport is `HttpURLConnection`. OpenLineage-Spark is one self-contained jar that
  relocates what it carries.
- **Java 8 bytecode.** Every Spark 3.x cluster can load it, including those still on Java 8 or 11:
  EMR 6.x, Databricks 15.4 LTS and below, and older Glue.
- **One jar for both Scala builds.** The only Spark API it touches takes and returns plain Java
  types, so `convalesce-emit-spark`, `_2.12` and `_2.13` are the same jar. The suffix only chooses
  which OpenLineage-Spark comes with it.
- **Nothing escapes into the job.** Every callback wraps its body; a failure to emit is logged and
  dropped. Verified: with the endpoint unreachable, `spark-submit` still exits 0.

## Build

```sh
./gradlew build            # compiles, formats, tests
./gradlew legacyTest       # the Spark 3.3 path, against real 3.3 jars
./gradlew openLineageTest  # the OpenLineage transport, run by `test` too
```

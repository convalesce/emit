# Version support

Every version below was run for real: the tool installed, a job or flow
executed, and the observation received over HTTP by a stand-in endpoint.

## What is supported

| Tool | Versions verified |
| --- | --- |
| Airflow | 2.5.3, 2.6.3, 2.7.3, 2.8.4, 2.9.3, 2.10.5, 2.11.0, 3.0.3 |
| Dagster | 1.7.16, 1.9.13, 1.10.0, 1.13.21 |
| Prefect | 2.20.26, 3.1.15, 3.8.5 |
| Great Expectations | 0.17.23, 0.18.22, 1.22.0 |
| Spark | 3.3.4, 3.5.3, 4.0.3 |
| Python | 3.9 (floor) through 3.12 |

One release per tool covers every version in its row:

- **Airflow 2.5 through 3.0 in one artifact**, because the plugin generates its hooks from
  whatever hookspecs the running Airflow declares rather than hard-coding them.
- **Spark on Java 8 and 11 clusters**, which is what a Java 8 target buys: EMR 6.x, Databricks
  15.4 LTS and below, and older Glue all load it.
- **Great Expectations 0.x and 1.x behind one import.** `action.py` reads the installed version
  and re-exports the class that major needs, so a checkpoint lists the same path either way.

Two versions could not be exercised, both for reasons inside the tool:

- **Prefect 2.20.26** was verified natively rather than in Docker: `import prefect` alone exits
  with SIGILL in that release's arm64 wheels, before any of our code runs.
- **Prefect 3.0.x** cannot be imported at all against current pydantic --
  `from prefect import flow` raises `PydanticUndefinedAnnotation` in Prefect's own `main.py`.
  Anyone on 3.0.x has to pin pydantic regardless of what they emit with. Verified 3.1.15 and
  3.8.5 instead.

Airflow 2.5.3 fires and registers correctly, checked by dispatching through its real listener
manager; its `airflow dags test` command does not route through listeners, which is why the
end-to-end DAG run shows nothing on that version alone.

## How one package covers every version

**Airflow.** The plugin reads the hookspecs of the Airflow it is running in
(`airflow.listeners.spec.*` on 2.x, `airflow._shared.listeners.spec.*` on 3.x)
and generates hook implementations whose parameters match exactly. Three
things made that necessary, and all three were found by registering with a
real Airflow rather than by reading:

- pluggy only sees methods marked with its `hookimpl`; without the marker the
  plugin loads, reports nothing, and never fires.
- pluggy reads a hook's parameters from its code object, so `**kwargs`
  receives nothing, and a parameter with a default is treated as optional and
  not passed at all. A fully defaulted signature fired on every task and
  delivered `None` for everything.
- Declaring a hook this Airflow does not specify makes pluggy reject the
  plugin, taking the scheduler down at startup.

Generating from the spec also absorbs real differences: `on_task_instance_failed`
gained an `error` argument in Airflow 2.10, and Airflow 3 dropped `session`
and added `on_task_instance_skipped`. On 2.7 to 2.9 no listener receives
`error`, ours included, because Airflow does not pass it.

**Great Expectations** is the one tool that needs two classes. In 0.x an
action is a plain object constructed with `(data_context, name)` that
dispatches to `_run`; in 1.x it is a pydantic model discriminated on `type`
whose `run` takes `(checkpoint_result, action_context)`. Nothing satisfies
both, so `action.py` picks `action_v0` or `action_v1` from the installed
version. A checkpoint lists the same path either way.

**Dagster and Prefect** export plain functions rather than decorated sensors
or hooks, because their decorator signatures have moved between versions and
this package deliberately does not depend on either. Prefect 2 calls state
hooks by keyword and Prefect 3 positionally; passing the function directly
lets each use its own convention.

**Spark** needs no version branch in the listener at all, because it never reads an event: it
hands each one to Spark's own `JsonProtocol` and forwards the string. The one thing that did move
is which method to call.

| Spark | Method | Returns |
| --- | --- | --- |
| 3.4 - 4.x | `sparkEventToJsonString(event)` | `String` |
| 3.0 - 3.3 | `sparkEventToJson(event)` | json4s `JValue` |

Resolved once by reflection, which keeps json4s off the compile classpath and lets one
dependency-free jar cover Spark 3.0 through 4.x.

That legacy path shipped broken in its first draft. The json4s idiom is `compact(render(v))`, but
`render` takes an implicit `Formats` that reflection sees as a real second parameter, so every call
threw and the failure was swallowed -- the plugin ran, reported nothing, and looked fine.
`compact` turns out to accept a `JValue` directly, so `render` is not needed. `./gradlew
legacyTest` now runs that path against real Spark 3.3.4 jars in its own source set, because a
hand-rolled fake would have proven nothing.

The same jar also loads on Java 21 (Spark 4.0.3), which is what Java 8 bytecode buys.

## What ran for real

`e2e/` starts each tool in Docker at the versions in `.github/workflows/e2e.yml`,
with the plugin installed the way a customer installs it, runs an example
workflow with one step that fails, and reads what reached a stand-in endpoint
over HTTP with the key. The first run found two things the in-process probes
had not:

- **The Airflow listener never sent anything at the default batch size.** Task
  hooks fire in a process Airflow forks per task and exits without telling
  the listener; with a batch of fifty, every task ended with its events still
  queued. The listener now flushes after each event.
- **The Dagster sensor forwarded the context wrapper**, whose state is private
  and whose run and event are properties, so it arrived as a bare object
  repr: no job name, no run id, no failure. The sensor now forwards
  `dagster_run`, `dagster_event` and `sensor_name`. The probe had passed a
  `DagsterRun` by hand, which is why it never saw this.

Also confirmed there: Airflow 3.0 fires the task hooks from its task SDK
subprocess with the plugin discovered through the entry point; the failure
message reaches the wire from Airflow 2.10 and not before; a Spark driver
flushes what it batched before exiting.

## Why the payload is not read

None of these plugins read a field off a tool object, and that is what keeps
one release working across so many versions. A plugin that reaches for
`task_instance.execution_date` breaks when Airflow renames it. Forwarding the
object whole means a rename changes what arrives, not whether anything
arrives, so a customer never has to upgrade a package inside their pipeline to
keep up with their own tooling.

# End-to-end stacks

Each directory under `stacks/` is one tool running for real in Docker, with the plugin
installed the way a customer installs it, an example workflow that has one
step that works and one that fails, and a stand-in ingest endpoint that
records what arrives.

The unit tests cover our own logic. The `tools` job in `ci.yml` proves each
tool's callback API still matches what a plugin declares, by calling the
callback by hand in one process. These prove the rest: that the tool
discovers the plugin, fires it from its own scheduler, daemon or task
process, and that what it sends reaches the endpoint with the key.

## Run one locally

Docker must be running. Pick a tool and a version from the matrix in
`.github/workflows/e2e.yml`:

```sh
make e2e TOOL=airflow VERSION=2.10.5
make e2e TOOL=dagster VERSION=1.13.21
make e2e TOOL=prefect VERSION=3.8.5
make e2e TOOL=gx VERSION=1.22.0
make e2e TOOL=spark VERSION=3.5.3    # needs `cd java && ./gradlew build -x test` first
```

Or directly:

```sh
cd e2e && EMIT_E2E_AIRFLOW=2.10.5 python -m pytest test_airflow.py
```

Stacks bind the receiver to `EMIT_E2E_PORT` (default 18080), so two can run
at once on different ports. `EMIT_E2E_KEEP=1` leaves a stack up after the
test for inspection.

## What each stack does

| Tool | Runs | Example workflow | The plugin fires from |
| --- | --- | --- | --- |
| Airflow | `airflow standalone` on the official image | a DAG whose second task raises | the task process, via the `airflow.plugins` entry point, and the scheduler for dag-run events |
| Dagster | `dagster dev` (web server, daemon, code location) | two jobs launched over GraphQL, one failing | the daemon's run-status sensors, in the code-server process |
| Prefect | `prefect server` plus a runner on the official image | two flows with flow and task hooks, one failing | Prefect calling the hooks, by keyword on 2.x and positionally on 3.x |
| Great Expectations | a checkpoint in the tool's own runtime | a suite over a small frame, listing the action by class path | GX constructing the action from the checkpoint |
| Spark | `spark-submit` on the official image | the same job the dry-run CI uses | the listener, batching and flushing before the driver exits |

## Traps

- **Let the scheduler parse the DAG before the CLI touches it.** On Airflow
  2.9 and later `dags unpause` parses the file and writes the `dag` row
  itself, and a row the CLI wrote before the scheduler's first parse left
  every run unscheduled on a slow runner. The test waits for a scheduler
  heartbeat (`airflow jobs check`) and for the DAG in `serialized_dag`,
  which only the scheduler writes. `standalone`'s "Airflow is ready" line
  is not a usable gate: it did not appear within five minutes here.
- **The stack directories live under `stacks/`, not beside the tests.** A
  directory named `airflow` next to `harness.py` becomes a namespace
  package that shadows the real one under mypy.
- **Prefect's arm64 image dies with an illegal instruction** on Apple
  silicon. The stack pins `linux/amd64`, which is native on CI and emulated
  locally.
- **`apache/airflow:2.5.3` without a Python suffix is Python 3.7**, below
  the floor the packages declare. The test names the Python the CI matrix
  uses.
- **A `$var` in a compose command is a compose variable.** Shell variables
  in `stacks/spark/compose.yml` are written `$$var`.
- **`compose run` does not rebuild.** The harness passes `--build`, or a
  version change silently reuses the previous version's image.

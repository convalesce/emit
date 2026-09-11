# Contributing

Thank you for helping. This page is everything a change needs to land.

## The one rule

Each plugin connects and sends. It does not parse, map or resolve anything:
whatever the tool handed the callback goes across as it is. A plugin that
reads a field off a tool object breaks when the tool renames it, and every
customer then has to upgrade a package inside their pipeline. Interpretation
happens after the payload arrives, never before. A change that reads the
payload will be asked to stop.

The second rule follows from the first: the Python client has **no
dependencies**, and a plugin depends on nothing but the client. It installs
into someone else's Airflow or Dagster and must not disturb a resolution
that already works. The Java client is the same: Java 8 bytecode and no
dependencies, because it loads into someone else's Spark driver.

## Set up

Python 3.9 or later for the packages, 3.12 for the tooling. Java 17 for the
Gradle build. Docker for the end-to-end stacks.

```sh
cd python
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e . -e plugins/airflow -e plugins/dagster -e plugins/prefect -e plugins/gx
pip install black isort flake8 mypy pylint pytest pytest-timeout build twine
```

The lint configuration is not committed. Copy the four files from the
`Write lint configuration` step in `.github/workflows/ci.yml` into
`python/`; they are the house standard and `make lint` reads them.

## The gates

Every pull request must pass all of these, and CI runs them:

| Gate | Command | Where |
| --- | --- | --- |
| Lint | `make lint` | `python/`, covers `src`, `plugins` and `../e2e` |
| Unit tests | `make test` | `python/` |
| Java | `./gradlew build` under JDK 17 | `java/` |
| Tools | `.github/scripts/probe_<tool>.py` on real installs | `ci.yml` |
| Stacks | `make e2e TOOL=<tool> VERSION=<version>` | repo root, needs Docker |

`make lint` is five tools in order: isort, black, flake8, mypy, pylint.
Running only the last three misses what CI checks. The Java gate must run
under JDK 17, the version CI uses; a newer JDK fails on the Java 8 target
before it compiles anything.

## Code style

Read one existing module before writing a new one; `python/src/convalesce_emit/client.py`
is a good model. In short:

- A module docstring saying what the module is for, ending with `Import as:`
  and the alias other modules use.
- Type hints on every signature. Docstrings with `:param` and `:return`.
- Section banners (`# ####...`) above each class.
- Comments say why, never what. A comment that restates the code is removed.
- Tests are `unittest` classes named `Test_<subject>1` with methods `test1`,
  `test2`, each with a docstring beginning `Test that`.
- 81 columns. Black and isort enforce the rest.

## Branches

`main` takes pull requests only; nothing is pushed to it directly. Releases
are cut from `release`, which only ever receives merges from `main`, and
every published tag points at a commit on it. See `docs/releasing.md`.

## Commits and pull requests

- One line per commit message: a subject, no body, no trailers. Write it as
  what the change does, in plain words, e.g.
  `airflow: send each event as it happens, or the task exits with it queued`.
- One concern per commit. A CI fix and a packaging change are two commits.
- A pull request says what changed and what was verified, and links the
  issue it closes. Bug fixes come with the test that would have caught them.

## Adding a plugin for a new tool

This is the most useful contribution. Open an issue first, or take one of
the open `tool-plugin` issues, then:

1. **Find the tool's push mechanism**: a listener, hook, callback or event
   API the tool calls when something happens. If the tool has none, it
   needs a different design and the issue should say so.
2. **Create `python/plugins/<tool>/`** from an existing plugin. The
   `pyproject.toml` depends only on `convalesce-emit`, never on the tool.
   Where the tool discovers plugins through an entry point, declare it;
   where it does not, export plain functions the customer wires in.
3. **Forward the tool's objects whole** through `cemit.dump`, with the tool
   name and version. Flush after each event if the tool may exit the
   process right after the callback. Never raise into the tool: a
   customer's run must not fail because we could not report on it.
4. **Unit tests** beside the code, under `src/<package>/test/`, with a
   recorder standing in for the emitter.
5. **A probe** in `.github/scripts/probe_<tool>.py` that registers with a
   real install of the tool and fires a real callback, and a row per
   version boundary in the `tools` matrix in `ci.yml`.
6. **A stack** under `e2e/stacks/<tool>/` that runs the tool in Docker with
   the plugin installed the way a customer installs it, plus a test in
   `e2e/test_<tool>.py`, and a row in the `e2e.yml` matrix.
7. **Docs**: a README in the plugin that shows the wiring, a row in
   `docs/version-support.md` with every version verified, and the tool in
   the table at the top of the repository README.

The version boundaries matter more than the count: list the versions where
the tool's callback API actually changed, each found by running it.

## Reporting a bug

Use the bug template. The most useful report names the tool and its exact
version, and includes the envelope that arrived or the log line that says
why nothing did (`CONVALESCE_DRY_RUN=true` prints every envelope).

# convalesce-emit-gx

Forward Great Expectations validation results to Convalesce: every
expectation's outcome, its counts and statistics, and the failing values
Great Expectations sampled. One setting keeps the values where they are.

## Install

```sh
pip install convalesce-emit-gx
```

## Wire it up

One class path serves both majors. `convalesce_emit_gx.action` picks the
implementation for the installed Great Expectations.

On 1.x, list the action on the checkpoint:

```python
from great_expectations import Checkpoint
from convalesce_emit_gx.action import ConvalesceValidationAction

checkpoint = Checkpoint(
    name="orders",
    validation_definitions=[...],
    actions=[ConvalesceValidationAction()],
)
```

On 0.x, list it in the checkpoint's action list:

```python
action_list=[
    {
        "name": "convalesce",
        "action": {
            "module_name": "convalesce_emit_gx.action",
            "class_name": "ConvalesceValidationAction",
        },
    }
]
```

Set `CONVALESCE_INGEST_KEY` where the checkpoint runs.

### Validating outside a checkpoint

Great Expectations runs actions only from a checkpoint.
`ValidationDefinition.run()`, `Batch.validate()` and a 0.x
`Validator.validate()` fire nothing, so send their result yourself:

```python
from convalesce_emit_gx import forward_validation_result

result = validation_definition.run()
forward_validation_result(result)
```

On 0.x, pass the validator too, `forward_validation_result(result,
validator=validator)`, so the platform is named from its engine.

### Naming the table a checkpoint validates

A table or query asset is matched to its table in the catalogue from the
datasource. A dataframe has no table that Great Expectations knows of, so
name it on the action:

```python
ConvalesceValidationAction(
    platform="postgres",
    dataset_name="my_db.my_schema.orders",
)
```

On 0.x the same fields go beside `class_name` in the action's entry:

```python
"action": {
    "module_name": "convalesce_emit_gx.action",
    "class_name": "ConvalesceValidationAction",
    "platform": "postgres",
    "dataset_name": "my_db.my_schema.orders",
},
```

| Field | What it names |
|---|---|
| `platform` | The platform the table lives on, as the catalogue names it: `postgres`, `snowflake`, `bigquery`. |
| `dataset_name` | The table, as the catalogue names it: `my_db.my_schema.orders`. Every result of the checkpoint is filed under it, so set it on a checkpoint that validates one table. |
| `platform_instance` | The instance of the platform, where your catalogue tells several apart. |

Each is optional, and what is set applies to that checkpoint alone.
`forward_validation_result` takes the same three as keyword arguments.

## What crosses

- Every field of the checkpoint result, each validation result and each
  expectation result, under every result format.
- Each datasource's platform, database and default schema. Never its
  connection string; a password inside a batch spec is masked.
- The query a query asset's batch is read with, up to 20,000 characters,
  so the tables and columns behind the batch can be read from it. Set
  `CONVALESCE_SEND_SOURCE=false` to keep it where it is.
- Each result's GX Cloud page, when GX Cloud stored it.
- The `platform`, `dataset_name` and `platform_instance` set on the action.

### Row values

A validation result holds some of the values of the table it checked, and
by default they are sent:

- Failing sample values. For a check that fails row by row, Great
  Expectations keeps a sample of the values that failed, 20 by default, and
  how often each occurred (`partial_unexpected_list`,
  `partial_unexpected_counts`).
- What a fuller result format adds. Under `COMPLETE`, every failing
  value and where its row is (`unexpected_list`, `unexpected_index_list`,
  `unexpected_index_query`). Where the result format asks for the failing
  rows themselves, those rows (`unexpected_rows`).
- Observed column values and their counts. A distinct-values or
  most-common-value expectation reports the values it found in the column
  (`observed_value`), and on 0.x how many rows hold each
  (`details.value_counts`).

They are what tells an investigation which rows broke a check. The rows
that passed, and the frame that was validated, stay where they are.

To send none of them, set this where the checkpoint runs:

```sh
CONVALESCE_GX_SEND_SAMPLES=false
```

Each value or list of values is then replaced by how many there were.
Counts, percentages and numeric statistics (a mean, a maximum, a row count)
are sent either way.

## Supported

Great Expectations 0.17 through 1.x, on Python 3.9 and later. Verified on
real installs: see
[version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

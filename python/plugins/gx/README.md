# convalesce-emit-gx

Forward Great Expectations validation results to Convalesce. Counts and
statistics cross; row values never do.

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

## What crosses

- Every field of the checkpoint result, each validation result and each
  expectation result, under every result format.
- Each datasource's platform, database and default schema. Never its
  connection string; a password inside a batch spec is masked.
- Each result's GX Cloud page, when GX Cloud stored it.
- Not the rows: sampled failing values, and the values a distinct-values or
  most-common-value expectation observed, cross as counts. Set
  `CONVALESCE_GX_SEND_SAMPLES=true` to send them.

## Supported

Great Expectations 0.17 through 1.x, on Python 3.9 and later. Verified on
real installs: see
[version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

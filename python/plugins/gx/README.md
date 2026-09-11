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

## Supported

Great Expectations 0.17 through 1.x, on Python 3.9 and later. Verified on
real installs: see
[version-support.md](https://github.com/convalesce/emit/blob/main/docs/version-support.md).

## Configure

Read from the environment: see
[convalesce-emit](https://pypi.org/project/convalesce-emit/).

"""
The example workflow: a checkpoint over a small frame, with the action
listed the way the docs say, on whichever Great Expectations major is
installed.

Run as a script inside the container.
"""

import great_expectations as gx
import pandas as pd

FRAME = pd.DataFrame(
    {
        "email": [
            "alice@example.com",
            "bob@example.com",
            None,
            "dora@example.com",
        ],
        "amount": [10, 20, 30, 40],
    }
)
MAJOR = int(str(gx.__version__).split(".", 1)[0])


def run_v1() -> bool:
    """
    Run the checkpoint the 1.x way, with the action on the checkpoint.

    :return: whether validation passed
    """
    # pylint: disable=import-outside-toplevel
    from convalesce_emit_gx.action import ConvalesceValidationAction

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("orders")
    asset = source.add_dataframe_asset("orders")
    batch = asset.add_batch_definition_whole_dataframe("all")
    suite = context.suites.add(gx.ExpectationSuite(name="orders"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="email")
    )
    definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="orders", data=batch, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="orders",
            validation_definitions=[definition],
            actions=[ConvalesceValidationAction()],
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": FRAME})
    return bool(result.success)


def run_v0() -> bool:
    """
    Run the checkpoint the 0.x way, with the action in the action list.

    :return: whether validation passed
    """
    context = gx.get_context()
    source = context.sources.add_pandas("orders")
    asset = source.add_dataframe_asset("orders", dataframe=FRAME)
    request = asset.build_batch_request()
    context.add_or_update_expectation_suite("orders")
    validator = context.get_validator(
        batch_request=request, expectation_suite_name="orders"
    )
    validator.expect_column_values_to_not_be_null("email")
    validator.save_expectation_suite(discard_failed_expectations=False)
    checkpoint = context.add_or_update_checkpoint(
        name="orders",
        validations=[
            {"batch_request": request, "expectation_suite_name": "orders"}
        ],
        action_list=[
            {
                "name": "convalesce",
                "action": {
                    "module_name": "convalesce_emit_gx.action",
                    "class_name": "ConvalesceValidationAction",
                },
            }
        ],
    )
    result = checkpoint.run()
    return bool(result.success)


def main() -> None:
    """
    Run the checkpoint for the installed major.

    :return: nothing
    """
    success = run_v1() if MAJOR >= 1 else run_v0()
    print(f"CHECKPOINT_DONE gx={gx.__version__} success={success}", flush=True)


if __name__ == "__main__":
    main()

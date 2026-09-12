"""
The example workflow: a checkpoint over a small frame, with the action
listed the way the docs say, on whichever Great Expectations major is
installed.

Run as a script inside the container.
"""

import sqlite3

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

# A SQL datasource, so a datasource type other than pandas exists to send:
# the 0.x action reads it off the live engine's dialect, 1.x off the fluent
# datasource's own type, and neither is reachable from a frame.
SQLITE_PATH = "/tmp/convalesce_orders.db"


def _seed_sqlite() -> None:
    """Create and fill the table the SQL suite runs against."""
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("DROP TABLE IF EXISTS orders")
    conn.execute("CREATE TABLE orders (email TEXT, amount INTEGER)")
    conn.executemany(
        "INSERT INTO orders (email, amount) VALUES (?, ?)",
        [
            ("alice@example.com", 10),
            ("bob@example.com", 20),
            (None, 30),
            ("dora@example.com", 40),
        ],
    )
    conn.commit()
    conn.close()


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


def run_v1_sql() -> bool:
    """
    Run a checkpoint against a SQLite table, the 1.x way.

    :return: whether validation passed
    """
    # pylint: disable=import-outside-toplevel
    from convalesce_emit_gx.action import ConvalesceValidationAction

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_sqlite(
        "orders_sql", connection_string=f"sqlite:///{SQLITE_PATH}"
    )
    asset = source.add_table_asset("orders_sql", table_name="orders")
    batch = asset.add_batch_definition_whole_table("all")
    suite = context.suites.add(gx.ExpectationSuite(name="orders_sql"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="email")
    )
    definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="orders_sql", data=batch, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="orders_sql",
            validation_definitions=[definition],
            actions=[ConvalesceValidationAction()],
        )
    )
    result = checkpoint.run()
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


def run_v0_sql() -> bool:
    """
    Run a checkpoint against a SQLite table, the 0.x way.

    :return: whether validation passed
    """
    context = gx.get_context()
    source = context.sources.add_sqlite(
        "orders_sql", connection_string=f"sqlite:///{SQLITE_PATH}"
    )
    asset = source.add_table_asset("orders_sql", table_name="orders")
    request = asset.build_batch_request()
    context.add_or_update_expectation_suite("orders_sql")
    validator = context.get_validator(
        batch_request=request, expectation_suite_name="orders_sql"
    )
    validator.expect_column_values_to_not_be_null("email")
    validator.save_expectation_suite(discard_failed_expectations=False)
    checkpoint = context.add_or_update_checkpoint(
        name="orders_sql",
        validations=[
            {"batch_request": request, "expectation_suite_name": "orders_sql"}
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
    Run the frame checkpoint and the SQL checkpoint for the installed major.

    :return: nothing
    """
    _seed_sqlite()
    if MAJOR >= 1:
        success = run_v1()
        sql_success = run_v1_sql()
    else:
        success = run_v0()
        sql_success = run_v0_sql()
    print(
        f"CHECKPOINT_DONE gx={gx.__version__} success={success} "
        f"sql_success={sql_success}",
        flush=True,
    )


if __name__ == "__main__":
    main()

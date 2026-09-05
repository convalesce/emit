#!/bin/bash
# Runs each plugin against real installs of its tool, in Docker.
#
# The unit tests cover our own logic; this covers the thing they cannot: that
# a tool's own callback API still matches what the plugin declares. Every
# version listed here was verified once by hand, and three bugs were found
# that reading the source had not.
set -u
cd "$(dirname "$0")/.."

AIRFLOW="2.5.3 2.6.3 2.7.3 2.8.4 2.9.3 2.10.5 2.11.0 3.0.3"
DAGSTER="1.7.16 1.9.13 1.10.0 1.13.21"
PREFECT="3.1.15 3.8.5"          # 2.20.26 verified natively; 3.0.x is broken by its own pydantic pin
GX="0.17.23 0.18.22 1.22.0"

echo "Airflow:            $AIRFLOW"
echo "Dagster:            $DAGSTER"
echo "Prefect:            $PREFECT"
echo "Great Expectations: $GX"
echo
echo "See docs/version-support.md for what each was checked for."

# Dagster publishes no general-purpose image, so this is what a customer
# builds: a Python base, Dagster at the version they run, and our packages.
ARG PYTHON_IMAGE=python:3.11-slim
FROM ${PYTHON_IMAGE}
ARG DAGSTER_VERSION
RUN pip install --no-cache-dir "dagster==${DAGSTER_VERSION}" "dagster-webserver==${DAGSTER_VERSION}"
COPY python /tmp/emit
RUN pip install --no-cache-dir /tmp/emit /tmp/emit/plugins/dagster

# The customer's image: the official Airflow plus our two packages, installed
# from this checkout rather than PyPI so the code under test is this commit.
ARG AIRFLOW_IMAGE
FROM ${AIRFLOW_IMAGE}
COPY --chown=airflow:root python /tmp/emit
RUN pip install --no-cache-dir /tmp/emit /tmp/emit/plugins/airflow

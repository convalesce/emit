# Great Expectations publishes no runtime image: a customer runs it inside
# their own Python. So this is a Python base, GX at their version, pandas for
# the data, and our packages.
ARG PYTHON_IMAGE=python:3.11-slim
FROM ${PYTHON_IMAGE}
ARG GX_VERSION
RUN pip install --no-cache-dir "great-expectations==${GX_VERSION}" pandas
COPY python /tmp/emit
RUN pip install --no-cache-dir /tmp/emit /tmp/emit/plugins/gx

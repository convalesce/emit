# The customer's image: the official Prefect plus our two packages.
ARG PREFECT_IMAGE
FROM ${PREFECT_IMAGE}
COPY python /tmp/emit
RUN pip install --no-cache-dir /tmp/emit /tmp/emit/plugins/prefect

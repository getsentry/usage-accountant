FROM us-docker.pkg.dev/sentryio/dhi/python:3.13-debian13-dev AS build

COPY py/ /app/py/

# Install `bigquery` extra to enable the `bigquery_fetcher` entrypoint
RUN pip install --no-cache-dir "/app/py[bigquery]"

FROM us-docker.pkg.dev/sentryio/dhi/python:3.13-debian13

COPY --from=build /opt/python/lib/python3.13/site-packages /opt/python/lib/python3.13/site-packages

USER nonroot

ENTRYPOINT ["/opt/python/bin/python3", "-m", "usageaccountant.datadog_fetcher"]

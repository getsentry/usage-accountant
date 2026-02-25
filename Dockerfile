FROM us-docker.pkg.dev/sentryio/dhi/python:3.13-debian13-dev AS build

COPY py/ /app/py/
RUN pip install --no-cache-dir /app/py

FROM us-docker.pkg.dev/sentryio/dhi/python:3.13-debian13

COPY --from=build /opt/python/lib/python3.13/site-packages /opt/python/lib/python3.13/site-packages

USER nonroot

ENTRYPOINT ["/opt/python/bin/python3", "-m", "usageaccountant.datadog_fetcher"]

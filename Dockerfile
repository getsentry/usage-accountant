FROM us-docker.pkg.dev/sentryio/dhi-mirror/python:3.13-debian13-dev AS build

COPY py/ /app/py/

RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir /app/py

FROM us-docker.pkg.dev/sentryio/dhi-mirror/python:3.13-debian13

COPY --from=build /opt/venv /opt/venv

USER nonroot

ENTRYPOINT ["/opt/venv/bin/python3", "-m", "usageaccountant.datadog_fetcher"]

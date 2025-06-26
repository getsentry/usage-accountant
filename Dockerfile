FROM python:3.11-slim as application

RUN groupadd -r usage-accountant --gid 1000 && useradd -r -m -g usage-accountant --uid 1000 usage-accountant

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc

COPY . /app/

WORKDIR /app

RUN pip install -e py

WORKDIR /app/py

USER usage-accountant

ENTRYPOINT ["python", "-m", "usageaccountant.datadog_fetcher"]

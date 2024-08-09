FROM python:3.11-slim as application

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc

COPY . /app/

WORKDIR /app

RUN pip install -e py

WORKDIR /app/py

ENTRYPOINT ["python", "-m", "usageaccountant.datadog_fetcher"]

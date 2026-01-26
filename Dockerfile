# Build stage - compile dependencies and install packages
FROM python:3.13-slim as builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy application code
COPY . /app/

WORKDIR /app

# Install the package and its dependencies
RUN pip install --no-cache-dir -e py

# Runtime stage - distroless base (no Python) with Python 3.13 from builder
FROM gcr.io/distroless/base-debian13:nonroot

# Copy Python 3.13 runtime from builder
COPY --from=builder /usr/local/bin/python3.13 /usr/bin/python3.13
COPY --from=builder /usr/local/lib/python3.13 /usr/lib/python3.13
COPY --from=builder /usr/local/lib/libpython3.13.so.1.0 /usr/lib/aarch64-linux-gnu/libpython3.13.so.1.0

# Copy the application code
COPY --from=builder /app /app

WORKDIR /app/py

# Use nonroot user (uid 65532)
USER nonroot

ENTRYPOINT ["python3.13", "-m", "usageaccountant.datadog_fetcher"]

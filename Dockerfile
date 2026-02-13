FROM python:3.13-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY src/ src/

# Build wheel
RUN pip install --no-cache-dir build && \
    python -m build --wheel


FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/dist/*.whl /tmp/

RUN pip install --no-cache-dir /tmp/*.whl && \
    rm -rf /tmp/*.whl

COPY templates/ /app/templates/

RUN useradd -m -u 1000 verda_cloud_provider
USER verda_cloud_provider

EXPOSE 8086

ENTRYPOINT ["verda-cloud-provider"]
CMD ["--config", "/config/config.yaml", "--port", "8086", "--log-level", "INFO"]

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
    wireguard-tools \
    iproute2 \
    procps \
    iptables \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/dist/*.whl /tmp/

RUN pip install --no-cache-dir /tmp/*.whl && \
    rm -rf /tmp/*.whl

COPY templates/ /app/templates/

COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8086

ENTRYPOINT ["/entrypoint.sh"]

CMD ["--config", "/config/config.yaml", "--port", "8086", "--log-level", "INFO"]

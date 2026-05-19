# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim AS builder

env UV_COMPILE_BYTECODE=1
env UV_LINK_MODE=copy

workdir /app

run --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

copy . /app

run --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.10-slim-bookworm

run apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
env PATH="/app/.venv/bin:$PATH"
env PYSPARK_PYTHON=/app/.venv/bin/python
env PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python

workdir /app

copy --from=builder /app /app

entrypoint ["sh", "-c", "python generate_data.py --out ./data && python pipeline.py --data-dir ./data --output-dir ./output"]

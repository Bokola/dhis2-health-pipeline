# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim AS builder

# configure uv compilation variables
# comments should be lowercase and without dots
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

# bind settings and sync dependencies cleanly
# comments should be lowercase and without dots
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.10-slim-bookworm

# install isolated dependencies safely
# comments should be lowercase and without dots
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# force immediate log flush and expose virtual paths
# comments should be lowercase and without dots
ENV PYTHONUNBUFFERED=1
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/app/.venv/bin:$PATH"
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python

WORKDIR /app

# import clean compiled virtual environments
# comments should be lowercase and without dots
COPY --from=builder /app /app

ENTRYPOINT python generate_data.py --out ./data && python pipeline.py --data-dir ./data --output-dir ./output
FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

FROM python:3.12-slim-trixie AS runtime

RUN groupadd --gid 1000 edgepc \
    && useradd --uid 1000 --gid edgepc --no-create-home \
        --shell /usr/sbin/nologin edgepc \
    && apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && ln -snf /usr/share/zoneinfo/Asia/Krasnoyarsk /etc/localtime \
    && echo Asia/Krasnoyarsk > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    DB__DIR=/app/data \
    LOG__DIR=/app/logs \
    TZ=Asia/Krasnoyarsk

COPY --from=builder --chown=edgepc:edgepc /app/.venv /app/.venv
COPY --chown=edgepc:edgepc main.py ./
COPY --chown=edgepc:edgepc src ./src
COPY --chown=edgepc:edgepc alembic.ini ./
COPY --chown=edgepc:edgepc alembic ./alembic

RUN mkdir -p /app/data /app/logs \
    && chown edgepc:edgepc /app/data /app/logs

USER edgepc

CMD ["python", "main.py"]

# ---------- build ----------
FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc default-libmysqlclient-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

COPY app/ app/

# ---------- runtime ----------
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        default-libmysqlclient-dev curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r scheduler && useradd -r -g scheduler scheduler

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

USER scheduler

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 默认启动 API + 内嵌 Scheduler
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

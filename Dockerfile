# ─────────────────────────────────────────────────────────────
# Stage 1: Builder — install dependencies
# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ curl \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


# ─────────────────────────────────────────────────────────────
# Stage 2: Runtime — lean final image
# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser -s /sbin/nologin appuser

WORKDIR /app

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/app"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Copy application source
COPY *.py ./

# Create data and log directories
RUN mkdir -p data logs \
 && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -sf http://localhost:8000/health | grep -q '"status":"ok"' || exit 1

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port 8000 --workers ${API_WORKERS:-1} --log-level ${LOG_LEVEL:-info}"]

# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc librdkafka-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY ingestion/requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN useradd --create-home --shell /bin/bash vectoryn

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY aiven-ca.pem /usr/local/share/ca-certificates/aiven-ca.crt
RUN update-ca-certificates

USER vectoryn
WORKDIR /home/vectoryn/app

COPY --from=builder --chown=vectoryn:vectoryn /root/.local /home/vectoryn/.local

ENV PATH=/home/vectoryn/.local/bin:$PATH
ENV PYTHONPATH=/home/vectoryn/app

COPY --chown=vectoryn:vectoryn ingestion/ ./ingestion/

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=40s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "ingestion.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

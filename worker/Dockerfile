# ─── Stage 1: Builder ────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    librdkafka-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --user --no-cache-dir \
    torch==2.2.2+cpu \
    --index-url https://download.pytorch.org/whl/cpu

COPY worker/requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

RUN python -c "\
from transformers import AutoTokenizer, AutoModel; \
AutoTokenizer.from_pretrained('sentence-transformers/all-MiniLM-L6-v2'); \
AutoModel.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')"

# ─── Stage 2: Runtime ────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN useradd --create-home --shell /bin/bash vectoryn

USER root
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Aiven uses a private CA — embed it in the system trust store
COPY aiven-ca.pem /usr/local/share/ca-certificates/aiven-ca.crt
RUN update-ca-certificates

USER vectoryn

WORKDIR /home/vectoryn/app

COPY --from=builder --chown=vectoryn:vectoryn /root/.local /home/vectoryn/.local
COPY --from=builder --chown=vectoryn:vectoryn /root/.cache /home/vectoryn/.cache

ENV PATH=/home/vectoryn/.local/bin:$PATH
ENV TRANSFORMERS_CACHE=/home/vectoryn/.cache/huggingface
ENV HF_HOME=/home/vectoryn/.cache/huggingface
ENV OMP_NUM_THREADS=2
ENV TOKENIZERS_PARALLELISM=false
ENV ENABLE_SPARSE=false
ENV PYTHONPATH=/home/vectoryn/app

COPY --chown=vectoryn:vectoryn worker/ ./worker/
COPY --chown=vectoryn:vectoryn ingestion/ ./ingestion/

RUN touch ingestion/__init__.py worker/__init__.py

CMD ["python", "worker/embedder.py"]

# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

ENV HF_HOME=/root/.cache/huggingface

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ librdkafka-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalar PyTorch CPU-only PRIMERO (pesa menos que CUDA y evita que
# sentence-transformers lo jale con CUDA como dependencia transitiva)
RUN pip install --user --no-cache-dir \
    torch==2.2.2+cpu \
    --index-url https://download.pytorch.org/whl/cpu

# Instalar el resto — versiones fijas para evitar sorpresas
COPY worker/requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Descargar modelo en build time → el runtime no necesita internet ni HuggingFace
RUN python -c "\
import os; \
os.environ['HF_HOME'] = '/root/.cache/huggingface'; \
from transformers import AutoTokenizer, AutoModel; \
AutoTokenizer.from_pretrained('sentence-transformers/all-MiniLM-L6-v2'); \
AutoModel.from_pretrained('sentence-transformers/all-MiniLM-L6-v2'); \
print('Model cached OK')"

RUN du -sh /root/.cache/huggingface && echo "Cache OK"

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

COPY --from=builder --chown=vectoryn:vectoryn /root/.local  /home/vectoryn/.local
COPY --from=builder --chown=vectoryn:vectoryn /root/.cache  /home/vectoryn/.cache

ENV PATH=/home/vectoryn/.local/bin:$PATH
ENV HF_HOME=/home/vectoryn/.cache/huggingface
ENV TRANSFORMERS_CACHE=/home/vectoryn/.cache/huggingface
ENV OMP_NUM_THREADS=1
ENV TOKENIZERS_PARALLELISM=false
ENV PYTHONPATH=/home/vectoryn/app

# Defaults ultra-conservadores para Render free (512MB)
ENV ENABLE_SPARSE=false
ENV ENABLE_PROMETHEUS=false
ENV WORKER_BATCH_SIZE=1
ENV WORKER_MAX_DOC_CHARS=50000

COPY --chown=vectoryn:vectoryn worker/    ./worker/
COPY --chown=vectoryn:vectoryn ingestion/ ./ingestion/

RUN touch ingestion/__init__.py worker/__init__.py

CMD ["python", "worker/embedder.py"]

# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# FIX: Establecer HF_HOME ANTES de descargar el modelo
# Evita la migración de caché "deprecated TRANSFORMERS_CACHE" al arrancar
ENV HF_HOME=/root/.cache/huggingface
ENV TRANSFORMERS_CACHE=/root/.cache/huggingface

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ librdkafka-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# PyTorch CPU-only — ahorra ~2.5GB vs CUDA build
RUN pip install --user --no-cache-dir \
    torch==2.2.2+cpu \
    --index-url https://download.pytorch.org/whl/cpu

COPY worker/requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# FIX: Descargar modelo UNA SOLA VEZ en formato nuevo (HF_HOME ya seteado).
# v3.x descargaba con AutoTokenizer/AutoModel → formato viejo → migración al arrancar.
# v4.0: usa transformers directamente con HF_HOME configurado → formato correcto.
RUN python -c "\
import os; \
os.environ['HF_HOME'] = '/root/.cache/huggingface'; \
from transformers import AutoTokenizer, AutoModel; \
AutoTokenizer.from_pretrained('sentence-transformers/all-MiniLM-L6-v2'); \
AutoModel.from_pretrained('sentence-transformers/all-MiniLM-L6-v2'); \
print('Model cached successfully')"

# Verificar tamaño de caché
RUN du -sh /root/.cache/huggingface && echo "Cache size OK"

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

# FIX: Solo HF_HOME — TRANSFORMERS_CACHE genera el warning de deprecación
#      y provoca la migración que duplica archivos en disco
ENV PATH=/home/vectoryn/.local/bin:$PATH
ENV HF_HOME=/home/vectoryn/.cache/huggingface
ENV TRANSFORMERS_CACHE=/home/vectoryn/.cache/huggingface
ENV OMP_NUM_THREADS=2
ENV TOKENIZERS_PARALLELISM=false
ENV PYTHONPATH=/home/vectoryn/app

# Defaults memory-safe para Render free tier (512MB)
ENV ENABLE_SPARSE=false
ENV WORKER_BATCH_SIZE=2

COPY --chown=vectoryn:vectoryn worker/    ./worker/
COPY --chown=vectoryn:vectoryn ingestion/ ./ingestion/

RUN touch ingestion/__init__.py worker/__init__.py

CMD ["python", "worker/embedder.py"]

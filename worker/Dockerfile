# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

ENV HF_HOME=/root/.cache/huggingface

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ librdkafka-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── OPTIMIZACIÓN v5: NO instalar PyTorch completo (~2.5GB → 0)
# sentence-transformers con backend ONNX usa onnxruntime (~40MB) en vez de torch (~220MB)
RUN pip install --user --no-cache-dir \
    "sentence-transformers>=2.7.0" \
    "onnxruntime>=1.17.0" \
    "optimum[onnxruntime]>=1.19.0"

COPY worker/requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Descargar y exportar modelo a ONNX UNA SOLA VEZ en build time
# → el contenedor arranca sin necesitar descargar nada
RUN python -c "\
import os; \
os.environ['HF_HOME'] = '/root/.cache/huggingface'; \
from sentence_transformers import SentenceTransformer; \
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2', backend='onnx'); \
model.save('/root/.cache/huggingface/onnx-model'); \
print('ONNX model exported and cached successfully')"

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

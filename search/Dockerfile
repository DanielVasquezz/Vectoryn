# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

ENV HF_HOME=/root/.cache/huggingface
ENV TRANSFORMERS_CACHE=/root/.cache/huggingface

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# PyTorch CPU-only
RUN pip install --user --no-cache-dir \
    torch==2.2.2+cpu \
    --index-url https://download.pytorch.org/whl/cpu

COPY search/requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# FIX: Descargar modelo UNA SOLA VEZ vía SentenceTransformer.
#
# v3.x tenía DOS descargas:
#   1. AutoTokenizer + AutoModel  → caché formato viejo (~90MB)
#   2. SentenceTransformer        → caché formato nuevo (~90MB)
#   TOTAL: ~180MB duplicados + migración al arrancar = más ~90MB extra.
#
# v4.0: Solo SentenceTransformer (que es lo que usa el código en runtime).
#   TOTAL: ~90MB. Ahorra ~90-180MB de imagen y elimina la migración.
RUN python -c "\
import os; \
os.environ['HF_HOME'] = '/root/.cache/huggingface'; \
from sentence_transformers import SentenceTransformer; \
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2'); \
_ = model.encode('warmup'); \
print('Model cached and verified')"

RUN du -sh /root/.cache/huggingface && echo "Cache size OK"

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN useradd --create-home --shell /bin/bash vectoryn
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /home/vectoryn/app

COPY --from=builder --chown=vectoryn:vectoryn /root/.local /home/vectoryn/.local
COPY --from=builder --chown=vectoryn:vectoryn /root/.cache /home/vectoryn/.cache

COPY --chown=vectoryn:vectoryn search/ ./search/

ENV PATH=/home/vectoryn/.local/bin:$PATH
ENV HF_HOME=/home/vectoryn/.cache/huggingface
ENV TRANSFORMERS_CACHE=/home/vectoryn/.cache/huggingface
ENV OMP_NUM_THREADS=2
ENV TOKENIZERS_PARALLELISM=false
ENV PYTHONPATH=/home/vectoryn/app

# Defaults memory-safe para Render free tier (512MB)
ENV ENABLE_SPARSE=false
ENV ENABLE_RERANKER=false

USER vectoryn

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=120s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')"

CMD ["uvicorn", "search.api:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]

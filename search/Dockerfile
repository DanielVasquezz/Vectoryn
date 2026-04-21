# ─── Stage 1: Builder ────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# CPU-only PyTorch (saves ~2.5GB vs CUDA build)
RUN pip install --user --no-cache-dir \
    torch==2.2.2+cpu \
    --index-url https://download.pytorch.org/whl/cpu

COPY search/requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Pre-download dense model only — saves ~90MB on every cold start.
# Cross-Encoder NOT pre-downloaded: it loads only if ENABLE_RERANKER=true,
# which requires >= 1GB RAM (Render Starter plan or higher).
RUN python -c "\
from transformers import AutoTokenizer, AutoModel; \
AutoTokenizer.from_pretrained('sentence-transformers/all-MiniLM-L6-v2'); \
AutoModel.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')"

# Pre-download via sentence-transformers as well (used at runtime)
RUN python -c "\
from sentence_transformers import SentenceTransformer; \
SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"

# ─── Stage 2: Runtime ────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN useradd --create-home --shell /bin/bash vectoryn
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /home/vectoryn/app

COPY --from=builder --chown=vectoryn:vectoryn /root/.local /home/vectoryn/.local
COPY --from=builder --chown=vectoryn:vectoryn /root/.cache /home/vectoryn/.cache

COPY --chown=vectoryn:vectoryn search/ ./search/

ENV PATH=/home/vectoryn/.local/bin:$PATH
ENV TRANSFORMERS_CACHE=/home/vectoryn/.cache/huggingface
ENV HF_HOME=/home/vectoryn/.cache/huggingface
ENV OMP_NUM_THREADS=2
ENV TOKENIZERS_PARALLELISM=false
ENV PYTHONPATH=/home/vectoryn/app

# Memory-safe defaults for Render free tier (512MB)
# Set ENABLE_RERANKER=true only on plans with >= 1GB RAM
ENV ENABLE_SPARSE=false
ENV ENABLE_RERANKER=false

USER vectoryn

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=120s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/health')"

CMD ["uvicorn", "search.api:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]
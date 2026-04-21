# gateway/Dockerfile — Vectoryn API Gateway
# ============================================================
# BUG FIX: CMD had "[IP_ADDRESS]" as a literal string instead of
# "0.0.0.0". This caused the container to fail to bind on startup.
# ============================================================

FROM python:3.11-slim

WORKDIR /app

# FIX: Added httpx for proxying requests, python-dotenv for env loading
RUN pip install --no-cache-dir fastapi uvicorn[standard] pydantic httpx python-dotenv

COPY gateway/ .

# FIX: "[IP_ADDRESS]" → "0.0.0.0" (bind to all interfaces inside container)
# Port 8080 for gateway (avoids conflict with ingestion on 8000)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

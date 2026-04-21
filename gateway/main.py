"""
Vectoryn API Gateway v2.1 — FIXED VERSION
==========================================
Fixes:
1. Prevents sending "id": null to ingestion (cause of intermittent 422 errors)
2. Sanitized payload before sending to ingestion
3. Better HTTP error handling
4. Request validation hardening
"""

import os
import logging
import uuid
from typing import Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "gateway", "message": "%(message)s"}'
)
logger = logging.getLogger("gateway")

API_KEY = os.getenv("VECTORYN_API_KEY", "your_secret_key_here")
INGESTION_URL = os.getenv("INGESTION_SERVICE_URL", "http://ingestion:8000")
SEARCH_URL = os.getenv("SEARCH_SERVICE_URL", "http://search:8001")

app = FastAPI(title="Vectoryn API Gateway", version="2.1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────
# MODELS
# ─────────────────────────────

class IngestRequest(BaseModel):
    id: Optional[str] = Field(default=None)
    content: str


class SearchRequest(BaseModel):
    query: str
    top_k: int = 3
    evaluate: bool = False


# ─────────────────────────────
# AUTH
# ─────────────────────────────

def require_auth(key: Optional[str]):
    if not key or key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")


# ─────────────────────────────
# HEALTH
# ─────────────────────────────

@app.get("/health")
async def health():
    status = {"gateway": "ok", "ingestion": "unknown", "search": "unknown"}

    async with httpx.AsyncClient(timeout=3.0) as client:
        for name, url in [("ingestion", INGESTION_URL), ("search", SEARCH_URL)]:
            try:
                r = await client.get(f"{url}/health")
                status[name] = "ok" if r.status_code == 200 else f"error:{r.status_code}"
            except Exception:
                status[name] = "down"

    return status


# ─────────────────────────────
# INGEST FIXED (CRITICAL)
# ─────────────────────────────

@app.post("/ingest")
async def ingest(req: IngestRequest, x_api_key: Optional[str] = Header(None)):
    require_auth(x_api_key)

    request_id = str(uuid.uuid4())[:8]
    logger.info(f"GATEWAY_INGEST request_id={request_id}")

    # CRITICAL FIX: build a safe payload
    payload = {
        "content": req.content.strip()
    }

    # ONLY include id if it exists (prevents null → intermittent 422)
    if req.id and req.id.strip():
        payload["id"] = req.id.strip()

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            res = await client.post(
                f"{INGESTION_URL}/ingest",
                json=payload,
                headers={
                    "X-API-Key": API_KEY,
                    "X-Request-ID": request_id
                },
            )

            # If ingestion fails, show the actual error
            if res.status_code >= 400:
                raise HTTPException(
                    status_code=res.status_code,
                    detail=res.text
                )

            return res.json()

        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Ingestion service down: {e}")


# ─────────────────────────────
# SEARCH (NO CRITICAL CHANGES)
# ─────────────────────────────

@app.post("/search")
async def search(req: SearchRequest, x_api_key: Optional[str] = Header(None)):
    require_auth(x_api_key)

    request_id = str(uuid.uuid4())[:8]

    async def stream():
        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                async with client.stream(
                    "POST",
                    f"{SEARCH_URL}/search",
                    json=req.dict(),
                    headers={
                        "X-API-Key": API_KEY,
                        "X-Request-ID": request_id
                    },
                ) as res:
                    async for chunk in res.aiter_bytes():
                        yield chunk

            except httpx.RequestError as e:
                yield f"Gateway error: {e}".encode()

    return StreamingResponse(stream(), media_type="text/plain")

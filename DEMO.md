# Vectoryn — Live Demo Guide

> This document shows recruiters and engineers how to run and test Vectoryn locally in under 5 minutes.

---

## Quickest path: just run it

**Requirements:** Docker Desktop + Groq API key (free at console.groq.com)

```bash
git clone https://github.com/DanielVasquezz/vectoryn.git
cd vectoryn
cp .env.example .env
# Add your GROQ_API_KEY to .env
docker compose up -d --build
```

Open **http://localhost:3000** — you're done.

---

## What to try first

### 1. Upload a document

Open the UI → click **Upload** tab → drop any `.txt` or `.md` file.

Or via API:

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key_here" \
  -d '{
    "id": "demo-001",
    "content": "Vectoryn uses a semantic cache to avoid redundant LLM calls. When a user asks a question, the system first checks Redis for a similar query using cosine similarity. Cache hits return in under 10ms instead of the usual 800ms."
  }'
```

**Response (~5ms):**
```json
{
  "status": "accepted",
  "doc_id": "demo-001",
  "message": "Document queued. Searchable in ~5–10 seconds.",
  "anonymized": false
}
```

The API responds immediately. Embedding happens asynchronously.

### 2. Wait ~10 seconds, then search

```bash
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key_here" \
  -d '{"query": "How does the cache reduce LLM costs?", "top_k": 3}'
```

Response streams via SSE — watch tokens appear in real-time.

### 3. Trigger the semantic cache

Ask the same question rephrased:

```bash
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key_here" \
  -d '{"query": "What is the purpose of the Redis cache in this system?", "top_k": 3}'
```

This hits the cache (cosine similarity > 0.97) and returns in **under 10ms** instead of ~800ms.

---

## Observability demo

```bash
docker compose --profile observability up -d --build
```

| Dashboard | URL |
|-----------|-----|
| Grafana (main dashboard) | http://localhost:3001 (admin / vectoryn) |
| Jaeger (distributed traces) | http://localhost:16686 |
| Prometheus (raw metrics) | http://localhost:9090 |

The Grafana dashboard shows live: cache hit rate, ingestion throughput, p50/p95 query latency, LLM token rate, and RAGAS faithfulness scores.

---

## PII Shield demo

The ingestion service automatically masks sensitive data before it reaches Kafka:

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key_here" \
  -d '{
    "id": "pii-test",
    "content": "Contact John at john.doe@example.com or call 555-123-4567. Card: 4111-1111-1111-1111"
  }'
```

**Response:**
```json
{
  "status": "accepted",
  "doc_id": "pii-test",
  "anonymized": true
}
```

The stored content replaces the email, phone, and card with `[EMAIL_REDACTED]`, `[PHONE_REDACTED]`, `[CARD_REDACTED]`.

---

## Health check

```bash
curl http://localhost:8080/health
```

```json
{
  "gateway": "ok",
  "ingestion": "ok",
  "search": "ok"
}
```

A `degraded` status for search (if Qdrant collection hasn't been created yet by the worker) means the worker is still loading ML models. Wait 30–60 seconds and retry.

---

## Run the tests

```bash
# Unit tests — no services needed
pytest tests/unit/ -v

# Full suite — requires docker compose up -d
pytest tests/ -v
```

---

## Teardown

```bash
docker compose down          # Stop services, keep data
docker compose down -v       # Stop and delete all data volumes
```

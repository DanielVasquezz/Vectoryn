<div align="center">

# Vectoryn

**Production RAG engine built with the same architectural patterns used at Netflix, Uber, and Google.**

[![CI/CD](https://github.com/DanielVasquezz/vectoryn/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/DanielVasquezz/vectoryn/actions/workflows/ci-cd.yml)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docker.com)
[![Kafka](https://img.shields.io/badge/Redpanda-Kafka--compatible-231F20?logo=apachekafka&logoColor=white)](https://redpanda.com)
[![Qdrant](https://img.shields.io/badge/Qdrant-Vector_DB-DC244C)](https://qdrant.tech)
[![License: MIT](https://img.shields.io/badge/License-MIT-22c55e)](LICENSE)

[Quick Start](#quick-start) · [Architecture](#architecture) · [API Docs](#api-reference) · [Deploy Free](#deploy-for-free) · [Tech Stack](#tech-stack)

</div>

---

Most RAG systems are demo-grade. They work on a single document, block while embedding, and fall apart the moment two users query simultaneously. **Vectoryn was built to avoid those failure modes from day one.**

The core pipeline is event-driven: documents are acknowledged in **~5ms** via Kafka, chunked and embedded asynchronously in batches by a dedicated worker, then indexed in Qdrant with both dense and sparse vectors. Queries go through a semantic cache first, then hybrid retrieval (RRF fusion), then cross-encoder reranking before hitting the LLM. Faithfulness is checked against retrieved context before the answer is streamed back.

```
Ingest:  POST /ingest → Kafka → Worker (batch embed) → Qdrant
Query:   POST /search → Semantic Cache? → Hybrid Retrieval → Rerank → LLM → SSE Stream
```

---

## What's Actually Different

| | Typical RAG | Vectoryn |
|---|---|---|
| **Ingestion** | API blocks while embedding | Kafka-decoupled — returns in ~5ms, scales independently |
| **Vector search** | Dense only | Hybrid dense + sparse (SPLADE), fused with RRF |
| **Ranking** | Top-K from vector DB | Cross-encoder reranking — query and document processed together |
| **Repeated queries** | LLM call every time | Semantic cache in Redis — cosine similarity threshold, not exact match |
| **Answer quality** | No validation | RAGAS faithfulness check — retries below 0.8 threshold |
| **Privacy** | Raw data indexed as-is | PII shield auto-masks emails, phones, credit cards before Kafka |
| **Observability** | `print()` statements | Prometheus + Grafana + Jaeger — metrics, dashboards, distributed traces |

---

## Performance

Measured on a 6 GB RAM local machine, 200 mixed queries:

| Metric | Value |
|--------|-------|
| p50 latency | 280ms |
| p95 latency | 430ms |
| Semantic cache hit rate | 37% |
| Average retrieval time | 120ms |
| LLM throughput (Groq) | ~1,100 tokens/sec |
| Cost reduction via cache | ~42% |

Cache hits drop latency from ~800ms to **under 10ms**. The cross-encoder adds 80-120ms but meaningfully improves precision on technical documents where dense search alone misses exact terms.

---

## Architecture

```
╔══════════════════════════════════════════════════════════════════╗
║                       INGESTION PIPELINE                         ║
╠══════════════════════════════════════════════════════════════════╣
║  Client → POST /ingest → Gateway (auth + rate limit)             ║
║                              │                                    ║
║                         Ingestion API                             ║
║                              │                                    ║
║                          PII Shield                               ║
║                    (mask emails, phones, cards)                   ║
║                              │                                    ║
║                    Redpanda / Kafka                               ║
║                    (topic: raw-documents)                         ║
║                              │                                    ║
║              ┌─── Embedding Worker ───┐                          ║
║              │  1. Sliding window chunker (500 tok, 50 overlap)   ║
║              │  2. Dense embed  — MiniLM-L6-v2 (384d)            ║
║              │  3. Sparse embed — SPLADE-PP-en-v1                 ║
║              │  4. Batch upsert → Qdrant                         ║
║              └────────────────────────┘                          ║
╠══════════════════════════════════════════════════════════════════╣
║                        SEARCH PIPELINE                            ║
╠══════════════════════════════════════════════════════════════════╣
║  Client → POST /search → Gateway → Search API                    ║
║                                         │                         ║
║                          Semantic Cache (Redis, cos > 0.97)       ║
║                                HIT ──► stream cached answer       ║
║                               MISS                                ║
║                          Query Expansion (Llama → 3 variants)     ║
║                          Hybrid Retrieval (Dense + Sparse + RRF)  ║
║                          Cross-Encoder Rerank (ms-marco → Top 3)  ║
║                          LLM Generation (Groq / Llama-3.1-8b)    ║
║                          RAGAS Faithfulness Check (≥ 0.8)         ║
║                          SSE Stream → Client                      ║
╚══════════════════════════════════════════════════════════════════╝
```

**Why Redpanda over Kafka?** Eliminates the JVM. Identical wire protocol — swap to managed Kafka (MSK, Confluent) with zero code changes.

**Why hybrid search?** Dense embeddings capture semantic meaning but miss exact keywords — product IDs, error codes, proper nouns. SPLADE adds keyword recall. RRF merges both ranking lists without tuning weights.

**Why a semantic cache instead of exact-match?** Users rephrase the same question constantly. "How do I reset my password?" and "What's the process for password recovery?" should hit the same cache entry.

---

## Resilience

Designed to **degrade gracefully**, not fail loudly:

- **Kafka down** → exponential backoff with jitter on the producer side
- **Worker crash** → consumer group rebalances; manual offset commit means no message is lost or double-processed  
- **Redis down** → cache passes through silently; every query still gets answered
- **LLM timeout** → retry with reduced context window before returning error
- **Qdrant collection missing** → `/ready` returns `degraded` instead of 500; search returns a user-facing message

---

## Quick Start

**Requirements:** Docker Desktop 24+ · 6 GB RAM · [Groq API key](https://console.groq.com) (free)

```bash
git clone https://github.com/DanielVasquezz/vectoryn.git
cd vectoryn
cp .env.example .env
```

Add your key to `.env`:

```env
GROQ_API_KEY=gsk_your_key_here
```

```bash
docker compose up -d --build
```

First launch downloads ~2.5 GB of images and ML models — takes 10-20 minutes. Subsequent starts take ~30 seconds from cache.

Wait for the worker to finish loading models:

```bash
docker compose logs -f worker
# Ready when you see: "Worker Online — BatchSize=5 Timeout=500ms"
```

Open the UI at **http://localhost:3000**

Verify everything is healthy:

```bash
curl http://localhost:8080/health
# {"gateway":"ok","ingestion":"ok","search":"ok"}
```

---

## API Reference

All requests require the `X-API-Key` header (set in `.env`, default: `your_secret_key_here`).

### `POST /ingest`

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key_here" \
  -d '{"id": "doc-001", "content": "Your document text here."}'
```

**Response:**
```json
{
  "status": "accepted",
  "doc_id": "doc-001",
  "message": "Document queued. Searchable in ~5-10 seconds.",
  "anonymized": false
}
```

The response is immediate (~5ms). Embedding happens asynchronously.

### `POST /search`

```bash
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key_here" \
  -d '{"query": "What is the main topic?", "top_k": 3}'
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | required | Natural language question (max 2000 chars) |
| `top_k` | integer | 3 | Documents to retrieve (1-20) |
| `evaluate` | boolean | false | Run RAGAS evaluation in background |

Response is SSE — tokens stream as they're generated.

### `GET /health`

```bash
curl http://localhost:8080/health
# {"gateway":"ok","ingestion":"ok","search":"ok"}
```

---

## Observability

```bash
docker compose --profile observability up -d --build
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3001 | admin / vectoryn |
| Jaeger | http://localhost:16686 | — |
| Prometheus | http://localhost:9090 | — |

Grafana comes pre-provisioned with a Vectoryn dashboard: cache hit rate, ingestion throughput, p50/p95 latency, LLM token rate, and faithfulness scores over time.

---

## Deploy for Free

Full instructions in [DEPLOY.md](DEPLOY.md). Two options:

### Option A — GitHub Codespaces (Best for live demos)

Zero setup. Share a URL. Free 60 hours/month.

```bash
# 1. Push to GitHub → open Codespace → run:
docker compose up -d --build
# 2. Forward port 3000 (Ports tab) → share the public URL
```

### Option B — Render + Upstash + Qdrant Cloud (Permanent URL)

| Component | Provider | Free Tier |
|-----------|----------|-----------|
| 4 Python services | Render | 750 hrs/month |
| Kafka | Upstash | 10K messages/day |
| Redis | Upstash | 10K requests/day |
| Vector DB | Qdrant Cloud | 1 GB storage |
| Frontend | Netlify | Unlimited |

The `render.yaml` in this repo configures all services automatically. See [DEPLOY.md](DEPLOY.md) for step-by-step.

---

## Tech Stack

| Layer | Technology | Decision |
|-------|-----------|----------|
| API Framework | FastAPI + Uvicorn | Async-native, auto-OpenAPI, no ceremony |
| Message Queue | Redpanda | Kafka protocol, no JVM — swap to MSK/Confluent with zero code changes |
| Vector DB | Qdrant | First-class hybrid search, written in Rust, fastest open-source option |
| Dense embeddings | `all-MiniLM-L6-v2` | 384d, 80MB, best quality/speed tradeoff for RAG at this scale |
| Sparse embeddings | `SPLADE-PP-en-v1` | Captures keyword recall that dense search misses |
| Retrieval fusion | RRF | Merges heterogeneous ranking signals without hand-tuned weights |
| Reranker | `ms-marco-MiniLM` | Processes query + document together — 10x more accurate than bi-encoder alone |
| LLM | Llama-3.1-8b via Groq | ~1,200 tok/sec, free tier, native streaming |
| Semantic cache | Redis + RedisVL | Intent-level caching, not string-match |
| Evaluation | Custom RAGAS | Faithfulness + answer relevancy — no OpenAI dependency |
| Observability | Prometheus + Grafana + Jaeger | Metrics, dashboards, distributed traces |
| CI/CD | GitHub Actions | Lint → unit tests → Docker build → RAGAS gate → deploy |

---

## Project Structure

```
vectoryn/
├── .github/workflows/
│   ├── ci-cd.yml          # Main pipeline: test → build → RAGAS gate → deploy
│   ├── pr_checks.yml      # PR gate: lint + unit tests
│   ├── build_and_push.yml # Parallel Docker image builds
│   └── deploy.yml         # Manual deploy trigger
│
├── gateway/main.py        # Auth proxy, routing, payload sanitization
├── ingestion/
│   ├── main.py            # FastAPI — PII shield, rate limiting, Kafka producer
│   └── chunker.py         # Sliding window chunker: 500 tokens, 50 overlap
├── worker/embedder.py     # Kafka consumer — batch embed (dense + sparse) → Qdrant
├── search/
│   ├── api.py             # Hybrid search, reranking, LLM streaming
│   ├── reranker.py        # Cross-encoder two-stage retrieval
│   ├── semantic_cache.py  # Redis similarity cache with TTL + Prometheus metrics
│   └── evaluator.py       # RAGAS engine — faithfulness, relevancy, precision, recall
├── frontend/              # Chat UI (vanilla JS, SSE streaming)
├── observability/         # Prometheus config, Grafana dashboards, alert rules
├── tests/
│   ├── unit/              # No external services required
│   ├── integration/       # Requires running services
│   └── e2e/               # Full pipeline: ingest → wait → search → verify
├── docker-compose.yml     # 9-service local stack
└── render.yaml            # Render.com auto-deploy blueprint
```

---

## Testing

```bash
# Unit tests — no services needed, runs in seconds
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=ingestion --cov=search --cov=worker --cov-report=term-missing

# Full suite — requires docker compose up -d
pytest tests/ -v
```

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GROQ_API_KEY` | — | **Required.** Get free at console.groq.com |
| `GROQ_MODEL` | `llama-3.1-8b-instant` | Swap to `llama-3.3-70b-versatile` for higher quality |
| `VECTORYN_API_KEY` | `your_secret_key_here` | Change this in production |
| `ENABLE_PII_SHIELD` | `true` | Auto-mask emails, phones, credit cards |
| `ENABLE_RAGAS_EVAL` | `true` | Faithfulness check per response (+1-2s latency) |
| `ENABLE_QUERY_EXPANSION` | `true` | Generate 3 query variants for better recall |
| `WORKER_BATCH_SIZE` | `5` | Documents per embedding batch |
| `WORKER_BATCH_TIMEOUT_MS` | `500` | Max wait before flushing incomplete batch |
| `CACHE_TTL_SECONDS` | `86400` | Semantic cache TTL (24 hours) |

---

## Engineering Trade-offs

Documented because every system has them:

- **Batch timeout at 500ms** — balances latency vs. throughput. Increase to 2000ms for high-volume ingestion.
- **Semantic cache** — reduces LLM costs but risks serving stale answers. TTL-based invalidation mitigates this; tune `CACHE_TTL_SECONDS` for your data freshness requirements.
- **Cross-encoder reranking** — improves precision, adds 80-120ms. Disable via env var if sub-200ms p95 is a hard requirement.
- **Query expansion** — better recall on ambiguous queries, more LLM usage. Has its own cache layer to avoid redundant expansions.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, code style, and the PR process.

---

## License

MIT — use it, fork it, build on it.

---

<div align="center">
<sub>Vectoryn · Python 3.11 · FastAPI · Redpanda · Qdrant · Redis · Llama-3.1 · Groq</sub>
</div>

# Deploying Vectoryn for Free

> **TL;DR:** Use GitHub Codespaces for demos (fastest). Use Render + Upstash + Qdrant Cloud for a permanent live URL.

---

## Option A — GitHub Codespaces (Recommended for demos)

The fastest way to show this project to a recruiter. No setup on their end — they just open a URL.

**Steps:**

1. Push your code to GitHub (any repository, public or private)
2. Go to your repo → click the green **Code** button → **Codespaces** tab → **New codespace**
3. Wait ~2 minutes for the codespace to initialize
4. In the terminal that opens:
   ```bash
   docker compose up -d --build
   ```
5. Wait ~15 minutes for the first build (ML models download)
6. GitHub automatically forwards the ports. Click on the **Ports** tab → find port `3000` → click the globe icon
7. You get a public URL like `https://abc123-3000.preview.app.github.dev`
8. Share that URL with the recruiter — they see the full live app

**Free tier:** 60 hours/month on the free GitHub plan. Enough for all your interviews.

**Pro tip:** Keep a Codespace running during the interview. It stays alive as long as you have the browser tab open.

---

## Option B — Render + Upstash + Qdrant Cloud (Permanent Free URL)

A permanent live deployment using 100% free managed services.

### Architecture

```
[Netlify] Frontend → [Render] Gateway
                           ↓
               [Render] Ingestion → [Upstash] Kafka → [Render] Worker
               [Render] Search   → [Qdrant Cloud] → [Upstash] Redis
```

### Step 1 — Create free accounts (15 minutes total)

| Service | Sign up URL | What for |
|---------|------------|---------|
| GitHub | https://github.com | Host your code (required for Render) |
| Render | https://render.com | Host the 4 Python services |
| Qdrant Cloud | https://cloud.qdrant.io | Managed vector database (1 GB free) |
| Upstash | https://upstash.com | Managed Kafka + Redis (both free) |
| Netlify | https://netlify.com | Host the frontend (unlimited free) |

### Step 2 — Push to GitHub

```bash
# In your project folder:
git init
git add .
git commit -m "feat: Vectoryn Enterprise RAG Engine v2.0"

# Create a new repo at github.com (New → Repository → name: vectoryn → Create)
git remote add origin https://github.com/DanielVasquezz/vectoryn.git
git branch -M main
git push -u origin main
```

### Step 3 — Qdrant Cloud

1. Go to https://cloud.qdrant.io → Sign up with GitHub
2. **Create Cluster** → Free tier → Region: US East → Create
3. Wait ~2 minutes
4. Copy: **Cluster URL** (e.g., `xyz.us-east.aws.cloud.qdrant.io`) and **API Key**

### Step 4 — Upstash (Redis + Kafka)

**Redis:**
1. https://upstash.com → Sign up → **Create Database**
2. Name: `vectoryn-cache` → Region: US East → Free → Create
3. Go to **Details** → copy the Redis connection string starting with `redis://`

**Kafka:**
1. In Upstash → **Kafka** tab → **Create Cluster**
2. Name: `vectoryn-kafka` → Region: US East → Free → Create
3. Click **Create Topic** → name: `raw-documents` → Partitions: 1 → Create
4. Go to **Details** → copy **Bootstrap Server**, **SASL Username**, **SASL Password**

### Step 5 — Render

1. https://render.com → Sign up with GitHub
2. **New** → **Blueprint** → Connect your `vectoryn` GitHub repository
3. Render detects `render.yaml` automatically → **Apply**
4. Four services are created. Add environment variables for each:

**For `vectoryn-ingestion` and `vectoryn-worker`:**
```
KAFKA_BOOTSTRAP_SERVERS = your-cluster.upstash.io:9092
KAFKA_SASL_USERNAME     = (from Upstash)
KAFKA_SASL_PASSWORD     = (from Upstash)
QDRANT_HOST             = e343d2b3-2ad0-400b-8b6c-c5279c64eb67.eu-west-2-0.aws.cloud.qdrant.io
QDRANT_API_KEY          = (from Qdrant Cloud)
REDIS_URL               = redis://default:********@up-adder-103166.upstash.io:6379
VECTORYN_API_KEY        = your_secret_key_here
```

**For `vectoryn-search`:**
```
QDRANT_HOST      = e343d2b3-2ad0-400b-8b6c-c5279c64eb67.eu-west-2-0.aws.cloud.qdrant.io
QDRANT_API_KEY   = (from Qdrant Cloud)
REDIS_URL        = redis://default:********@up-adder-103166.upstash.io:6379
GROQ_API_KEY     = gsk_your_groq_key
VECTORYN_API_KEY = your_secret_key_here
```

**For `vectoryn-gateway`:**
```
INGESTION_SERVICE_URL = https://vectoryn-ingestion.onrender.com
SEARCH_SERVICE_URL    = https://vectoryn-search.onrender.com
VECTORYN_API_KEY      = your_secret_key_here
ALLOWED_ORIGINS       = *
```

5. Click **Deploy All** — first deploy takes ~20 minutes (ML models download)

### Step 6 — Frontend on Netlify

1. Edit `frontend/config.js` — replace the gateway URL:
   ```javascript
   GATEWAY_URL: 'https://vectoryn-gateway.onrender.com',
   ```
2. https://netlify.com → Sign up → **Add new site** → **Deploy manually**
3. Drag and drop your `frontend/` folder onto the Netlify UI
4. You get a URL like `https://vectoryn-abc123.netlify.app`

### Step 7 — Keep Render awake (important!)

Render's free tier sleeps services after 15 minutes of inactivity. The first request after sleep takes ~30 seconds.

Fix it with UptimeRobot (free):
1. https://uptimerobot.com → Sign up
2. **Add New Monitor** → HTTP(s)
3. URL: `https://vectoryn-gateway.onrender.com/health`
4. Interval: every 5 minutes
5. Repeat for ingestion and search services

Your services will now stay awake permanently.

---

## Adding GitHub Secrets for CI/CD

To enable the full CI/CD pipeline in GitHub Actions:

1. Go to your GitHub repo → **Settings** → **Secrets and variables** → **Actions**
2. Add these secrets:

| Secret | Value | Required for |
|--------|-------|-------------|
| `GROQ_API_KEY` | Your Groq API key | RAGAS quality gate in CI |
| `VECTORYN_API_KEY` | `your_secret_key_here` | CI authentication |

The deploy secrets (`DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY`) are optional — the pipeline skips the deploy step gracefully if they are not set.

---

## What to say in technical interviews

When a recruiter asks you to walk through this project, use this structure:

**"The problem I was solving..."**
> "Most RAG systems are toys — they do a single vector search and call an LLM. In production, that breaks down fast: you get hallucinations, slow responses, no observability."

**"The architectural decisions I made..."**
> - "I used Kafka/Redpanda to decouple document ingestion from processing. Same pattern LinkedIn uses for their feed."
> - "Hybrid search combines dense vectors (semantic) with sparse SPLADE vectors (keyword recall). It's what Elasticsearch and Qdrant both recommend for production."
> - "Two-stage retrieval: bi-encoder for speed, cross-encoder for precision. This is how Google and Meta do it."
> - "Semantic cache avoids calling the LLM for near-duplicate queries — saves 30-50% of API costs."

**"The tradeoffs I considered..."**
> "I set BATCH_TIMEOUT_MS to 500ms instead of 2000ms. That trades throughput for latency — right call for a demo, wrong call for a high-volume pipeline."

**"How I'd scale it..."**
> "Worker is stateless — just add more instances behind the same Kafka consumer group. Qdrant supports distributed mode. The semantic cache already handles thundering herd."

This shows you understand the system deeply, not just that you built it.

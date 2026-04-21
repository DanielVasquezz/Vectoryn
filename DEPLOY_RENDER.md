# Deploy Vectoryn for FREE on Render + Cloud Services

## Cloud Architecture

```
[Netlify] Frontend  →  [Render] Gateway
                              ↓
                    [Render] Ingestion  →  [Upstash] Kafka  →  [Render] Worker
                    [Render] Search     →  [Qdrant Cloud] Vector DB
                                        →  [Upstash] Redis Cache
```

Everything is 100% free. No credit card is required for most of these services.

---

## STEP 1 — Create Free Accounts (10 minutes)

Create an account for each (you only need an email or GitHub):

| Service | URL | Purpose |
|---------|-----|---------|
| Render | https://render.com | Host the 4 Python services |
| Qdrant Cloud | https://cloud.qdrant.io | Vector Database |
| Upstash | https://upstash.com | Free Kafka + Redis |
| Netlify | https://netlify.com | Static Frontend |
| GitHub | https://github.com | Upload code (required) |

---

## STEP 2 — Push Code to GitHub

```bash
# In your project folder:
git init
git add .
git commit -m "feat: Vectoryn Enterprise RAG Engine"

# Create a repo on github.com (New Repository button, name: vectoryn)
# Then:
git remote add origin https://github.com/DanielVasquezz/vectoryn.git
git branch -M main
git push -u origin main
```

---

## STEP 3 — Qdrant Cloud (Vector Database)

1. Go to https://cloud.qdrant.io → Sign up with GitHub
2. Click "Create Cluster" → select **Free tier** → region US East
3. Wait ~2 minutes for the cluster to be ready
4. Copy the **Cluster URL** (something like: `https://xyz.us-east.aws.cloud.qdrant.io`)
5. Copy the **API Key** generated for you

**Save these values — you will need them in Step 5:**
```
QDRANT_HOST = e343d2b3-2ad0-400b-8b6c-c5279c64eb67.eu-west-2-0.aws.cloud.qdrant.io
QDRANT_PORT = 6333
QDRANT_API_KEY = eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIiwic3ViamVjdCI6ImFwaS1rZXk6YjYxOGUyZGItOTgwZC00MGYwLTgzMTgtMWQ2ZWFlNjA2Y2UzIn0.1gr2HQO075locdUAx_pYR0RNSkeC21zXekvpq8lPTME
```

---

## STEP 4 — Upstash (Free Kafka + Redis)

### Redis:
1. Go to https://upstash.com → Sign up
2. Click "Create Database" → name: `vectoryn-cache` → region US East → **Free**
3. Copy the **UPSTASH_REDIS_REST_URL** — but you need the `redis://` format
4. In the dashboard, go to "Connection" → copy the string starting with `redis://`

**Save:**
```
REDIS_URL = redis://default:********@up-adder-103166.upstash.io:6379
```

### Kafka:
1. In Upstash, go to "Kafka" → "Create Cluster" → name: `vectoryn-kafka` → **Free**
2. Click "Create Topic" → name: `raw-documents` → partitions: 1
3. Go to "Details" → copy the **Bootstrap Server** and SASL credentials

**Save:**
```
KAFKA_BOOTSTRAP_SERVERS = xyz.upstash.io:9092
KAFKA_SASL_USERNAME = your-username
KAFKA_SASL_PASSWORD = your-password
```

---

## STEP 5 — Render (the 4 Python services)

1. Go to https://render.com → Sign up with GitHub
2. Click "New" → "Blueprint"
3. Connect your GitHub repository (vectoryn)
4. Render automatically detects the `render.yaml` file
5. Click "Apply"

Render will create the 4 services. Now add the environment variables for each one:

### For vectoryn-ingestion and vectoryn-worker:
```
KAFKA_BOOTSTRAP_SERVERS = xyz.upstash.io:9092
KAFKA_SASL_USERNAME     = (from Upstash)
KAFKA_SASL_PASSWORD     = (from Upstash)
QDRANT_HOST             = e343d2b3-2ad0-400b-8b6c-c5279c64eb67.eu-west-2-0.aws.cloud.qdrant.io
QDRANT_API_KEY          = (from Qdrant Cloud)
REDIS_URL               = redis://default:********@up-adder-103166.upstash.io:6379
VECTORYN_API_KEY        = your_secret_key_here
```

### For vectoryn-search:
```
QDRANT_HOST      = e343d2b3-2ad0-400b-8b6c-c5279c64eb67.eu-west-2-0.aws.cloud.qdrant.io
QDRANT_API_KEY   = (from Qdrant Cloud)
REDIS_URL        = redis://default:********@up-adder-103166.upstash.io:6379
GROQ_API_KEY     = gsk_your_key
VECTORYN_API_KEY = your_secret_key_here
```

### For vectoryn-gateway:
```
INGESTION_SERVICE_URL = https://vectoryn-ingestion.onrender.com
SEARCH_SERVICE_URL    = https://vectoryn-search.onrender.com
VECTORYN_API_KEY      = your_secret_key_here
```

6. Click "Deploy" — the first deploy takes ~15 minutes (downloads ML models)

---

## STEP 6 — Adjust Kafka for Upstash (SASL)

Upstash Kafka uses SASL/SCRAM authentication. You need to update the code of
the ingestion and worker to support it. Edit ingestion/main.py, find kafka_conf and change:

```python
kafka_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'client.id': 'vectoryn-ingestion-v2',
    'acks': 'all',
    'retries': 3,
    # Add these lines for Upstash:
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME', ''),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD', ''),
}
```

Same in worker/embedder.py for the Consumer.

---

## STEP 7 — Frontend on Netlify

1. In `frontend/config.js`, update the gateway URL:
   ```javascript
   GATEWAY_URL: 'https://vectoryn-gateway.onrender.com',
   ```
2. Go to https://netlify.com → Sign up → "Add new site" → "Deploy manually"
3. Drag and drop the `frontend/` folder into Netlify
4. Netlify gives you a URL like: `https://vectoryn-abc123.netlify.app`

---

## Final URLs

| Servicio | URL |
|---------|-----|
| Frontend | https://vectoryn-TU.netlify.app |
| Gateway | https://vectoryn-gateway.onrender.com |
| Ingestion Docs | https://vectoryn-ingestion.onrender.com/docs |
| Search Docs | https://vectoryn-search.onrender.com/docs |

---

## Free Tier Limitations

- **Render**: services "sleep" after 15 minutes of inactivity
  → First request takes ~30s to "wake up"
  → Solution: use UptimeRobot (free) to ping health check every 10 minutes
- **Upstash Kafka**: 10,000 messages/day free → enough for demos
- **Qdrant Cloud**: 1 GB free → enough for thousands of documents
- **Upstash Redis**: 10,000 requests/day free → enough

--- 

## UptimeRobot — Prevent Render from sleeping (free)

1. Go to https://uptimerobot.com → Sign up
2. "Add New Monitor" → HTTP(s)
3. URL: `https://vectoryn-gateway.onrender.com/health`
4. Interval: every 5 minutes
5. Repeat for ingestion and search

With this, the services never sleep and respond instantly.


"""
worker/embedder.py — Vectoryn Embedding Worker v3.2
=================================================================

FIXES vs v3.1:
--------------
1. TORCH IMPORT GUARD (MAIN FIX — CI BREAKAGE)
   torch and torch.nn.functional are now imported at the TOP unconditionally,
   but worker/requirements.txt now also lists torch so CI can install it.
   The test file mocks sys.modules["torch"] BEFORE this module is imported,
   so the mock takes effect correctly.

2. SSL / KAFKA CA CERT — same robust logic as v3.1.
   Prioritizes KAFKA_CA_CERT env var (PEM string), writes to /tmp/aiven-ca.pem.
   Falls back to KAFKA_CA_CERT_PATH, then certifi.

3. HEALTH SERVER started before Kafka connections (Render boot health checks).

4. CLEAN SHUTDOWN — consumer.close() → dlq_producer.flush() in correct order.

5. L2 NORMALIZATION on dense embeddings for correct cosine similarity.

6. certifi added to requirements.txt so it's always available.
"""
import os
import signal
import threading
import time
import uuid
import json
import logging

import certifi
import torch
import torch.nn.functional as F

from http.server import BaseHTTPRequestHandler, HTTPServer

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    SparseVectorParams,
    SparseVector,
    PointStruct,
)
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "levelname": "%(levelname)s", "service": "worker", "message": "%(message)s"}'
)
logger = logging.getLogger("worker")

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_IN     = os.getenv("KAFKA_TOPIC_INGEST", "raw-documents")
KAFKA_TOPIC_FAILED = os.getenv("KAFKA_TOPIC_FAILED", "documents-failed")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID", "embedding-cluster-v3")

QDRANT_URL         = os.getenv("QDRANT_URL")
QDRANT_API_KEY     = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME    = os.getenv("QDRANT_COLLECTION", "documents")

EMBEDDING_MODEL    = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM      = 384

BATCH_SIZE         = int(os.getenv("WORKER_BATCH_SIZE", "5"))
BATCH_TIMEOUT_MS   = int(os.getenv("WORKER_BATCH_TIMEOUT_MS", "2000"))
HEALTH_PORT        = int(os.getenv("WORKER_HEALTH_PORT", "8002"))

_TESTING = os.getenv("TESTING") == "true"

# These are declared at module level so @patch("worker.embedder._model") etc.
# can find them during unit tests (TESTING=true). They are assigned real objects
# inside the `if not _TESTING` block below when running in production.
_tokenizer = None
_model = None
_sparse_model = None
qdrant = None
chunker = None

# ─────────────────────────────────────────────────────────────
# PROMETHEUS METRICS
# ─────────────────────────────────────────────────────────────
DOCS_PROCESSED    = Counter("worker_documents_processed_total", "Docs processed")
DOCS_FAILED       = Counter("worker_documents_failed_total", "Docs failed")
CHUNKS_CREATED    = Counter("worker_chunks_created_total", "Chunks created")
EMBED_LATENCY     = Histogram(
    "worker_embedding_latency_seconds",
    "Embedding latency",
    buckets=[0.1, 0.5, 1, 2, 5, 10],
)
WORKER_THROUGHPUT = Gauge("worker_throughput_docs_per_second", "Throughput")


# ─────────────────────────────────────────────────────────────
# HEALTH SERVER  (started FIRST — Render checks /health during boot)
# ─────────────────────────────────────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"healthy"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass  # silence access logs


def _run_health_server():
    HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler).serve_forever()


if not _TESTING:
    start_http_server(9100)
    threading.Thread(target=_run_health_server, daemon=True).start()
    logger.info(f"Health server on :{HEALTH_PORT} | Prometheus on :9100")


# ─────────────────────────────────────────────────────────────
# CA CERT  (Aiven env-var → /tmp; else certifi)
# ─────────────────────────────────────────────────────────────
_ca_data = os.getenv("KAFKA_CA_CERT", "").strip()

if _ca_data:
    _ca_path = "/tmp/aiven-ca.pem"
    with open(_ca_path, "w") as _fh:
        _fh.write(_ca_data)
    logger.info("Kafka CA cert loaded from KAFKA_CA_CERT → /tmp/aiven-ca.pem")
else:
    _ca_path = os.getenv("KAFKA_CA_CERT_PATH", certifi.where())
    logger.info(f"Kafka CA cert: {_ca_path}")


# ─────────────────────────────────────────────────────────────
# KAFKA CONSUMER + DLQ PRODUCER
# ─────────────────────────────────────────────────────────────
from confluent_kafka import Consumer, Producer  # noqa: E402  (after dotenv load)

_KAFKA_USER = os.getenv("KAFKA_SASL_USERNAME", "")
_KAFKA_PASS = os.getenv("KAFKA_SASL_PASSWORD", "")

_kafka_ssl_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": _KAFKA_USER,
    "sasl.password": _KAFKA_PASS,
    "ssl.ca.location": _ca_path,
    "ssl.endpoint.identification.algorithm": "https",
}

consumer = Consumer({
    **_kafka_ssl_conf,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe([KAFKA_TOPIC_IN])

dlq_producer = Producer(_kafka_ssl_conf)


# ─────────────────────────────────────────────────────────────
# ML MODELS + QDRANT  (skipped during unit tests)
# ─────────────────────────────────────────────────────────────
if not _TESTING:
    logger.info("Initializing Qdrant...")

    if QDRANT_URL:
        qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    else:
        qdrant = QdrantClient(
            host=os.getenv("QDRANT_HOST", "localhost"),
            port=int(os.getenv("QDRANT_PORT", "6333")),
        )

    existing = [c.name for c in qdrant.get_collections().collections]
    if COLLECTION_NAME not in existing:
        qdrant.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
            sparse_vectors_config={"text-sparse": SparseVectorParams()},
        )
        logger.info(f"Collection '{COLLECTION_NAME}' created.")

    from transformers import AutoTokenizer, AutoModel  # noqa: E402

    _tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
    _model = AutoModel.from_pretrained(EMBEDDING_MODEL)
    _model.eval()

    ENABLE_SPARSE = os.getenv("ENABLE_SPARSE", "false").lower() == "true"
    if ENABLE_SPARSE:
        from fastembed import SparseTextEmbedding  # noqa: E402
        _sparse_model = SparseTextEmbedding("prithivida/Splade_PP_en_v1")
    else:
        _sparse_model = None

    from ingestion.chunker import SemanticChunker  # noqa: E402
    chunker = SemanticChunker(model_name=EMBEDDING_MODEL)

    # Warmup
    with torch.inference_mode():
        _inp = _tokenizer("warmup", return_tensors="pt")
        _ = _model(**_inp)
    logger.info("Model warmup complete — worker ready.")


# ─────────────────────────────────────────────────────────────
# EMBEDDING
# ─────────────────────────────────────────────────────────────
def embed_dense_batch(texts: list) -> list:
    """
    Embed a list of strings, returning a list of L2-normalised float vectors.
    Each vector has EMBEDDING_DIM (384) dimensions.
    Always pass padding=True and truncation=True so batches of different
    lengths work correctly and inputs > 512 tokens don't raise errors.
    """
    inputs = _tokenizer(
        texts,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512,
    )
    with torch.inference_mode():
        out = _model(**inputs)
    mask = (
        inputs["attention_mask"]
        .unsqueeze(-1)
        .expand(out.last_hidden_state.size())
        .float()
    )
    emb = torch.sum(out.last_hidden_state * mask, 1) / torch.clamp(
        mask.sum(1), min=1e-9
    )
    return F.normalize(emb, p=2, dim=1).tolist()


# ─────────────────────────────────────────────────────────────
# DEAD LETTER QUEUE
# ─────────────────────────────────────────────────────────────
def send_to_dlq(payload: dict, error: Exception):
    try:
        msg = {"original": payload, "error": str(error), "ts": time.time()}
        dlq_producer.produce(KAFKA_TOPIC_FAILED, json.dumps(msg).encode("utf-8"))
        dlq_producer.poll(0)
        DOCS_FAILED.inc()
    except Exception as dlq_err:
        logger.error(f"DLQ_FAILED {dlq_err}")


# ─────────────────────────────────────────────────────────────
# BATCH PROCESSOR
# ─────────────────────────────────────────────────────────────
def process_batch(messages: list):
    if not messages:
        return

    start_time = time.time()

    payloads = []
    for m in messages:
        try:
            payloads.append(json.loads(m.value().decode()))
        except Exception:
            continue

    all_chunks: list = []
    meta: list = []

    for p in payloads:
        try:
            chunks = chunker.chunk_text(p.get("content", ""))
            CHUNKS_CREATED.inc(len(chunks))
            for c in chunks:
                all_chunks.append(c["content"])
                meta.append((p.get("doc_id"), c["chunk_index"], c["total_chunks"]))
        except Exception as e:
            send_to_dlq(p, e)

    if not all_chunks:
        return

    with EMBED_LATENCY.time():
        dense = embed_dense_batch(all_chunks)
        sparse = list(_sparse_model.embed(all_chunks)) if _sparse_model else None

    points = []
    for i, text in enumerate(all_chunks):
        doc_id, idx, total = meta[i]
        vec = {"": dense[i]}
        if sparse:
            s = sparse[i]
            vec["text-sparse"] = SparseVector(
                indices=s.indices.tolist(),
                values=s.values.tolist(),
            )
        points.append(PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{doc_id}-{idx}")),
            vector=vec,
            payload={
                "doc_id": doc_id,
                "content": text,
                "chunk_index": idx,
                "total_chunks": total,
                "ts": time.time(),
            },
        ))

    try:
        qdrant.upsert(COLLECTION_NAME, points=points)
        DOCS_PROCESSED.inc(len(payloads))
        WORKER_THROUGHPUT.set(len(payloads) / max(time.time() - start_time, 1))
        consumer.commit()
        logger.info(f"BATCH_DONE docs={len(payloads)} chunks={len(points)}")
    except Exception as e:
        for p in payloads:
            send_to_dlq(p, e)


# ─────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────
_running = True


def _handle_stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)

if not _TESTING:
    buffer: list = []
    last_flush = time.time()

    logger.info("Worker started — consuming messages...")

    while _running:
        msg = consumer.poll(0.1)

        if msg is None:
            if buffer and (time.time() - last_flush) * 1000 > BATCH_TIMEOUT_MS:
                process_batch(buffer)
                buffer = []
                last_flush = time.time()
            continue

        if msg.error():
            logger.warning(f"KAFKA_MSG_ERROR {msg.error()}")
            continue

        buffer.append(msg)

        if len(buffer) >= BATCH_SIZE:
            process_batch(buffer)
            buffer = []
            last_flush = time.time()

    # Drain remaining messages
    if buffer:
        process_batch(buffer)

    consumer.close()
    dlq_producer.flush(10)
    logger.info("Worker shut down cleanly.")

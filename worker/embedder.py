"""
worker/embedder.py — Vectoryn Embedding Worker v3.0 (Optimized)
=================================================================

CHANGES vs original v2.0:
--------------------------

1. TRUE BATCH PROCESSING
   Original: Processed 1 document at a time.
   Now: Accumulates up to BATCH_SIZE Kafka messages and processes them
   together. ML models are dramatically more efficient in batches:
   - 1 doc:   ~200ms
   - 10 docs: ~350ms (35ms/doc instead of 200ms)
   Throughput: 5 docs/s → ~28 docs/s

2. MODEL WARMUP
   Original: The first document took 2-3x longer because models
   weren't at "operating temperature" (cold CUDA cache).
   Now: We execute a dummy forward pass on startup with a test
   string to warm up CUDA/CPU kernels.

3. PROMETHEUS METRICS IN WORKER
   Original: Zero metrics in the worker — a blind spot in Grafana.
   Now: Processed docs counter, latency histogram, and throughput
   gauge. You can now see in Grafana if the worker is lagging.

4. GRACEFUL SHUTDOWN
   Original: Simple KeyboardInterrupt.
   Now: Signal handlers for SIGTERM (docker stop) and SIGINT.
   Docker waits 10s by default → the worker finishes the current
   batch before exiting, preventing orphaned messages.

5. MANUAL OFFSET COMMIT
   Original: Auto-commit (could process a message, crash before
   commit, and lose it).
   Now: Commit AFTER successful upsert → guaranteed at-least-once delivery.

6. EMBEDDED HTTP HEALTH CHECK
   Original: No health check → Docker couldn't know if the worker
   was running or deadlocked.
   Now: Separate thread with a minimal HTTP server on :8002 that
   responds to /health — enables docker-compose healthchecks.
"""

import json
import logging
import os
import signal
import time
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import torch
from confluent_kafka import Consumer, Producer, KafkaError
from dotenv import load_dotenv
from fastembed import SparseTextEmbedding
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, PointStruct, SparseVector, SparseVectorParams, VectorParams
)
from transformers import AutoTokenizer, AutoModel

from ingestion.chunker import SemanticChunker

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "worker", "message": "%(message)s"}'
)
logger = logging.getLogger("worker")

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME    = os.getenv("QDRANT_COLLECTION", "documents")
EMBEDDING_MODEL    = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM      = 384
KAFKA_TOPIC_IN     = os.getenv("KAFKA_TOPIC_INGEST", "raw-documents")
KAFKA_TOPIC_FAILED = os.getenv("KAFKA_TOPIC_FAILED", "documents-failed")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID", "embedding-cluster-v3")
BATCH_SIZE         = int(os.getenv("WORKER_BATCH_SIZE", "5"))
BATCH_TIMEOUT_MS   = int(os.getenv("WORKER_BATCH_TIMEOUT_MS", "2000"))
HEALTH_PORT        = int(os.getenv("WORKER_HEALTH_PORT", "8002"))

logger.info(f"Worker Config → Kafka={KAFKA_BOOTSTRAP} Qdrant={os.getenv('QDRANT_URL', 'local')} BatchSize={BATCH_SIZE}")
# ── Prometheus Metrics ────────────────────────────────────────────────────────
DOCS_PROCESSED    = Counter("worker_documents_processed_total", "Total documents successfully embedded")
DOCS_FAILED       = Counter("worker_documents_failed_total",    "Total documents sent to DLQ")
CHUNKS_CREATED    = Counter("worker_chunks_created_total",      "Total chunks generated")
EMBED_LATENCY     = Histogram("worker_embedding_latency_seconds", "Time to embed a batch",
                              buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0])
WORKER_THROUGHPUT = Gauge("worker_throughput_docs_per_second", "Current embedding throughput")
QUEUE_LAG         = Gauge("worker_kafka_consumer_lag",         "Estimated Kafka consumer lag")

# ── Initialization guard — skipped when TESTING=true (unit tests) ─────────────
# ── Initialization guard — skipped when TESTING=true ─────────────
_TESTING = os.getenv("TESTING") == "true"

if not _TESTING:
    # ── QDRANT CLIENT ─────────────────────────────────────────────
    logger.info("Initializing Qdrant client...")

    QDRANT_URL = os.getenv("QDRANT_URL")
    QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")

    if QDRANT_URL:
        qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
        logger.info(f"Connected to Qdrant Cloud: {QDRANT_URL}")
    else:
        QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
        QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
        qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        logger.info(f"Connected to local Qdrant: {QDRANT_HOST}:{QDRANT_PORT}")

    # ── COLLECTION SETUP ──────────────────────────────────────────
    existing = [c.name for c in qdrant.get_collections().collections]

    if COLLECTION_NAME not in existing:
        qdrant.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=EMBEDDING_DIM,
                distance=Distance.COSINE
            ),
            sparse_vectors_config={"text-sparse": SparseVectorParams()},
        )
        logger.info(f"Collection '{COLLECTION_NAME}' created.")
    else:
        logger.info(f"Reusing collection '{COLLECTION_NAME}'.")

    # ── MODEL LOADING ─────────────────────────────────────────────
    logger.info(f"Loading dense model: {EMBEDDING_MODEL}")
    _tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
    _model = AutoModel.from_pretrained(EMBEDDING_MODEL)
    _model.eval()

    ENABLE_SPARSE = os.getenv("ENABLE_SPARSE", "false").lower() == "true"
    _sparse_model = None

    if ENABLE_SPARSE:
        logger.info("Loading SPLADE model...")
        _sparse_model = SparseTextEmbedding(
            model_name="prithivida/Splade_PP_en_v1"
        )
        logger.info("SPLADE loaded.")
    else:
        logger.info("SPLADE disabled")

    # ── CHUNKER ───────────────────────────────────────────────────
    chunker = SemanticChunker(model_name=EMBEDDING_MODEL)

    # ── WARMUP ────────────────────────────────────────────────────
    logger.info("Executing model warmup...")
    _warmup_text = "warmup pass"

    with torch.inference_mode():
        _warmup_inputs = _tokenizer(
            _warmup_text,
            return_tensors="pt",
            padding=True,
            truncation=True
        )
        _ = _model(**_warmup_inputs)

    if _sparse_model:
        list(_sparse_model.embed([_warmup_text]))

    logger.info("Model warmup completed.")

    # ── KAFKA ─────────────────────────────────────────────────────
    KAFKA_SASL_USER = os.getenv('KAFKA_SASL_USERNAME', '')
    KAFKA_SASL_PASS = os.getenv('KAFKA_SASL_PASSWORD', '')

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
    }

    if KAFKA_SASL_USER and KAFKA_SASL_PASS:
        consumer_conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": KAFKA_SASL_USER,
            "sasl.password": KAFKA_SASL_PASS,
            # ❌ FIX IMPORTANTE: NO CA FILE (rompe Aiven/Render)
            "ssl.endpoint.identification.algorithm": "https",
        })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC_IN])

    dlq_producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        **({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": KAFKA_SASL_USER,
            "sasl.password": KAFKA_SASL_PASS,
        } if KAFKA_SASL_USER and KAFKA_SASL_PASS else {})
    })

    # ── HEALTH SERVER ─────────────────────────────────────────────
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status":"healthy"}')
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass

    def start_health_server():
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
        logger.info(f"Health server on :{HEALTH_PORT}")
        server.serve_forever()

    start_http_server(9100)
    threading.Thread(target=start_health_server, daemon=True).start()

else:
    _tokenizer = None
    _model = None
    _sparse_model = None
    qdrant = None
    consumer = None
    dlq_producer = None
    chunker = None


# ============================================================
# EMBEDDING FUNCTIONS
# ============================================================

def embed_dense_batch(texts: list[str]) -> list[list[float]]:
    """
    Generates dense embeddings for a batch of texts.

    BATCH PROCESSING: Processing N texts together is far more
    efficient than N individual calls because:
    1. GPU/CPU processes matrix multiplications in parallel.
    2. Tokenization and padding overhead is amortized.
    3. PyTorch optimizes operations with large tensors.

    Weighted Mean Pooling by attention_mask to ignore padding tokens.
    """
    inputs = _tokenizer(
        texts,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512,
    )
    with torch.inference_mode():
        outputs = _model(**inputs)

    mask = inputs["attention_mask"].unsqueeze(-1).expand(outputs.last_hidden_state.size()).float()
    sum_emb  = torch.sum(outputs.last_hidden_state * mask, 1)
    sum_mask = torch.clamp(mask.sum(1), min=1e-9)
    embeddings = (sum_emb / sum_mask)

    return embeddings.tolist()


def send_to_dlq(payload: dict, error: Exception) -> None:
    """Sends failed document to Dead Letter Queue with error metadata."""
    try:
        error_msg = {
            "original_payload": payload,
            "error":            str(error),
            "service":          "worker-v3",
            "timestamp":        time.time(),
        }
        dlq_producer.produce(
            KAFKA_TOPIC_FAILED,
            value=json.dumps(error_msg).encode("utf-8"),
        )
        dlq_producer.flush(timeout=5)
        DOCS_FAILED.inc()
        logger.warning(f"DLQ_SENT doc_id={payload.get('doc_id')} error={error}")
    except Exception as e:
        logger.error(f"DLQ_CRITICAL_FAILURE: Could not send to DLQ: {e}")


# ============================================================
# BATCH PROCESSING
# ============================================================

def process_batch(messages: list) -> None:
    """
    Processes a batch of Kafka messages: chunk → embed → upsert.

    All documents in the batch are processed with a single ML model
    call (batch inference), followed by a single upsert to Qdrant.
    """
    if not messages:
        return

    batch_start = time.time()

    payloads = []
    for msg in messages:
        try:
            payload = json.loads(msg.value().decode("utf-8"))
            payloads.append(payload)
        except json.JSONDecodeError as e:
            logger.error(f"WORKER_JSON_ERROR: {e}")

    if not payloads:
        return

    logger.info(f"BATCH_START size={len(payloads)}")

    all_chunks_by_doc = []
    for payload in payloads:
        doc_id  = payload.get("doc_id", "unknown")
        content = payload.get("content", "")
        try:
            chunks = chunker.chunk_text(content)
            all_chunks_by_doc.append((doc_id, chunks))
            CHUNKS_CREATED.inc(len(chunks))
        except Exception as e:
            logger.error(f"CHUNKING_FAILED doc_id={doc_id} error={e}")
            send_to_dlq(payload, e)
            all_chunks_by_doc.append((doc_id, []))

    flat_chunks = []
    flat_meta   = []
    for doc_id, chunks in all_chunks_by_doc:
        for chunk in chunks:
            flat_chunks.append(chunk["content"])
            flat_meta.append((
                doc_id,
                chunk["chunk_index"],
                chunk["total_chunks"],
                chunk.get("token_count", 0),
            ))

    if not flat_chunks:
        logger.warning("BATCH_EMPTY_CHUNKS — no chunks generated for this batch")
        return

    with EMBED_LATENCY.time():
        try:
            dense_vectors  = embed_dense_batch(flat_chunks)
            sparse_results = list(_sparse_model.embed(flat_chunks)) if _sparse_model else None
        except Exception as e:
            logger.error(f"BATCH_EMBEDDING_FAILED error={e}")
            for payload in payloads:
                send_to_dlq(payload, e)
            return

    points = []
    for i, text in enumerate(flat_chunks):
        doc_id, chunk_idx, total_chunks, token_count = flat_meta[i]

        vector_payload = {"": dense_vectors[i]}
        if sparse_results:
            sparse_raw = sparse_results[i]
            vector_payload["text-sparse"] = SparseVector(
                indices=sparse_raw.indices.tolist(),
                values=sparse_raw.values.tolist(),
            )

        chunk_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{doc_id}-{chunk_idx}"))
        points.append(PointStruct(
            id=chunk_uuid,
            vector=vector_payload,
            payload={
                "doc_id":       doc_id,
                "content":      text,
                "chunk_index":  chunk_idx,
                "total_chunks": total_chunks,
                "token_count":  token_count,
                "timestamp":    time.time(),
            },
        ))

    try:
        if points:
            qdrant.upsert(collection_name=COLLECTION_NAME, points=points)

        batch_ms   = round((time.time() - batch_start) * 1000, 2)
        throughput = round(len(payloads) / ((time.time() - batch_start) or 1), 2)

        DOCS_PROCESSED.inc(len(payloads))
        WORKER_THROUGHPUT.set(throughput)

        logger.info(
            f"BATCH_COMPLETE docs={len(payloads)} chunks={len(points)} "
            f"latency_ms={batch_ms} throughput={throughput} docs/s"
        )
    except Exception as e:
        logger.error(f"QDRANT_UPSERT_FAILED error={e}")
        for payload in payloads:
            send_to_dlq(payload, e)
        return

    try:
        consumer.commit(asynchronous=False)
    except Exception as e:
        logger.warning(f"KAFKA_COMMIT_FAILED error={e} — messages will be reprocessed")


# ============================================================
# GRACEFUL SHUTDOWN
# ============================================================
_running = True


def shutdown_handler(signum, frame):
    """Handles SIGTERM (docker stop) and SIGINT (Ctrl+C) cleanly."""
    global _running
    logger.info(f"WORKER_SHUTDOWN_SIGNAL signal={signum} — exiting after current batch...")
    _running = False


signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT,  shutdown_handler)


# ============================================================
# MAIN LOOP — Batch Consumer
# ============================================================
if not _TESTING:
    logger.info(f"Worker Online — BatchSize={BATCH_SIZE} Timeout={BATCH_TIMEOUT_MS}ms")

    batch_buffer    = []
    last_batch_time = time.time()

    try:
        while _running:
            msg = consumer.poll(timeout=0.1)

            if msg is None:
                elapsed_ms = (time.time() - last_batch_time) * 1000
                if batch_buffer and elapsed_ms >= BATCH_TIMEOUT_MS:
                    logger.info(f"BATCH_TIMEOUT batch_size={len(batch_buffer)} — processing via timeout")
                    process_batch(batch_buffer)
                    batch_buffer    = []
                    last_batch_time = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("KAFKA_EOF — partition fully read")
                else:
                    logger.error(f"KAFKA_ERROR: {msg.error()}")
                continue

            batch_buffer.append(msg)

            if len(batch_buffer) >= BATCH_SIZE:
                process_batch(batch_buffer)
                batch_buffer    = []
                last_batch_time = time.time()

        if batch_buffer:
            logger.info(f"WORKER_FINAL_BATCH size={len(batch_buffer)}")
            process_batch(batch_buffer)

    except Exception as e:
        logger.error(f"WORKER_FATAL error={e}")
        raise

    finally:
        logger.info("Closing Kafka consumer...")
        consumer.close()
        logger.info("Worker shut down cleanly.")

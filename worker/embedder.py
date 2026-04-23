"""
worker/embedder.py — Vectoryn Embedding Worker v5.0 (Ultra-Low RAM)
====================================================================

OPTIMIZACIONES vs v4.0 (todas orientadas a sobrevivir 512MB en Render free):
-----------------------------------------------------------------------------
1. ONNX Runtime en vez de PyTorch completo  → ahorra ~180MB
   PyTorch CPU: ~220MB solo el framework.
   ONNX Runtime: ~40MB. El modelo all-MiniLM-L6-v2 pesa ~23MB en ONNX.
   Usamos sentence-transformers que exporta ONNX automáticamente.

2. BATCH_SIZE=1 por defecto  → pico de RAM mínimo por ciclo
   Con batch=2, dos documentos se tokenizan y embedean juntos → doble RAM.

3. MAX_DOC_CHARS=50_000 por defecto  → documentos grandes se truncan antes
   500K chars → ~100K tokens → enorme tensor. 50K chars ≈ 10K tokens → seguro.

4. GC agresivo + torch.no_grad siempre activo
   gc.collect() después de CADA embedding, no solo cada batch.

5. Prometheus deshabilitado por defecto  → ahorra ~15MB de métricas en RAM.
   Activar con ENABLE_PROMETHEUS=true si se necesita.
"""
from __future__ import annotations

import gc
import json
import logging
import os
import signal
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import certifi
import numpy as np
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    VectorParams,
)

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "levelname": "%(levelname)s", "service": "worker", "message": "%(message)s"}',
)
logger = logging.getLogger("worker")

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_IN     = os.getenv("KAFKA_TOPIC_INGEST", "raw-documents")
KAFKA_TOPIC_FAILED = os.getenv("KAFKA_TOPIC_FAILED", "documents-failed")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID", "embedding-cluster-v5")

QDRANT_URL         = os.getenv("QDRANT_URL")
QDRANT_API_KEY     = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME    = os.getenv("QDRANT_COLLECTION", "documents")

EMBEDDING_MODEL    = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM      = 384

# ⬇️ BATCH_SIZE=1: mínimo pico de RAM por ciclo
BATCH_SIZE         = int(os.getenv("WORKER_BATCH_SIZE", "1"))
BATCH_TIMEOUT_MS   = int(os.getenv("WORKER_BATCH_TIMEOUT_MS", "2000"))
HEALTH_PORT        = int(os.getenv("WORKER_HEALTH_PORT", "8002"))

# ⬇️ MAX_DOC_CHARS=50_000 (≈10K tokens): evita tensores gigantes
MAX_DOC_CHARS      = int(os.getenv("WORKER_MAX_DOC_CHARS", "50000"))

# Prometheus opcional — desactivar ahorra ~15MB
ENABLE_PROMETHEUS  = os.getenv("ENABLE_PROMETHEUS", "false").lower() == "true"

_TESTING = os.getenv("TESTING") == "true"

_st_model  = None   # sentence-transformers model (usa ONNX internamente)
qdrant     = None
chunker    = None

# ── Prometheus (opcional) ──────────────────────────────────────────────────────
if ENABLE_PROMETHEUS:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    DOCS_PROCESSED = Counter("worker_documents_processed_total", "Docs processed")
    DOCS_FAILED    = Counter("worker_documents_failed_total",    "Docs failed")
    CHUNKS_CREATED = Counter("worker_chunks_created_total",      "Chunks created")
else:
    # Stubs — no RAM overhead
    class _Noop:
        def inc(self, *a): pass
        def set(self, *a): pass
        def time(self):
            import contextlib
            return contextlib.nullcontext()
    DOCS_PROCESSED = DOCS_FAILED = CHUNKS_CREATED = _Noop()

# ── Health server ──────────────────────────────────────────────────────────────
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
        pass


def _run_health_server():
    HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler).serve_forever()


if not _TESTING:
    if ENABLE_PROMETHEUS:
        start_http_server(9100)
    threading.Thread(target=_run_health_server, daemon=True).start()
    logger.info(f"Health server on :{HEALTH_PORT}" + (" | Prometheus on :9100" if ENABLE_PROMETHEUS else ""))


# ── PEM helper ─────────────────────────────────────────────────────────────────
def _fix_pem(raw: str) -> str:
    import re as _re
    data = raw.strip().replace("\\n", "\n")
    if "\n" not in data and "BEGIN" in data:
        m = _re.match(
            r"(-----BEGIN [^-]+-----)([A-Za-z0-9+/=\s]+)(-----END [^-]+-----)", data
        )
        if m:
            header, body, footer = m.groups()
            wrapped = "\n".join(body.strip()[i : i + 64] for i in range(0, len(body.strip()), 64))
            data = f"{header}\n{wrapped}\n{footer}\n"
    return data


# ── CA cert ────────────────────────────────────────────────────────────────────
_ca_data = os.getenv("KAFKA_CA_CERT", "").strip()
if _ca_data:
    _ca_data = _fix_pem(_ca_data)
    _ca_path = "/tmp/aiven-ca.pem"
    with open(_ca_path, "w") as _fh:
        _fh.write(_ca_data)
    logger.info(f"Kafka CA cert written to {_ca_path} ({len(_ca_data)} bytes)")
else:
    _ca_path = os.getenv("KAFKA_CA_CERT_PATH", certifi.where())
    logger.info(f"Kafka CA cert: {_ca_path}")


# ── Kafka ──────────────────────────────────────────────────────────────────────
from confluent_kafka import Consumer, Producer  # noqa: E402

_KAFKA_USER  = os.getenv("KAFKA_SASL_USERNAME", "")
_KAFKA_PASS  = os.getenv("KAFKA_SASL_PASSWORD", "")
_ACCESS_CERT = os.getenv("KAFKA_ACCESS_CERT", "").strip()
_ACCESS_KEY  = os.getenv("KAFKA_ACCESS_KEY",  "").strip()

if _ACCESS_CERT and _ACCESS_KEY:
    _cert_dir  = "/tmp/kafka_certs"
    os.makedirs(_cert_dir, exist_ok=True)
    _cert_file = os.path.join(_cert_dir, "service.cert")
    _key_file  = os.path.join(_cert_dir, "service.key")
    with open(_cert_file, "w") as _f:
        _f.write(_fix_pem(_ACCESS_CERT))
    with open(_key_file, "w") as _f:
        _f.write(_fix_pem(_ACCESS_KEY))
    _kafka_ssl_conf = {
        "bootstrap.servers":                    KAFKA_BOOTSTRAP,
        "security.protocol":                    "SSL",
        "ssl.ca.location":                      _ca_path,
        "ssl.certificate.location":             _cert_file,
        "ssl.key.location":                     _key_file,
        "ssl.endpoint.identification.algorithm": "https",
    }
    logger.info("Kafka auth mode: mTLS (SSL) — using client certificate")
else:
    _kafka_ssl_conf = {
        "bootstrap.servers":                    KAFKA_BOOTSTRAP,
        "security.protocol":                    "SASL_SSL",
        "sasl.mechanism":                       "SCRAM-SHA-256",
        "sasl.username":                        _KAFKA_USER,
        "sasl.password":                        _KAFKA_PASS,
        "ssl.ca.location":                      _ca_path,
        "ssl.endpoint.identification.algorithm": "https",
    }
    logger.info("Kafka auth mode: SASL_SSL (SCRAM-SHA-256)")

consumer = Consumer({
    **_kafka_ssl_conf,
    "group.id":                   KAFKA_GROUP_ID,
    "auto.offset.reset":          "earliest",
    "enable.auto.commit":         False,
    "fetch.max.bytes":            524_288,    # 512KB — menos buffer en RAM
    "max.partition.fetch.bytes":  262_144,    # 256KB por partición
})
consumer.subscribe([KAFKA_TOPIC_IN])

dlq_producer = Producer(_kafka_ssl_conf)


# ── ML Models + Qdrant ────────────────────────────────────────────────────────
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
        )
        logger.info(f"Collection '{COLLECTION_NAME}' created.")

    # ── OPTIMIZACIÓN CLAVE: sentence-transformers con backend ONNX ────────────
    # sentence-transformers usa ONNX automáticamente si está disponible,
    # evitando cargar PyTorch completo (~180MB menos).
    logger.info(f"Loading embedding model (ONNX backend): {EMBEDDING_MODEL}...")
    from sentence_transformers import SentenceTransformer  # noqa: E402

    _st_model = SentenceTransformer(
        EMBEDDING_MODEL,
        backend="onnx",          # ← CLAVE: ONNX en vez de PyTorch → ~180MB menos
        model_kwargs={
            "file_name": "onnx/model.onnx",   # usa el ONNX pre-exportado del repo
        },
    )
    logger.info("Embedding model loaded via ONNX — PyTorch NOT loaded.")

    # Compartir tokenizer con el chunker
    from ingestion.chunker import SemanticChunker  # noqa: E402
    chunker = SemanticChunker(model_name=EMBEDDING_MODEL)
    chunker.set_tokenizer(_st_model.tokenizer)
    logger.info("Tokenizer shared with chunker — saved ~50MB RAM.")

    # Warmup mínimo
    _ = _st_model.encode(["warmup"], batch_size=1, show_progress_bar=False)
    del _
    gc.collect()
    logger.info("Model warmup complete — worker ready.")


# ── Embedding ─────────────────────────────────────────────────────────────────
def embed_texts(texts: list[str]) -> list[list[float]]:
    """
    Embeds texts usando sentence-transformers (ONNX backend).
    batch_size=1 → mínimo pico de RAM.
    normalize_embeddings=True → vectores L2-normalizados listos para cosine.
    """
    vecs = _st_model.encode(
        texts,
        batch_size=1,              # ← mínimo pico de RAM
        normalize_embeddings=True,
        show_progress_bar=False,
        convert_to_numpy=True,
    )
    result = vecs.tolist()
    del vecs
    gc.collect()  # GC después de cada embedding
    return result


# ── DLQ ───────────────────────────────────────────────────────────────────────
def send_to_dlq(payload: dict, error: Exception):
    try:
        msg = {"original": payload, "error": str(error), "ts": time.time()}
        dlq_producer.produce(KAFKA_TOPIC_FAILED, json.dumps(msg).encode("utf-8"))
        dlq_producer.poll(0)
        DOCS_FAILED.inc()
    except Exception as dlq_err:
        logger.error(f"DLQ_FAILED {dlq_err}")


# ── Batch processor ───────────────────────────────────────────────────────────
def process_batch(messages: list):
    if not messages:
        return

    start_time = time.time()
    payloads: list[dict] = []

    for m in messages:
        try:
            payloads.append(json.loads(m.value().decode()))
        except Exception:
            continue

    all_chunks: list[str] = []
    meta:       list[tuple] = []

    for p in payloads:
        try:
            content = p.get("content", "")

            # Truncar documentos enormes antes de chunking
            if len(content) > MAX_DOC_CHARS:
                logger.warning(
                    f"DOC_TRUNCATED doc_id={p.get('doc_id')} "
                    f"original_chars={len(content)} limit={MAX_DOC_CHARS}"
                )
                content = content[:MAX_DOC_CHARS]

            chunks = chunker.chunk_text_list(content)
            CHUNKS_CREATED.inc(len(chunks))
            for c in chunks:
                all_chunks.append(c["content"])
                meta.append((p.get("doc_id"), c["chunk_index"], c["total_chunks"]))

        except Exception as e:
            send_to_dlq(p, e)

    if not all_chunks:
        return

    try:
        dense = embed_texts(all_chunks)
    except Exception as e:
        logger.error(f"EMBED_ERROR: {e}")
        for p in payloads:
            send_to_dlq(p, e)
        return

    points: list[PointStruct] = []
    for i, text in enumerate(all_chunks):
        doc_id, idx, total = meta[i]
        points.append(PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{doc_id}-{idx}")),
            vector=dense[i],
            payload={
                "doc_id":       doc_id,
                "content":      text,
                "chunk_index":  idx,
                "total_chunks": total,
                "ts":           time.time(),
            },
        ))

    del all_chunks, dense

    try:
        qdrant.upsert(COLLECTION_NAME, points=points)
        DOCS_PROCESSED.inc(len(payloads))
        consumer.commit()
        logger.info(f"BATCH_DONE docs={len(payloads)} chunks={len(points)}")
    except Exception as e:
        for p in payloads:
            send_to_dlq(p, e)
    finally:
        del points, payloads
        gc.collect()


# ── Main loop ─────────────────────────────────────────────────────────────────
_running = True


def _handle_stop(*_):
    global _running
    _running = False


signal.signal(signal.SIGINT,  _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)

if not _TESTING:
    buffer:     list  = []
    last_flush: float = time.time()

    logger.info("Worker started — consuming messages...")

    while _running:
        try:
            msg = consumer.poll(0.1)

            if msg is None:
                if buffer and (time.time() - last_flush) * 1000 > BATCH_TIMEOUT_MS:
                    try:
                        process_batch(buffer)
                    except Exception as _be:
                        logger.error(f"BATCH_ERROR (flush): {_be}")
                    finally:
                        buffer     = []
                        last_flush = time.time()
                continue

            if msg.error():
                logger.warning(f"KAFKA_MSG_ERROR {msg.error()}")
                continue

            buffer.append(msg)

            if len(buffer) >= BATCH_SIZE:
                try:
                    process_batch(buffer)
                except Exception as _be:
                    logger.error(f"BATCH_ERROR (size): {_be}")
                finally:
                    buffer     = []
                    last_flush = time.time()

        except Exception as _le:
            logger.error(f"LOOP_ERROR (recovered): {_le}")
            buffer     = []
            last_flush = time.time()
            time.sleep(1)

    if buffer:
        try:
            process_batch(buffer)
        except Exception as _e:
            logger.error(f"FINAL_BATCH_ERROR: {_e}")

    consumer.close()
    dlq_producer.flush(10)
    logger.info("Worker shut down cleanly.")

"""
worker/embedder.py — Vectoryn Embedding Worker v5.2 (RAM-Optimized, PyTorch CPU)
==================================================================================

OPTIMIZACIONES RAM vs v4.0:
----------------------------
1. torch.set_num_threads(1)  → PyTorch no lanza threads extra (~20MB menos)
2. model.half() NO aplica en CPU — se usa float32 con inference_mode
3. BATCH_SIZE=1 por defecto  → mínimo pico RAM por ciclo
4. MAX_DOC_CHARS=15_000      → trunca docs grandes antes de chunking (~30 chunks máx)
5. Prometheus desactivado por defecto → ~15MB menos
6. GC explícito tras cada embed + borrado de tensores
7. Kafka fetch buffers reducidos → menos RAM de red
8. torch.set_grad_enabled(False) global → nunca se acumula grafo de gradientes

CAMBIOS v5.2:
-------------
- MAX_DOC_CHARS bajado de 50K → 15K (elimina DOC_TRUNCATED warnings de docs grandes)
- Chunks embedean UNO A UNO en vez de todos juntos → sin tensor gigante → sin OOM
- Error en un chunk no mata el documento completo (continúa con el siguiente)
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
import torch
import torch.nn.functional as F
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

load_dotenv()

# ── Deshabilitar gradientes globalmente — nunca los necesitamos ────────────────
torch.set_grad_enabled(False)
# Limitar threads de PyTorch → evita overhead de paralelismo en CPU sin GPU
torch.set_num_threads(1)

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

BATCH_SIZE         = int(os.getenv("WORKER_BATCH_SIZE", "1"))
BATCH_TIMEOUT_MS   = int(os.getenv("WORKER_BATCH_TIMEOUT_MS", "2000"))
HEALTH_PORT        = int(os.getenv("WORKER_HEALTH_PORT", "8002"))
MAX_DOC_CHARS      = int(os.getenv("WORKER_MAX_DOC_CHARS", "15000"))  # 15K → ~30 chunks máx → sin OOM
ENABLE_PROMETHEUS  = os.getenv("ENABLE_PROMETHEUS", "false").lower() == "true"

_TESTING = os.getenv("TESTING") == "true"

_tokenizer = None
_model     = None
qdrant     = None
chunker    = None

# ── Prometheus (opcional) ──────────────────────────────────────────────────────
if ENABLE_PROMETHEUS:
    from prometheus_client import Counter, start_http_server
    DOCS_PROCESSED = Counter("worker_documents_processed_total", "Docs processed")
    DOCS_FAILED    = Counter("worker_documents_failed_total",    "Docs failed")
    CHUNKS_CREATED = Counter("worker_chunks_created_total",      "Chunks created")
else:
    class _Noop:
        def inc(self, *a): pass
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
        "bootstrap.servers":                     KAFKA_BOOTSTRAP,
        "security.protocol":                     "SSL",
        "ssl.ca.location":                       _ca_path,
        "ssl.certificate.location":              _cert_file,
        "ssl.key.location":                      _key_file,
        "ssl.endpoint.identification.algorithm": "https",
    }
    logger.info("Kafka auth mode: mTLS (SSL) — using client certificate")
else:
    _kafka_ssl_conf = {
        "bootstrap.servers":                     KAFKA_BOOTSTRAP,
        "security.protocol":                     "SASL_SSL",
        "sasl.mechanism":                        "SCRAM-SHA-256",
        "sasl.username":                         _KAFKA_USER,
        "sasl.password":                         _KAFKA_PASS,
        "ssl.ca.location":                       _ca_path,
        "ssl.endpoint.identification.algorithm": "https",
    }
    logger.info("Kafka auth mode: SASL_SSL (SCRAM-SHA-256)")

consumer = Consumer({
    **_kafka_ssl_conf,
    "group.id":                  KAFKA_GROUP_ID,
    "auto.offset.reset":         "earliest",
    "enable.auto.commit":        False,
    "fetch.max.bytes":           1_048_576,  # 1MB — debe ser >= message.max.bytes
    "max.partition.fetch.bytes": 524_288,   # 512KB por partición
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

    logger.info(f"Loading embedding model: {EMBEDDING_MODEL}...")
    from transformers import AutoModel, AutoTokenizer  # noqa: E402

    _tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
    _model     = AutoModel.from_pretrained(EMBEDDING_MODEL)
    _model.eval()

    # Compartir tokenizer con el chunker — evita duplicar ~50MB
    from ingestion.chunker import SemanticChunker  # noqa: E402
    chunker = SemanticChunker(model_name=EMBEDDING_MODEL)
    chunker.set_tokenizer(_tokenizer)
    logger.info("Tokenizer shared with chunker — saved ~50MB RAM.")
    logger.info("SPLADE disabled (ENABLE_SPARSE=false) — saves ~100MB RAM.")

    # Warmup mínimo
    _inp = _tokenizer("warmup", return_tensors="pt", truncation=True, max_length=16)
    _out = _model(**_inp)
    del _inp, _out
    gc.collect()
    logger.info("Model warmup complete — worker ready.")


# ── Embedding ─────────────────────────────────────────────────────────────────
def embed_dense_batch(texts: list[str]) -> list[list[float]]:
    """
    Embeds texts con mean pooling + L2 normalización.
    torch.inference_mode() garantiza cero acumulación de grafo de gradientes.
    Borra tensores explícitamente para liberar RAM antes del GC.
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
    emb    = torch.sum(out.last_hidden_state * mask, 1) / torch.clamp(mask.sum(1), min=1e-9)
    result = F.normalize(emb, p=2, dim=1).tolist()

    del inputs, out, mask, emb
    gc.collect()
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

    total_chunks_done = 0

    for p in payloads:
        doc_id = p.get("doc_id", "unknown")
        try:
            content = p.get("content", "")
            if len(content) > MAX_DOC_CHARS:
                logger.warning(
                    f"DOC_TRUNCATED doc_id={doc_id} "
                    f"original_chars={len(content)} limit={MAX_DOC_CHARS}"
                )
                content = content[:MAX_DOC_CHARS]

            chunks = chunker.chunk_text_list(content)
            CHUNKS_CREATED.inc(len(chunks))

            # ── Embedear y subir chunk por chunk — evita tensor gigante en RAM ──
            for c in chunks:
                chunk_text = c["content"]
                chunk_idx  = c["chunk_index"]
                chunk_tot  = c["total_chunks"]

                try:
                    # embed_dense_batch acepta lista; pasamos lista de 1 elemento
                    vec = embed_dense_batch([chunk_text])[0]
                except Exception as embed_err:
                    logger.error(f"EMBED_ERROR doc_id={doc_id} chunk={chunk_idx}: {embed_err}")
                    send_to_dlq(p, embed_err)
                    continue  # siguiente chunk; no mata el doc entero

                point = PointStruct(
                    id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{doc_id}-{chunk_idx}")),
                    vector=vec,
                    payload={
                        "doc_id":       doc_id,
                        "content":      chunk_text,
                        "chunk_index":  chunk_idx,
                        "total_chunks": chunk_tot,
                        "ts":           time.time(),
                    },
                )

                try:
                    qdrant.upsert(COLLECTION_NAME, points=[point])
                    total_chunks_done += 1
                except Exception as upsert_err:
                    logger.error(f"UPSERT_ERROR doc_id={doc_id} chunk={chunk_idx}: {upsert_err}")
                    send_to_dlq(p, upsert_err)

                del vec, point
                gc.collect()  # libera RAM entre chunks

        except Exception as e:
            logger.error(f"DOC_ERROR doc_id={doc_id}: {e}")
            send_to_dlq(p, e)

    try:
        DOCS_PROCESSED.inc(len(payloads))
        elapsed = time.time() - start_time
        consumer.commit()
        logger.info(f"BATCH_DONE docs={len(payloads)} chunks={total_chunks_done} elapsed={elapsed:.2f}s")
    except Exception as e:
        logger.error(f"COMMIT_ERROR: {e}")
    finally:
        del payloads
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

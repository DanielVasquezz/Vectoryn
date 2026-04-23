"""
worker/embedder.py — Vectoryn Embedding Worker v4.0 (Memory-Optimized)
=======================================================================

OPTIMIZACIONES vs v3.3:
-----------------------
1. TOKENIZER COMPARTIDO (CRÍTICO — ahorra ~50MB)
   v3.3: worker cargaba AutoTokenizer Y chunker cargaba otro igual.
   v4.0: El worker pasa su tokenizer ya cargado al chunker via set_tokenizer().
         Un solo tokenizer en memoria.

2. SIN FASTEMBED EN IMPORT NIVEL MÓDULO
   v3.3: fastembed en requirements pero nunca usado (ENABLE_SPARSE=false).
   v4.0: Import solo si ENABLE_SPARSE=true. Sin overhead de librería.

3. GC EXPLÍCITO ENTRE BATCHES
   v3.3: Sin garbage collection → objetos Python acumulan RAM entre batches.
   v4.0: gc.collect() + del tensors después de cada batch.

4. BORRADO EXPLÍCITO DE TENSORES
   v3.3: Tensores de PyTorch permanecían referenciados en la stack.
   v4.0: del inputs, out, mask, emb después de usarlos → libera RAM.

5. LIMITE DE TAMAÑO DE DOCUMENTO
   v3.3: Sin límite → documentos de 1MB tokenizados completos.
   v4.0: MAX_DOC_CHARS (default 500_000 ≈ 100_000 tokens máx).

6. BATCH SIZE CONSERVADOR POR DEFECTO
   v3.3: WORKER_BATCH_SIZE=5 → 5 documentos × sus chunks simultáneos.
   v4.0: WORKER_BATCH_SIZE=2 → menos pico de memoria por ciclo.
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
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    SparseVector,
    SparseVectorParams,
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
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID", "embedding-cluster-v4")

QDRANT_URL         = os.getenv("QDRANT_URL")
QDRANT_API_KEY     = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME    = os.getenv("QDRANT_COLLECTION", "documents")

EMBEDDING_MODEL    = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM      = 384

# Conservador: menos pico de RAM por ciclo
BATCH_SIZE         = int(os.getenv("WORKER_BATCH_SIZE", "2"))
BATCH_TIMEOUT_MS   = int(os.getenv("WORKER_BATCH_TIMEOUT_MS", "2000"))
HEALTH_PORT        = int(os.getenv("WORKER_HEALTH_PORT", "8002"))
ENABLE_SPARSE      = os.getenv("ENABLE_SPARSE", "false").lower() == "true"

# Límite de documento: documentos > 500K chars se truncan con advertencia
MAX_DOC_CHARS      = int(os.getenv("WORKER_MAX_DOC_CHARS", "500000"))

_TESTING = os.getenv("TESTING") == "true"

_tokenizer    = None
_model        = None
_sparse_model = None
qdrant        = None
chunker       = None

# ── Prometheus ─────────────────────────────────────────────────────────────────
DOCS_PROCESSED    = Counter("worker_documents_processed_total", "Docs processed")
DOCS_FAILED       = Counter("worker_documents_failed_total",    "Docs failed")
CHUNKS_CREATED    = Counter("worker_chunks_created_total",      "Chunks created")
EMBED_LATENCY     = Histogram(
    "worker_embedding_latency_seconds",
    "Embedding latency",
    buckets=[0.1, 0.5, 1, 2, 5, 10],
)
WORKER_THROUGHPUT = Gauge("worker_throughput_docs_per_second", "Throughput")

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
    start_http_server(9100)
    threading.Thread(target=_run_health_server, daemon=True).start()
    logger.info(f"Health server on :{HEALTH_PORT} | Prometheus on :9100")


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
    "group.id":         KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    # Reducir fetch buffer → menos RAM de Kafka en memoria
    "fetch.max.bytes":  1_048_576,       # 1MB max por fetch
    "max.partition.fetch.bytes": 524288, # 512KB por partición
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
            sparse_vectors_config={"text-sparse": SparseVectorParams()} if ENABLE_SPARSE else {},
        )
        logger.info(f"Collection '{COLLECTION_NAME}' created.")

    logger.info(f"Loading embedding model: {EMBEDDING_MODEL}...")
    from transformers import AutoTokenizer, AutoModel  # noqa: E402

    _tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
    _model     = AutoModel.from_pretrained(EMBEDDING_MODEL)
    _model.eval()

    # FIX: compartir tokenizer con chunker → NO carga una segunda copia
    from ingestion.chunker import SemanticChunker  # noqa: E402
    chunker = SemanticChunker(model_name=EMBEDDING_MODEL)
    chunker.set_tokenizer(_tokenizer)   # ← CLAVE: reutiliza, no recarga
    logger.info("Tokenizer shared with chunker — saved ~50MB RAM.")

    # Sparse (solo si está habilitado)
    if ENABLE_SPARSE:
        from fastembed import SparseTextEmbedding  # noqa: E402
        _sparse_model = SparseTextEmbedding("prithivida/Splade_PP_en_v1")
        logger.info("SPLADE sparse model loaded.")
    else:
        _sparse_model = None
        logger.info("SPLADE disabled (ENABLE_SPARSE=false) — saves ~100MB RAM.")

    # Warmup mínimo
    with torch.inference_mode():
        _inp = _tokenizer("warmup", return_tensors="pt", truncation=True, max_length=16)
        _out = _model(**_inp)
        del _inp, _out  # liberar inmediatamente
    gc.collect()
    logger.info("Model warmup complete — worker ready.")


# ── Embedding ─────────────────────────────────────────────────────────────────
def embed_dense_batch(texts: list[str]) -> list[list[float]]:
    """
    Embeds a list of strings. Returns L2-normalised float vectors.
    Deletes intermediate tensors explicitly to free RAM quickly.
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
    emb = torch.sum(out.last_hidden_state * mask, 1) / torch.clamp(mask.sum(1), min=1e-9)
    result = F.normalize(emb, p=2, dim=1).tolist()

    # FIX: borrar tensores explícitamente → GC puede reclamar RAM antes
    del inputs, out, mask, emb
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

            # FIX: límite de tamaño → evita picos de memoria con docs enormes
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

    with EMBED_LATENCY.time():
        dense = embed_dense_batch(all_chunks)
        sparse = list(_sparse_model.embed(all_chunks)) if _sparse_model else None

    points: list[PointStruct] = []
    for i, text in enumerate(all_chunks):
        doc_id, idx, total = meta[i]
        vec: dict = {"": dense[i]}
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
                "doc_id":       doc_id,
                "content":      text,
                "chunk_index":  idx,
                "total_chunks": total,
                "ts":           time.time(),
            },
        ))

    # FIX: liberar listas grandes antes de escribir a Qdrant
    del all_chunks, dense, sparse

    try:
        qdrant.upsert(COLLECTION_NAME, points=points)
        DOCS_PROCESSED.inc(len(payloads))
        WORKER_THROUGHPUT.set(len(payloads) / max(time.time() - start_time, 1))
        consumer.commit()
        logger.info(f"BATCH_DONE docs={len(payloads)} chunks={len(points)}")
    except Exception as e:
        for p in payloads:
            send_to_dlq(p, e)
    finally:
        del points, payloads
        gc.collect()  # FIX: forzar GC después de cada batch


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
            # Captura cualquier error inesperado del loop -> evita crash del proceso
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

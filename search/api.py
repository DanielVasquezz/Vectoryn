"""
search/api.py — Vectoryn Search API v3.5
=========================================

CHANGES vs v3.4:
-----------------------
FIX #1 — DOUBLE MODEL LOADING (CRÍTICO — ~90MB desperdiciados)
   v3.4: Cargaba AutoTokenizer+AutoModel Y SentenceTransformer por separado.
         Ambos cargan el mismo modelo all-MiniLM-L6-v2 → ~90MB duplicados.
   v3.5: Carga SOLO SentenceTransformer. Si falla, fallback a AutoModel.
         Esto libera ~90MB de RAM al startup.

FIX #2 — ENABLE_SPARSE DUPLICADO (bug)
   v3.4: ENABLE_SPARSE se asignaba dos veces (línea 63 y línea 88).
         La segunda asignación sobreescribía el valor de la primera.
   v3.5: Una sola asignación limpia.

FIX #3 — ENABLE_RERANKER (OOM)
   v3.4: Cross-Encoder siempre cargaba → ~80MB → OOM cuando se combina
         con el modelo de embeddings.
   v3.5: Reranker respeta ENABLE_RERANKER desde reranker.py (ya corregido).
         Con ambos flags en false: startup usa ~290MB (cabe en 512MB).
"""

import asyncio
import os
import time
import logging
from typing import Optional

import torch
import torch.nn.functional as F
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from groq import AsyncGroq
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field, field_validator, model_validator
from qdrant_client import QdrantClient
from qdrant_client.models import Prefetch, SparseVector, FusionQuery, Fusion
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from transformers import AutoTokenizer, AutoModel

from search.evaluator import get_evaluator
from search.reranker import get_reranker
from search.semantic_cache import get_cache

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "search", "message": "%(message)s"}'
)
logger = logging.getLogger("search")

# ── Config ─────────────────────────────────────────────────────────────────────
QDRANT_URL        = os.getenv("QDRANT_URL", "")
QDRANT_HOST       = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT       = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_API_KEY    = os.getenv("QDRANT_API_KEY", "")
COLLECTION_NAME   = os.getenv("QDRANT_COLLECTION", "documents")
EMBEDDING_MODEL   = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
GROQ_API_KEY      = os.getenv("GROQ_API_KEY")
GROQ_MODEL        = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
ALLOWED_ORIGINS   = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
JAEGER_ENDPOINT   = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
VECTORYN_API_KEY  = os.getenv("VECTORYN_API_KEY")
ENABLE_QUERY_EXP  = os.getenv("ENABLE_QUERY_EXPANSION", "true").lower() == "true"
ENABLE_RAGAS      = os.getenv("ENABLE_RAGAS_EVAL", "true").lower() == "true"
MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "6000"))

# FIX #2: Una sola asignación (v3.4 tenía esta variable duplicada)
ENABLE_SPARSE     = os.getenv("ENABLE_SPARSE", "false").lower() == "true"

DENSE_PREFETCH_LIMIT  = int(os.getenv("DENSE_PREFETCH_LIMIT", "20"))
SPARSE_PREFETCH_LIMIT = int(os.getenv("SPARSE_PREFETCH_LIMIT", "8"))
FUSION_LIMIT          = int(os.getenv("FUSION_LIMIT", "30"))
RERANK_POOL           = int(os.getenv("RERANK_POOL", "30"))
RERANK_RETRY_POOL     = int(os.getenv("RERANK_RETRY_POOL", "40"))
STREAM_CHUNK_SIZE     = int(os.getenv("STREAM_CHUNK_SIZE", "80"))

if not GROQ_API_KEY:
    raise RuntimeError("GROQ_API_KEY is not configured.")

_qdrant_display = QDRANT_URL if QDRANT_URL else f"{QDRANT_HOST}:{QDRANT_PORT}"
logger.info(
    f"Config → Qdrant={_qdrant_display} | Model={GROQ_MODEL} | "
    f"RAGAS={ENABLE_RAGAS} | Expansion={ENABLE_QUERY_EXP} | "
    f"Sparse={ENABLE_SPARSE} | Dense={DENSE_PREFETCH_LIMIT} "
    f"Sparse={SPARSE_PREFETCH_LIMIT} Fusion={FUSION_LIMIT}"
)
logger.info(f"OTEL → Endpoint: {JAEGER_ENDPOINT or 'DISABLED'}")

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="Vectoryn — Search & Retrieval API",
    description="Production RAG pipeline: Hybrid Search + LLM Generation + RAGAS Evaluation",
    version="3.5.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["POST", "GET", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)


@app.middleware("http")
async def validate_api_key(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    excluded = {"/health", "/ready", "/docs", "/redoc", "/openapi.json", "/metrics", "/metrics/ragas"}
    if request.url.path in excluded:
        return await call_next(request)
    if not VECTORYN_API_KEY:
        return await call_next(request)
    if request.headers.get("X-API-Key") != VECTORYN_API_KEY:
        return JSONResponse(status_code=401, content={"detail": "Invalid or missing API Key"})
    return await call_next(request)


Instrumentator().instrument(app).expose(app)


# ── OpenTelemetry ─────────────────────────────────────────────────────────────
def _setup_tracing() -> trace.Tracer:
    resource = Resource.create({"service.name": "search"})
    if not JAEGER_ENDPOINT:
        logger.warning("OTEL_EXPORTER_OTLP_ENDPOINT not configured — tracing disabled")
        return trace.get_tracer("search-service")
    try:
        exporter = OTLPSpanExporter(endpoint=JAEGER_ENDPOINT, insecure=True)
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(
            exporter,
            export_timeout_millis=5000,
            max_export_batch_size=512,
            schedule_delay_millis=5000,
        ))
        trace.set_tracer_provider(provider)
        logger.info(f"OpenTelemetry tracing active → {JAEGER_ENDPOINT}")
        return trace.get_tracer("search-service")
    except Exception as e:
        logger.warning(f"OpenTelemetry setup failed — tracing disabled: {e}")
        return trace.get_tracer("search-service")


tracer = _setup_tracing()
FastAPIInstrumentor.instrument_app(app)


# ── Embedding Model ────────────────────────────────────────────────────────────
# FIX #1: Carga UN SOLO modelo — SentenceTransformer si está disponible,
#         AutoModel como fallback. v3.4 cargaba AMBOS → ~90MB duplicados.
logger.info(f"Loading embedding model: {EMBEDDING_MODEL}...")

_st_model    = None
_tokenizer   = None
_auto_model  = None
_use_st      = False

try:
    from sentence_transformers import SentenceTransformer
    _st_model = SentenceTransformer(EMBEDDING_MODEL)
    _use_st   = True
    logger.info("sentence-transformers loaded — using normalized embeddings. (~90MB)")
except ImportError:
    logger.warning("sentence-transformers not installed — loading AutoModel fallback...")
    _tokenizer  = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
    _auto_model = AutoModel.from_pretrained(EMBEDDING_MODEL)
    _auto_model.eval()
    logger.info("AutoModel loaded as fallback.")

logger.info("Dense model ready.")


# ── SPLADE (opcional) ──────────────────────────────────────────────────────────
_sparse_model = None
if ENABLE_SPARSE:
    try:
        from fastembed import SparseTextEmbedding
        logger.info("Loading SPLADE sparse model...")
        _sparse_model = SparseTextEmbedding(model_name="prithivida/Splade_PP_en_v1")
        logger.info("SPLADE loaded.")
    except Exception as e:
        logger.warning(f"SPLADE load failed — dense-only fallback: {e}")
else:
    logger.info("SPLADE disabled (ENABLE_SPARSE=false) — dense-only search.")


# ── Qdrant ─────────────────────────────────────────────────────────────────────
if QDRANT_URL:
    qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY or None)
    logger.info(f"Connected to Qdrant Cloud: {QDRANT_URL}")
elif QDRANT_API_KEY:
    qdrant = QdrantClient(url=f"https://{QDRANT_HOST}", api_key=QDRANT_API_KEY)
    logger.info(f"Connected to Qdrant (built URL): https://{QDRANT_HOST}")
else:
    qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    logger.info(f"Connected to local Qdrant: {QDRANT_HOST}:{QDRANT_PORT}")

groq_client = AsyncGroq(api_key=GROQ_API_KEY)
reranker    = get_reranker()
cache       = get_cache()
evaluator   = get_evaluator() if ENABLE_RAGAS else None

service_start_time = time.time()
_ragas_history: list[dict] = []
MAX_RAGAS_HISTORY = 100


# ── Embedding Helpers ──────────────────────────────────────────────────────────
def _l2_normalize(vec: list[float]) -> list[float]:
    t = torch.tensor(vec, dtype=torch.float32)
    return F.normalize(t, p=2, dim=0).tolist()


def get_dense_embedding(text: str) -> list[float]:
    # FIX #1: Solo un branch — SentenceTransformer O AutoModel, nunca los dos.
    if _use_st and _st_model is not None:
        return _st_model.encode(text, normalize_embeddings=True).tolist()

    # Fallback AutoModel
    inputs = _tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
    with torch.inference_mode():
        outputs = _auto_model(**inputs)
    mask     = inputs["attention_mask"].unsqueeze(-1).expand(outputs.last_hidden_state.size()).float()
    sum_emb  = torch.sum(outputs.last_hidden_state * mask, 1)
    sum_mask = torch.clamp(mask.sum(1), min=1e-9)
    return _l2_normalize((sum_emb / sum_mask).squeeze().tolist())


def get_sparse_embedding(text: str) -> Optional[SparseVector]:
    if _sparse_model is None:
        return None
    raw = list(_sparse_model.embed([text]))[0]
    return SparseVector(indices=raw.indices.tolist(), values=raw.values.tolist())


def truncate_context(contexts: list[str], max_chars: int = MAX_CONTEXT_CHARS) -> list[str]:
    # Guard: empty list → return empty (caller handles no-docs case)
    if not contexts:
        return []
    result, total = [], 0
    for ctx in contexts:
        if total + len(ctx) > max_chars:
            if not result:
                result.append(ctx[:max_chars])
            break
        result.append(ctx)
        total += len(ctx)
    return result if result else [contexts[0][:max_chars]]


def _build_context_string(hits: list) -> str:
    parts = []
    for hit in hits:
        payload = hit.payload or {}
        content = payload.get("content", "")
        title   = payload.get("title", "")
        url     = payload.get("url", "")
        header  = f"[Source: {title or url}]\n" if (title or url) else ""
        parts.append(f"{header}{content}")
    return "\n\n---\n\n".join(parts) if parts else "No documents indexed yet."


async def expand_query(query: str) -> list[str]:
    with tracer.start_as_current_span("query-expansion") as span:
        try:
            resp = await groq_client.chat.completions.create(
                messages=[{
                    "role": "user",
                    "content": (
                        f"Generate exactly 3 search query variations for RAG retrieval, "
                        f"one per line, no numbering, no markdown. "
                        f"Original query: {query}"
                    ),
                }],
                model="llama-3.1-8b-instant",
                temperature=0.7,
                max_tokens=150,
            )
            lines = [v.strip() for v in resp.choices[0].message.content.split("\n") if v.strip()]
            all_q = list(dict.fromkeys([query] + lines[:3]))
            span.set_attribute("expanded.count", len(all_q))
            return all_q
        except Exception as e:
            logger.warning(f"QUERY_EXPANSION_FAILED error={e} — using original query")
            return [query]


# ── Qdrant Helpers ─────────────────────────────────────────────────────────────
def _collection_exists() -> bool:
    try:
        return COLLECTION_NAME in [c.name for c in qdrant.get_collections().collections]
    except Exception as e:
        logger.error(f"QDRANT_COLLECTIONS_ERROR: {e}")
        return False


def _query_qdrant(q_dense: list[float], q_sparse: Optional[SparseVector]) -> list:
    if q_sparse is not None:
        response = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            prefetch=[
                Prefetch(query=q_dense,  limit=DENSE_PREFETCH_LIMIT),
                Prefetch(query=q_sparse, using="text-sparse", limit=SPARSE_PREFETCH_LIMIT),
            ],
            query=FusionQuery(fusion=Fusion.RRF),
            limit=FUSION_LIMIT,
        )
    else:
        response = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=q_dense,
            limit=FUSION_LIMIT,
        )
    for hit in response.points:
        logger.debug(
            f"QDRANT_SCORE id={hit.id} score={hit.score:.4f} "
            f"content_preview={str(hit.payload.get('content', ''))[:80]!r}"
        )
    return response.points


# ── Request / Response Models ──────────────────────────────────────────────────
class QueryPayload(BaseModel):
    model_config = {"populate_by_name": True}

    query:    str
    top_k:    int  = 3
    evaluate: bool = Field(default=False, alias="enable_eval")

    @model_validator(mode="before")
    @classmethod
    def _coerce_eval_field(cls, data: dict) -> dict:
        if "evaluate" in data and "enable_eval" not in data:
            data["enable_eval"] = data.pop("evaluate")
        return data

    @field_validator("query")
    @classmethod
    def query_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Query cannot be empty.")
        if len(v) > 2000:
            raise ValueError("Query exceeds 2000 characters.")
        return v.strip()

    @field_validator("top_k")
    @classmethod
    def top_k_valid(cls, v: int) -> int:
        if not 1 <= v <= 20:
            raise ValueError("top_k must be between 1 and 20.")
        return v


class EvalPayload(BaseModel):
    question:     str
    answer:       str
    contexts:     list[str]
    ground_truth: Optional[str] = None


# ── Endpoints ──────────────────────────────────────────────────────────────────
@app.get("/health", tags=["Ops"])
async def health_check():
    return {
        "status":            "healthy",
        "service":           "search",
        "version":           "3.5.0",
        "uptime_seconds":    round(time.time() - service_start_time, 2),
        "embedding_model":   EMBEDDING_MODEL,
        "embedding_backend": "sentence-transformers" if _use_st else "AutoModel+L2norm",
        "llm_model":         GROQ_MODEL,
        "ragas_enabled":     ENABLE_RAGAS,
        "query_expansion":   ENABLE_QUERY_EXP,
        "sparse_enabled":    ENABLE_SPARSE and _sparse_model is not None,
        "retrieval": {
            "dense_prefetch":  DENSE_PREFETCH_LIMIT,
            "sparse_prefetch": SPARSE_PREFETCH_LIMIT if ENABLE_SPARSE else 0,
            "fusion_limit":    FUSION_LIMIT,
            "rerank_pool":     RERANK_POOL,
        },
    }


@app.get("/ready", tags=["Ops"])
async def readiness_check():
    try:
        collection_ok = _collection_exists()
        return {
            "status":            "ready" if collection_ok else "degraded",
            "qdrant_connected":  True,
            "collection_exists": collection_ok,
        }
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "not_ready", "reason": str(e)})


@app.get("/metrics/ragas", tags=["Observability"])
async def get_ragas_metrics():
    if not _ragas_history:
        return {"message": "No RAGAS evaluations recorded yet.", "history": []}
    scores = [h["scores"] for h in _ragas_history]
    return {
        "total_evaluations": len(_ragas_history),
        "averages": {
            "faithfulness":      round(sum(s["faithfulness"]      for s in scores) / len(scores), 4),
            "answer_relevancy":  round(sum(s["answer_relevancy"]  for s in scores) / len(scores), 4),
            "context_precision": round(sum(s["context_precision"] for s in scores) / len(scores), 4),
            "context_recall":    round(sum(s["context_recall"]    for s in scores) / len(scores), 4),
        },
        "recent": _ragas_history[-10:],
    }


@app.post("/search", tags=["Pipeline"])
@limiter.limit("30/minute")
async def retrieve_knowledge(
    request:          Request,
    query:            QueryPayload,
    background_tasks: BackgroundTasks,
):
    request_start = time.time()
    logger.info(f"RAG_REQUEST query_length={len(query.query)} top_k={query.top_k} evaluate={query.evaluate}")

    try:
        loop = asyncio.get_event_loop()

        with tracer.start_as_current_span("dense-embedding") as span:
            span.set_attribute("query.length", len(query.query))
            if ENABLE_SPARSE and _sparse_model is not None:
                query_dense, query_sparse = await asyncio.gather(
                    loop.run_in_executor(None, get_dense_embedding, query.query),
                    loop.run_in_executor(None, get_sparse_embedding, query.query),
                )
            else:
                query_dense  = await loop.run_in_executor(None, get_dense_embedding, query.query)
                query_sparse = None

        with tracer.start_as_current_span("cache-lookup") as span:
            cached = cache.lookup(query_dense)
            if cached:
                span.set_attribute("cache.hit", True)
                logger.info(f"RAG_CACHE_HIT query={query.query[:50]}")
                return StreamingResponse((x for x in [cached]), media_type="text/event-stream")
            span.set_attribute("cache.hit", False)

        queries = await expand_query(query.query) if ENABLE_QUERY_EXP else [query.query]
        logger.info(f"RAG_QUERIES count={len(queries)} queries={[q[:40] for q in queries]}")

        with tracer.start_as_current_span("hybrid-retrieval") as span:
            if not _collection_exists():
                logger.warning(f"COLLECTION_NOT_FOUND: '{COLLECTION_NAME}' — no documents indexed yet")
                all_results = []
            else:
                all_results, seen_ids = [], set()
                try:
                    for q_text in queries:
                        if q_text == query.query:
                            q_dense, q_sparse = query_dense, query_sparse
                        else:
                            if ENABLE_SPARSE and _sparse_model is not None:
                                q_dense, q_sparse = await asyncio.gather(
                                    loop.run_in_executor(None, get_dense_embedding, q_text),
                                    loop.run_in_executor(None, get_sparse_embedding, q_text),
                                )
                            else:
                                q_dense  = await loop.run_in_executor(None, get_dense_embedding, q_text)
                                q_sparse = None

                        for hit in _query_qdrant(q_dense, q_sparse):
                            if hit.id not in seen_ids:
                                all_results.append(hit)
                                seen_ids.add(hit.id)

                except Exception as qdrant_err:
                    logger.error(f"QDRANT_QUERY_ERROR: {qdrant_err}")
                    all_results = []

            span.set_attribute("candidates.raw", len(all_results))
            logger.info(f"RAG_CANDIDATES raw={len(all_results)}")

        with tracer.start_as_current_span("reranking") as span:
            final_docs = reranker.rerank(
                query=query.query,
                candidates=all_results[:RERANK_POOL],
                top_k=query.top_k,
            )
            span.set_attribute("candidates.reranked", len(final_docs))

        raw_contexts = [hit.payload.get("content", "") for hit in final_docs]
        contexts     = truncate_context(raw_contexts)

        # Guard: no documents found → return friendly message instead of crashing
        if not contexts:
            async def _no_docs_stream():
                msg = (
                    "No hay documentos indexados todavía. "
                    "Ve a la pestaña Upload, sube un documento y espera unos segundos antes de preguntar."
                )
                yield msg
            return StreamingResponse(_no_docs_stream(), media_type="text/event-stream")

        context_str  = _build_context_string(final_docs[:len(contexts)])

        retrieval_ms = round((time.time() - request_start) * 1000, 2)
        logger.info(f"RAG_RETRIEVAL_DONE docs={len(contexts)} latency_ms={retrieval_ms}")

        max_retries      = 1
        current_retry    = 0
        final_answer     = ""
        is_faithful_pass = False

        while current_retry <= max_retries:
            with tracer.start_as_current_span(f"agentic-turn-{current_retry}") as span:
                span.set_attribute("agent.retry_count", current_retry)

                if current_retry > 0:
                    logger.info(f"RAG_RETRYING attempt={current_retry}")
                    extended_docs = reranker.rerank(
                        query=query.query,
                        candidates=all_results[:RERANK_RETRY_POOL],
                        top_k=query.top_k + 3,
                    )
                    contexts    = truncate_context([h.payload.get("content", "") for h in extended_docs])
                    context_str = _build_context_string(extended_docs[:len(contexts)])

                grounding_note = (
                    "\n\nIMPORTANT: Your previous response was rejected for insufficient "
                    "grounding. Be strictly factual, cite only what appears in the context."
                    if current_retry > 0 else ""
                )

                llm_response = await groq_client.chat.completions.create(
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a precise document analysis assistant. "
                                "Answer only from the provided context. "
                                "If the context is insufficient, say so explicitly."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"CONTEXT:\n{context_str}\n\n"
                                f"QUESTION: {query.query}"
                                f"{grounding_note}"
                            ),
                        },
                    ],
                    model=GROQ_MODEL,
                    temperature=0.0,
                    max_tokens=800,
                )
                candidate_answer = llm_response.choices[0].message.content

                if ENABLE_RAGAS and evaluator:
                    logger.info(f"RAG_FAITHFULNESS_CHECK attempt={current_retry}")
                    is_faithful = await evaluator.fast_check_faithfulness(candidate_answer, contexts)
                    span.set_attribute("agent.faithful", is_faithful)
                    if is_faithful:
                        logger.info(f"RAG_AGENT_PASS attempt={current_retry}")
                        final_answer     = candidate_answer
                        is_faithful_pass = True
                        break
                    else:
                        logger.warning(f"RAG_HALLUCINATION_DETECTED attempt={current_retry}")
                        current_retry += 1
                else:
                    final_answer = candidate_answer
                    break

        if not final_answer:
            final_answer = "I could not generate a verifiable answer from the available documents."

        async def event_stream():
            try:
                for i in range(0, len(final_answer), STREAM_CHUNK_SIZE):
                    await asyncio.sleep(0.005)
                    yield final_answer[i: i + STREAM_CHUNK_SIZE]

                if is_faithful_pass or not (ENABLE_RAGAS and evaluator):
                    cache.store(query_dense, final_answer)

                total_ms = round((time.time() - request_start) * 1000, 2)
                logger.info(
                    f"RAG_COMPLETE total_ms={total_ms} retries={current_retry} "
                    f"faithful={is_faithful_pass} context_docs={len(contexts)}"
                )
            except Exception as stream_err:
                logger.error(f"RAG_STREAM_ERROR error={stream_err}")
                yield f"\n\n[Stream error: {stream_err}]"

        if query.evaluate and evaluator:
            captured_answer = final_answer

            async def run_ragas_eval():
                scores = await evaluator.evaluate_and_record(
                    question=query.query,
                    answer=captured_answer,
                    contexts=contexts,
                )
                if len(_ragas_history) >= MAX_RAGAS_HISTORY:
                    _ragas_history.pop(0)
                _ragas_history.append({
                    "timestamp": time.time(),
                    "question":  query.query[:100],
                    "scores":    scores.to_dict(),
                })

            background_tasks.add_task(run_ragas_eval)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    except Exception as e:
        logger.error(f"RAG_ERROR error={e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/evaluate", tags=["Observability"])
async def evaluate_rag_quality(payload: EvalPayload):
    if not evaluator:
        raise HTTPException(
            status_code=503,
            detail="RAGAS evaluation disabled. Set ENABLE_RAGAS_EVAL=true to enable."
        )
    logger.info(f"EVAL_REQUEST question_length={len(payload.question)}")
    scores = await evaluator.evaluate(
        question=payload.question,
        answer=payload.answer,
        contexts=payload.contexts,
        ground_truth=payload.ground_truth,
    )
    evaluator.record_metrics(scores)
    if len(_ragas_history) >= MAX_RAGAS_HISTORY:
        _ragas_history.pop(0)
    _ragas_history.append({
        "timestamp": time.time(),
        "question":  payload.question[:100],
        "scores":    scores.to_dict(),
    })
    return {
        "scores":        scores.to_dict(),
        "overall_score": scores.overall_score,
        "is_acceptable": scores.is_acceptable,
        "thresholds": {
            "faithfulness_min":     0.70,
            "answer_relevancy_min": 0.65,
            "overall_min":          0.70,
        },
    }

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import re
from dotenv import load_dotenv

load_dotenv()
from pydantic import BaseModel, field_validator
from confluent_kafka import Producer
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from prometheus_fastapi_instrumentator import Instrumentator
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
import json
import uuid
import os
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "ingestion", "message": "%(message)s"}'
)
logger = logging.getLogger("ingestion")

# ── Config ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALLOWED_ORIGINS   = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:5500").split(",")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC_INGEST", "raw-documents")
JAEGER_ENDPOINT   = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
VECTORYN_API_KEY  = os.getenv("VECTORYN_API_KEY")

logger.info(f"Config → Kafka: {KAFKA_BOOTSTRAP} | Topic: {KAFKA_TOPIC}")
logger.info(f"CORS → Allowed origins: {ALLOWED_ORIGINS}")
logger.info(f"OTEL → Endpoint: {JAEGER_ENDPOINT or 'DISABLED'}")

# ── Rate Limiter ──────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="Vectoryn — Ingestion Service",
    description="Real-time document ingestion pipeline via Kafka",
    version="2.0.0"
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)

@app.middleware("http")
async def validate_api_key(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    path = request.url.path
    if path in ["/health", "/ready", "/docs", "/openapi.json", "/metrics"]:
        return await call_next(request)
    if not VECTORYN_API_KEY:
        return await call_next(request)
    api_key = request.headers.get("X-API-Key")
    if api_key != VECTORYN_API_KEY:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid or missing API Key (X-API-Key header required)"}
        )
    return await call_next(request)

# ── Prometheus ────────────────────────────────────────────────
Instrumentator().instrument(app).expose(app)

# ── OpenTelemetry — ROBUST SETUP (Won't crash if Jaeger is down) ─────
# CRITICAL FIX: The original setup threw an exception if Jaeger
# was unavailable, breaking the service startup.
# Now: If JAEGER_ENDPOINT is empty or Jaeger doesn't respond,
# we use a NoOpTracer that does nothing — no errors, no spam.
def _setup_tracing() -> trace.Tracer:
    service_name = os.getenv("OTEL_SERVICE_NAME", "ingestion")
    resource = Resource.create({"service.name": service_name})

    if not JAEGER_ENDPOINT:
        logger.warning("OTEL_EXPORTER_OTLP_ENDPOINT not configured — tracing disabled")
        return trace.get_tracer("ingestion-service")

    try:
        exporter = OTLPSpanExporter(endpoint=JAEGER_ENDPOINT, insecure=True)
        provider = TracerProvider(resource=resource)
        # FIX: Timeout in BatchSpanProcessor to prevent blocking if Jaeger goes down
        processor = BatchSpanProcessor(
            exporter,
            export_timeout_millis=5000,   # 5s max — does not block requests
            max_export_batch_size=512,
            schedule_delay_millis=5000,
        )
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        logger.info(f"OpenTelemetry tracing active → {JAEGER_ENDPOINT}")
        return trace.get_tracer("ingestion-service")
    except Exception as e:
        logger.warning(f"OpenTelemetry setup failed — tracing disabled: {e}")
        return trace.get_tracer("ingestion-service")

tracer = _setup_tracing()
FastAPIInstrumentor.instrument_app(app)

# ── Kafka Producer ────────────────────────────────────────────
KAFKA_SASL_USER = os.getenv('KAFKA_SASL_USERNAME', '')
KAFKA_SASL_PASS = os.getenv('KAFKA_SASL_PASSWORD', '')

kafka_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'client.id': 'vectoryn-ingestion-v2',
    'acks': 'all',
    'retries': 3,
    'delivery.timeout.ms': 10000,
}

# Cloud Kafka (Aiven / Upstash) uses SASL_SSL — enable if credentials exist
if KAFKA_SASL_USER and KAFKA_SASL_PASS:
    kafka_conf.update({
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': KAFKA_SASL_USER,
        'sasl.password': KAFKA_SASL_PASS,
        'ssl.ca.location': '/usr/local/share/ca-certificates/aiven-ca.crt',
    })
    logger.info('Kafka SASL/SSL enabled (cloud mode — Aiven/Upstash)')
producer = Producer(kafka_conf)
service_start_time = time.time()

# ── PII Shield ────────────────────────────────────────────────
class PIIShield:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.patterns = {
            "EMAIL": r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
            "PHONE": r"\b\+?\d{1,3}[-.\s]?\(?\d{1,3}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b",
            "CREDIT_CARD": r"\b(?:\d[ -]*?){13,16}\b",
        }

    def mask(self, text: str) -> str:
        if not self.enabled:
            return text
        masked_text = text
        for label, pattern in self.patterns.items():
            masked_text = re.sub(pattern, f"[{label}_HIDDEN]", masked_text)
        if masked_text != text:
            logger.info("PII_SHIELD_TRIGGERED: Sensitive data detected and masked.")
        return masked_text

pii_shield = PIIShield(enabled=os.getenv("ENABLE_PII_SHIELD", "true").lower() == "true")


class DocumentPayload(BaseModel):
    id: str = None
    content: str

    @field_validator("content")
    @classmethod
    def content_must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Document content cannot be empty.")
        if len(v) > 100_000:
            raise ValueError("Document exceeds the limit of 100,000 characters.")
        return v.strip()


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"KAFKA_DELIVERY_FAILURE topic={msg.topic()} error={err}")
    else:
        logger.info(f"KAFKA_DELIVERY_SUCCESS topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


@app.get("/health", tags=["Ops"])
async def health_check():
    return {
        "status": "healthy",
        "service": "ingestion",
        "version": "2.0.0",
        "uptime_seconds": round(time.time() - service_start_time, 2)
    }


@app.get("/ready", tags=["Ops"])
async def readiness_check():
    try:
        metadata = producer.list_topics(timeout=3)
        return {"status": "ready", "kafka_topics": len(metadata.topics)}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "not_ready", "reason": str(e)})


@app.post("/ingest", tags=["Pipeline"])
@limiter.limit("100/minute")
async def ingest_document(request: Request, doc: DocumentPayload):
    if not doc.id:
        doc.id = str(uuid.uuid4())

    with tracer.start_as_current_span("kafka-produce") as span:
        span.set_attribute("doc.id", doc.id)
        span.set_attribute("doc.length", len(doc.content))

        try:
            masked_content = pii_shield.mask(doc.content)
            json_message = json.dumps({"doc_id": doc.id, "content": masked_content})

            producer.produce(
                topic=KAFKA_TOPIC,
                key=doc.id.encode("utf-8"),
                value=json_message.encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)

            logger.info(f"INGEST_ACCEPTED doc_id={doc.id} content_length={len(masked_content)}")

            return {
                "status": "accepted",
                "message": "Document processed (Privacy Shield active) and sent to Kafka.",
                "doc_id": doc.id,
                "anonymized": masked_content != doc.content
            }
        except Exception as e:
            logger.error(f"INGEST_ERROR doc_id={doc.id} error={e}")
            span.record_exception(e)
            raise HTTPException(status_code=500, detail=str(e))

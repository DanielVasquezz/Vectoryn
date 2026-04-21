#!/usr/bin/env bash
# seed_demo.sh — Pre-carga documentos técnicos de demo en Vectoryn
# Uso: ./seed_demo.sh [API_URL] [API_KEY]
#
# Por defecto usa localhost. En Render: ./seed_demo.sh https://tu-app.onrender.com your_secret_key_here

set -euo pipefail

API_URL="${1:-http://localhost:8000}"
API_KEY="${2:-your_secret_key_here}"
INGESTION_URL="${API_URL}/ingest"

echo "🚀 Vectoryn Demo Seeder"
echo "   URL: $API_URL"
echo ""

# Función para ingestar un documento
ingest() {
  local title="$1"
  local text="$2"
  local source="$3"

  echo -n "  📄 Ingesting: $title ... "
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$INGESTION_URL" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $API_KEY" \
    -d "{\"title\": \"$title\", \"content\": \"$text\", \"source\": \"$source\"}")
  if [ "$STATUS" = "200" ] || [ "$STATUS" = "201" ] || [ "$STATUS" = "202" ]; then
    echo "✅"
  else
    echo "⚠️  HTTP $STATUS (puede ser normal si ya existe)"
  fi
}

echo "📚 Cargando documentos técnicos de demo..."
echo ""

ingest \
  "Attention Is All You Need — Transformer Architecture" \
  "The Transformer model introduced in 2017 relies entirely on attention mechanisms, dispensing with recurrence and convolutions. The encoder maps input sequences to continuous representations. The decoder generates output sequences one element at a time. Multi-head attention allows the model to jointly attend to information from different representation subspaces at different positions. The scaled dot-product attention computes a weighted sum of values, where the weight assigned to each value is computed by a compatibility function of the query with the corresponding key." \
  "arxiv:1706.03762"

ingest \
  "RAG — Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks" \
  "RAG combines parametric memory (pre-trained language model) with non-parametric memory (dense vector index of Wikipedia). The retriever uses a bi-encoder DPR model to find relevant passages. The generator is a seq2seq model conditioned on the question and retrieved passages. RAG-Token allows the model to retrieve different documents for each token generated, while RAG-Sequence uses the same document for the full sequence. This hybrid architecture outperforms purely parametric models on open-domain QA benchmarks." \
  "arxiv:2005.11401"

ingest \
  "SPLADE — Sparse Lexical and Expansion Model for First Stage Ranking" \
  "SPLADE learns sparse representations in the BERT vocabulary space. Each document and query is represented as a sparse vector over the vocabulary, enabling efficient inverted index retrieval. Unlike BM25, SPLADE performs query and document expansion through the MLM head, assigning weights to terms that do not appear in the original text. SPLADE-v2 achieves state-of-the-art performance on BEIR benchmark while maintaining sparse retrieval efficiency. The model is trained with FLOPS regularization to control sparsity." \
  "arxiv:2109.10086"

ingest \
  "Kubernetes — Pod Scheduling and Resource Management" \
  "Kubernetes schedules Pods to Nodes based on resource requests, node affinity, taints and tolerations. The kube-scheduler selects an optimal node using a two-phase process: filtering (removes nodes that do not satisfy constraints) and scoring (ranks remaining nodes). Resource requests define the minimum resources a container needs; limits define the maximum. QoS classes (Guaranteed, Burstable, BestEffort) determine eviction priority under memory pressure. Horizontal Pod Autoscaler scales deployments based on CPU, memory, or custom metrics via the Metrics API." \
  "kubernetes.io/docs"

ingest \
  "Apache Kafka — Distributed Event Streaming Architecture" \
  "Kafka is a distributed log where producers append records to topics partitioned across brokers. Consumers read from partitions at their own pace using offsets stored in __consumer_offsets. Consumer groups enable parallel processing: each partition is assigned to exactly one consumer in the group. Replication factor ensures durability; ISR (In-Sync Replicas) tracks replicas that are caught up to the leader. Kafka Streams and ksqlDB enable stateful stream processing directly on the broker. Retention policies control disk usage: time-based or size-based log compaction." \
  "kafka.apache.org/docs"

ingest \
  "Vector Databases — HNSW Index for Approximate Nearest Neighbor Search" \
  "HNSW (Hierarchical Navigable Small World) builds a multi-layer graph where upper layers contain long-range connections and lower layers have fine-grained local connections. Search starts at the top layer and greedily descends to the query's neighborhood. Construction parameters ef_construction and M control index quality and memory usage. Query parameter ef controls the search beam width, trading recall for speed. Qdrant, Weaviate, and Pinecone all implement HNSW. For hybrid search, HNSW handles dense vectors while sparse vectors use standard inverted indexes; RRF (Reciprocal Rank Fusion) merges both result lists." \
  "qdrant.tech/docs"

ingest \
  "Prometheus — Metrics Collection and Alerting" \
  "Prometheus scrapes metrics from HTTP /metrics endpoints in a pull-based model. Time series are identified by metric name and key-value label pairs stored in a custom TSDB. PromQL queries support instant vectors, range vectors, and aggregations. Recording rules pre-compute expensive queries. Alertmanager deduplicates, groups, and routes alerts to receivers (Slack, PagerDuty, email). The remote_write protocol allows long-term storage in Thanos or Cortex. Exemplars link metrics to distributed traces via trace IDs for unified observability." \
  "prometheus.io/docs"

ingest \
  "Cross-Encoder Reranking for Information Retrieval" \
  "Bi-encoders (dense retrievers) produce independent query and document embeddings enabling fast ANN search but miss fine-grained interactions. Cross-encoders process query and document together through a transformer, producing a relevance score with full attention between both texts. The two-stage pipeline uses a bi-encoder for candidate retrieval (top-100) then a cross-encoder for reranking (top-10). MiniLM-L6-v2 is a distilled cross-encoder achieving 90% of large model performance at 6x lower latency. RAGAS metrics (faithfulness, answer relevancy, context precision, context recall) evaluate end-to-end RAG quality." \
  "sbert.net/docs"

ingest \
  "Redis — Semantic Cache for LLM Applications" \
  "Semantic caching stores LLM responses indexed by query embeddings in Redis. On a new query, cosine similarity search finds cached responses within a configurable threshold (e.g. 0.92). Cache hits skip the LLM call entirely, reducing latency from ~2s to ~10ms and cutting API costs. Redis Stack provides the VSS (Vector Similarity Search) module with HNSW or FLAT index types. TTL policies evict stale entries. The cache key combines the embedding vector and optional metadata filters (user_id, document_collection) to scope results correctly." \
  "redis.io/docs"

ingest \
  "Distributed Tracing with OpenTelemetry and Jaeger" \
  "OpenTelemetry (OTel) provides vendor-neutral APIs and SDKs for traces, metrics, and logs. A trace is a DAG of spans representing a request's journey. Each span carries operation name, timestamps, attributes, events, and a status. Context propagation via W3C TraceContext headers links spans across services. The OTel Collector receives, processes, and exports telemetry to backends like Jaeger. Jaeger's adaptive sampling reduces overhead in high-throughput systems. Service dependency graphs and flamegraph views help identify bottlenecks. Correlating traces with Prometheus metrics via exemplars enables RCA (Root Cause Analysis) in under 5 minutes." \
  "opentelemetry.io/docs"

echo ""
echo "✅ Seed completado. Tu demo está lista."
echo ""
echo "Prueba una query:"
echo "  curl -s -X POST $API_URL/search \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -H 'X-API-Key: $API_KEY' \\"
echo "    -d '{\"query\": \"how does hybrid search work with SPLADE?\", \"top_k\": 3}'"

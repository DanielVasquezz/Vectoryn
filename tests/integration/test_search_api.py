"""
tests/integration/test_search_api.py — Fixed for search/api.py v3

FIXES vs original:
------------------
1. Patched `search.api.get_query_embedding` — function was renamed to
   `get_dense_embedding` in v3. Original test patches a name that doesn't
   exist → mock silently does nothing → test passes for the wrong reasons.

2. Patched `search.api.qdrant.search` — v3 uses `qdrant.query_points`,
   not `qdrant.search`. Same problem: silent mock failure.

3. `groq_client` is now `AsyncGroq` — requires `AsyncMock` not `MagicMock`.
   Using `MagicMock` on an async method raises `TypeError: object is not
   awaitable` at runtime.

4. The agentic loop calls `groq_client.chat.completions.create` and awaits
   it — the mock must return a proper awaitable with `.choices[0].message.content`.

5. Added test for the `/evaluate` endpoint (RAGAS) — completely missing
   in the original test suite despite being a core feature.
"""

import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

# ── Mock all heavy dependencies before importing the app ──────────────────────
# Order matters: patch modules that are imported at module-level in api.py
_torch_mock = MagicMock()
_torch_mock.inference_mode.return_value.__enter__ = lambda s: s
_torch_mock.inference_mode.return_value.__exit__ = MagicMock(return_value=False)

import sys
sys.modules["torch"]         = _torch_mock
sys.modules["fastembed"]     = MagicMock()
sys.modules["transformers"] = MagicMock()

# Prometheus must be mocked before any import creates Gauge/Counter at module level
import prometheus_client
from unittest.mock import patch as _patch

with _patch("transformers.AutoTokenizer"), \
     _patch("transformers.AutoModel"), \
     _patch("qdrant_client.QdrantClient"), \
     _patch("groq.AsyncGroq"), \
     _patch("groq.Groq"), \
     _patch("search.reranker.get_reranker"), \
     _patch("search.semantic_cache.get_cache"), \
     _patch("search.evaluator.get_evaluator"):
    from search.api import app

from fastapi.testclient import TestClient

client = TestClient(app)


class TestHealthEndpoints:
    def test_health_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_includes_ragas_status(self):
        """v3 health check exposes ragas_enabled and query_expansion flags."""
        response = client.get("/health")
        data = response.json()
        assert "ragas_enabled" in data
        assert "query_expansion" in data
        assert "llm_model" in data

    def test_ready_endpoint(self):
        """
        /ready checks Qdrant connectivity.
        We mock qdrant so it doesn't need a real server.
        """
        with patch("search.api.qdrant") as mock_qdrant:
            mock_collection = MagicMock()
            mock_collection.name = "documents"
            mock_qdrant.get_collections.return_value.collections = [mock_collection]

            response = client.get("/ready")
            assert response.status_code == 200


class TestSearchEndpoint:
    def test_empty_query_returns_422(self):
        """Pydantic validation: empty query string must be rejected."""
        response = client.post("/search", json={"query": ""})
        assert response.status_code == 422
        assert "empty" in response.text.lower()

    def test_query_too_long_returns_422(self):
        """Queries > 2000 chars should be rejected at the API boundary."""
        response = client.post("/search", json={"query": "x" * 2001})
        assert response.status_code == 422

    def test_top_k_out_of_range_returns_422(self):
        """top_k must be 1-20."""
        response = client.post("/search", json={"query": "test", "top_k": 0})
        assert response.status_code == 422

        response = client.post("/search", json={"query": "test", "top_k": 21})
        assert response.status_code == 422

    def test_valid_search_returns_200(self):
        """
        Full happy-path mock: cache miss → retrieval → rerank → LLM → 200.

        KEY FIX: patch `get_dense_embedding` not `get_query_embedding`
        KEY FIX: patch `qdrant.query_points` not `qdrant.search`
        KEY FIX: groq mock uses AsyncMock since groq_client is AsyncGroq
        """
        # Mock a Qdrant hit
        mock_hit = MagicMock()
        mock_hit.id = "test-uuid-1"
        mock_hit.payload = {"content": "Vectoryn is an enterprise RAG system."}

        # Mock reranker output (same hit after reranking)
        mock_reranker = MagicMock()
        mock_reranker.rerank.return_value = [mock_hit]

        # Mock cache: miss on lookup, success on store
        mock_cache = MagicMock()
        mock_cache.lookup.return_value = None  # Cache miss → go through pipeline

        # Mock LLM response (AsyncMock because groq_client is AsyncGroq)
        mock_llm_response = MagicMock()
        mock_llm_response.choices = [MagicMock()]
        mock_llm_response.choices[0].message.content = "Vectoryn uses Redpanda as message broker."

        with patch("search.api.get_dense_embedding", return_value=[0.1] * 384), \
             patch("search.api.get_sparse_embedding", return_value=MagicMock()), \
             patch("search.api.qdrant") as mock_qdrant, \
             patch("search.api.reranker", mock_reranker), \
             patch("search.api.cache", mock_cache), \
             patch("search.api.evaluator", None), \
             patch("search.api.ENABLE_QUERY_EXP", False), \
             patch("search.api.groq_client") as mock_groq:

            # query_points is the v3 method (not .search)
            mock_response = MagicMock()
            mock_response.points = [mock_hit]
            mock_qdrant.query_points.return_value = mock_response

            # AsyncGroq requires AsyncMock
            mock_groq.chat.completions.create = AsyncMock(
                return_value=mock_llm_response
            )

            response = client.post(
                "/search",
                json={"query": "What is Vectoryn?", "top_k": 3},
            )

        assert response.status_code == 200
        assert len(response.text) > 0

    def test_cache_hit_returns_immediately(self):
        """When semantic cache hits, the pipeline skips LLM entirely."""
        cached_response = "This answer was cached from a previous identical query."

        mock_cache = MagicMock()
        mock_cache.lookup.return_value = cached_response

        with patch("search.api.get_dense_embedding", return_value=[0.1] * 384), \
             patch("search.api.get_sparse_embedding", return_value=MagicMock()), \
             patch("search.api.cache", mock_cache), \
             patch("search.api.groq_client") as mock_groq:

            # groq should NEVER be called on a cache hit
            mock_groq.chat.completions.create = AsyncMock()

            response = client.post(
                "/search",
                json={"query": "cached question", "top_k": 3},
            )

        assert response.status_code == 200
        assert cached_response in response.text
        # LLM was not called
        mock_groq.chat.completions.create.assert_not_called()


class TestEvaluateEndpoint:
    """
    Tests for the /evaluate (RAGAS) endpoint.
    This was completely missing in the original test suite.
    """

    def test_evaluate_disabled_returns_503(self):
        """If RAGAS is disabled, /evaluate should return 503."""
        with patch("search.api.evaluator", None):
            response = client.post(
                "/evaluate",
                json={
                    "question": "What is Vectoryn?",
                    "answer": "Vectoryn is a RAG system.",
                    "contexts": ["Vectoryn is an enterprise RAG infrastructure."],
                },
            )
        assert response.status_code == 503

    def test_evaluate_returns_four_scores(self):
        """
        When evaluator is active, /evaluate must return all 4 RAGAS scores.
        """
        from search.evaluator import RAGASScores

        mock_scores = RAGASScores(
            faithfulness=0.92,
            answer_relevancy=0.88,
            context_precision=0.79,
            context_recall=0.74,
        )

        mock_evaluator = MagicMock()
        mock_evaluator.evaluate = AsyncMock(return_value=mock_scores)
        mock_evaluator.record_metrics = MagicMock()

        with patch("search.api.evaluator", mock_evaluator):
            response = client.post(
                "/evaluate",
                json={
                    "question": "What message broker does Vectoryn use?",
                    "answer": "Vectoryn uses Redpanda, a Kafka-compatible broker.",
                    "contexts": [
                        "Vectoryn uses Redpanda as its message broker. "
                        "Redpanda is Kafka-compatible and written in C++."
                    ],
                },
            )

        assert response.status_code == 200
        data = response.json()

        # All 4 RAGAS metrics must be present
        scores = data["scores"]
        assert "faithfulness" in scores
        assert "answer_relevancy" in scores
        assert "context_precision" in scores
        assert "context_recall" in scores

        # overall_score and is_acceptable flags
        assert "overall_score" in data
        assert "is_acceptable" in data

    def test_ragas_metrics_endpoint(self):
        """/metrics/ragas returns history or empty message."""
        response = client.get("/metrics/ragas")
        assert response.status_code == 200
        data = response.json()
        # Either has history or the empty state message
        assert "history" in data or "total_evaluations" in data

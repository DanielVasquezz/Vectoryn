"""
search/reranker.py — Vectoryn Reranker v2.0
============================================

CHANGES vs v1.0:
-----------------------
FIX #1 — OOM EN RENDER FREE TIER
   v1.0: CrossEncoder siempre cargaba al startup → ~80MB extra → OOM.
   v2.0: Controlado por ENABLE_RERANKER=true|false (default: false).
         Cuando está deshabilitado, usa los scores de Qdrant directamente
         como ranking. No se pierde funcionalidad core, solo precisión extra.

MEMORY BUDGET (free tier 512MB):
   Con ENABLE_RERANKER=false:  CrossEncoder no carga → ahorra ~80MB.
   Con ENABLE_RERANKER=true:   Requiere al menos 1GB RAM (Render Starter+).
"""

import logging
import os

logger = logging.getLogger("search")

ENABLE_RERANKER    = os.getenv("ENABLE_RERANKER", "false").lower() == "true"
RERANKER_MODEL     = os.getenv("RERANKER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")


class Reranker:
    """
    Two-stage retrieval reranker.

    Stage 1 (Qdrant): Bi-Encoder fast recall — top-N candidates.
    Stage 2 (this):   Cross-Encoder precise reranking — top-K final.

    When ENABLE_RERANKER=false, falls back to Qdrant score ordering.
    This preserves the pipeline interface without loading the model.
    """

    def __init__(self):
        self._model = None

        if ENABLE_RERANKER:
            try:
                from sentence_transformers import CrossEncoder
                logger.info(f"Loading Cross-Encoder Reranker: {RERANKER_MODEL}...")
                self._model = CrossEncoder(RERANKER_MODEL)
                logger.info("Reranker loaded and ready.")
            except Exception as e:
                logger.warning(f"Reranker load failed — falling back to score ordering: {e}")
                self._model = None
        else:
            logger.info(
                "Reranker disabled (ENABLE_RERANKER=false) — "
                "using Qdrant score ordering. Saves ~80MB RAM."
            )

    def rerank(self, query: str, candidates: list, top_k: int = 3) -> list:
        """
        Reranks candidates by relevance.

        If model is loaded: uses Cross-Encoder scoring (precise).
        If disabled/failed: uses Qdrant scores as-is (fast, good enough).

        Args:
            query:      The user query string.
            candidates: List of Qdrant ScoredPoint objects.
            top_k:      Number of results to return.

        Returns:
            List of top_k ScoredPoint objects ordered by relevance.
        """
        if not candidates:
            return []

        if self._model is not None:
            pairs = [[query, c.payload.get("content", "")] for c in candidates]
            scores = self._model.predict(pairs)

            scored = sorted(
                zip(scores, candidates),
                key=lambda x: x[0],
                reverse=True,
            )
            return [c for _, c in scored[:top_k]]

        # Fallback: Qdrant already returns by score desc — just slice
        return candidates[:top_k]


# ── Singleton ─────────────────────────────────────────────────
_reranker_instance = None

def get_reranker() -> Reranker:
    global _reranker_instance
    if _reranker_instance is None:
        _reranker_instance = Reranker()
    return _reranker_instance

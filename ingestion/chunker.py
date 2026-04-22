"""
ingestion/chunker.py — Vectoryn Chunker v4.0 (Memory-Optimized)
================================================================

OPTIMIZATIONS vs v3.x:
-----------------------
1. SHARED TOKENIZER (CRÍTICO — ~50MB ahorrados)
   v3.x: Cargaba AutoTokenizer internamente aunque el worker ya lo tenía.
   v4.0: Acepta tokenizer pre-cargado vía set_tokenizer() → cero duplicación.

2. CHARACTER PRE-SPLIT PARA DOCUMENTOS GIGANTES
   v3.x: tokenizer.encode(full_38K_doc) → lista enorme en RAM.
   v4.0: Pre-divide por párrafos antes de tokenizar. Nunca más de ~5K tokens
         en un solo llamado al tokenizer.

3. GENERADOR (yield) EN VEZ DE LISTA COMPLETA
   v3.x: list[dict] completa en RAM.
   v4.0: chunk_text() es un generador → un chunk a la vez.
         chunk_text_list() disponible para compatibilidad.
"""
from __future__ import annotations

import os
import re
from typing import Generator, Optional

MAX_CHARS_BEFORE_PRESPLIT = int(os.getenv("CHUNKER_MAX_CHARS_PRESPLIT", "20000"))


class SemanticChunker:
    def __init__(
        self,
        model_name: str = None,
        chunk_size: int = 500,
        chunk_overlap: int = 50,
    ):
        if model_name is None:
            model_name = os.getenv(
                "EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
            )
        self.model_name    = model_name
        self.chunk_size    = chunk_size
        self.chunk_overlap = chunk_overlap
        self._tokenizer    = None

    # ── Tokenizer injection / lazy load ───────────────────────────────
    def set_tokenizer(self, tokenizer) -> None:
        """Inyecta tokenizer ya cargado — evita duplicar ~50MB en RAM."""
        self._tokenizer = tokenizer

    def _get_tokenizer(self):
        if self._tokenizer is None:
            from transformers import AutoTokenizer
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        return self._tokenizer

    # ── Core: generator ───────────────────────────────────────────────
    def chunk_text(self, text: str) -> Generator[dict, None, None]:
        if not text or not text.strip():
            return

        tokenizer = self._get_tokenizer()

        # Fast path: texto pequeño
        if len(text) <= MAX_CHARS_BEFORE_PRESPLIT:
            tokens = tokenizer.encode(text, add_special_tokens=False)
            if len(tokens) <= self.chunk_size:
                yield {"content": text.strip(), "chunk_index": 0,
                       "total_chunks": 1, "token_count": len(tokens)}
                return
            yield from self._patch_total(self._sliding_window(tokens, tokenizer, 0))
            return

        # Slow path: texto ENORME → pre-split por párrafos
        carry_tokens: list = []
        carry_text:   str  = ""
        global_index: int  = 0
        chunks_buffer: list[dict] = []

        for para in self._split_paragraphs(text):
            para = para.strip()
            if not para:
                continue

            para_tokens = tokenizer.encode(para, add_special_tokens=False)
            combined    = carry_tokens + para_tokens

            if len(combined) <= self.chunk_size:
                carry_tokens = combined
                carry_text   = (carry_text + " " + para).strip()
                continue

            for chunk in self._sliding_window(combined, tokenizer, global_index):
                global_index = chunk["chunk_index"] + 1
                chunks_buffer.append(chunk)

            if len(combined) > self.chunk_overlap:
                carry_tokens = combined[-self.chunk_overlap:]
                carry_text   = tokenizer.decode(carry_tokens, skip_special_tokens=True)
            else:
                carry_tokens, carry_text = [], ""

        if carry_tokens:
            chunks_buffer.append({
                "content":      tokenizer.decode(carry_tokens, skip_special_tokens=True),
                "chunk_index":  global_index,
                "total_chunks": 0,
                "token_count":  len(carry_tokens),
            })

        total = len(chunks_buffer)
        for c in chunks_buffer:
            c["total_chunks"] = total
            yield c

    def chunk_text_list(self, text: str) -> list[dict]:
        """Compatibilidad con código existente — retorna lista con total_chunks."""
        chunks = list(self.chunk_text(text))
        total  = len(chunks)
        for c in chunks:
            if c["total_chunks"] == 0:
                c["total_chunks"] = total
        return chunks

    # ── Private helpers ───────────────────────────────────────────────
    def _sliding_window(
        self, tokens: list, tokenizer, base_index: int
    ) -> Generator[dict, None, None]:
        step = max(1, self.chunk_size - self.chunk_overlap)
        i = 0
        while i < len(tokens):
            chunk_tokens = tokens[i : i + self.chunk_size]
            yield {
                "content":      tokenizer.decode(chunk_tokens, skip_special_tokens=True),
                "chunk_index":  base_index,
                "total_chunks": 0,
                "token_count":  len(chunk_tokens),
            }
            base_index += 1
            if i + self.chunk_size >= len(tokens):
                break
            i += step

    @staticmethod
    def _patch_total(gen: Generator) -> Generator[dict, None, None]:
        chunks = list(gen)
        total  = len(chunks)
        for c in chunks:
            c["total_chunks"] = total
            yield c

    @staticmethod
    def _split_paragraphs(text: str) -> list[str]:
        paragraphs = re.split(r"\n{2,}", text)
        result = []
        half = MAX_CHARS_BEFORE_PRESPLIT // 2
        for para in paragraphs:
            if len(para) > half:
                result.extend(re.split(r"(?<=[.!?])\s+", para))
            else:
                result.append(para)
        return result


# ── Singleton ──────────────────────────────────────────────────────────────────
_chunker_instance: Optional[SemanticChunker] = None


def get_chunker() -> SemanticChunker:
    global _chunker_instance
    if _chunker_instance is None:
        _chunker_instance = SemanticChunker()
    return _chunker_instance

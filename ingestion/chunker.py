import os

# ============================================================
# High-Performance Chunker (Enterprise Edition)
# ============================================================
# Sliding window chunking with overlap (LangChain-style)
# ============================================================

class SemanticChunker:
    def __init__(self, model_name: str = None, chunk_size: int = 500, chunk_overlap: int = 50):
        """
        - chunk_size: max tokens per chunk
        - chunk_overlap: overlap between chunks
        """

        if model_name is None:
            model_name = os.getenv(
                "EMBEDDING_MODEL",
                "sentence-transformers/all-MiniLM-L6-v2"
            )

        self.model_name = model_name
        self.tokenizer = None  # 👈 lazy load
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    # ========================================================
    # LAZY TOKENIZER LOADING (FIX CRÍTICO)
    # ========================================================
    def _get_tokenizer(self):
        if self.tokenizer is None:
            from transformers import AutoTokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        return self.tokenizer

    def chunk_text(self, text: str) -> list[dict]:
        tokenizer = self._get_tokenizer()

        tokens = tokenizer.encode(text, add_special_tokens=False)
        total_tokens = len(tokens)

        # ✅ Small text → single chunk
        if total_tokens <= self.chunk_size:
            return [{
                "content": text,
                "chunk_index": 0,
                "total_chunks": 1,
                "token_count": total_tokens,
            }]

        chunks = []
        step = self.chunk_size - self.chunk_overlap

        for i in range(0, total_tokens, step):
            chunk_tokens = tokens[i : i + self.chunk_size]
            chunk_content = tokenizer.decode(chunk_tokens, skip_special_tokens=True)

            chunks.append({
                "content": chunk_content,
                "chunk_index": len(chunks),
                "token_count": len(chunk_tokens),
                "total_chunks": 0,  # placeholder
            })

            if i + self.chunk_size >= total_tokens:
                break

        # Fix final
        total = len(chunks)
        for c in chunks:
            c["total_chunks"] = total

        return chunks


# ============================================================
# SINGLETON (NO RECARGA TOKENIZER)
# ============================================================
_chunker_instance = None

def get_chunker() -> SemanticChunker:
    global _chunker_instance
    if _chunker_instance is None:
        _chunker_instance = SemanticChunker()
    return _chunker_instance

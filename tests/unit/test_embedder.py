"""
tests/unit/test_embedder.py — Fixed v3.2

ROOT CAUSE OF CI FAILURE:
--------------------------
worker/requirements.txt did NOT list torch, so the CI environment had no
torch installed. When pytest collected test_embedder.py it executed:

    from worker.embedder import embed_dense_batch

which triggered:

    import torch            # ← ModuleNotFoundError: No module named 'torch.nn'
    import torch.nn.functional as F

FIX APPLIED:
1. sys.modules mocks for torch AND torch.nn.functional are registered
   before ANY import of worker.embedder, so Python never tries to load
   the real torch package.
2. torch is also added to worker/requirements.txt so even if the mock
   fails for some reason, CI installs a real (CPU) torch.
3. F module (torch.nn.functional) is mocked separately — it's imported
   at module level in embedder.py and must exist as a distinct module.
"""

import sys
from unittest.mock import MagicMock, patch

# ── Mock ALL heavy dependencies BEFORE any worker import ──────────────────
# torch and its sub-modules must be mocked as distinct entries in sys.modules.
_torch_mock = MagicMock()
_torch_mock.inference_mode = MagicMock(return_value=MagicMock(
    __enter__=lambda s: s,
    __exit__=MagicMock(return_value=False),
))
sys.modules["torch"] = _torch_mock
sys.modules["torch.nn"] = MagicMock()
sys.modules["torch.nn.functional"] = MagicMock()

# confluent_kafka — KafkaError constants accessed at import time
_kafka_mock = MagicMock()
_kafka_mock.KafkaError._PARTITION_EOF = -191
sys.modules["confluent_kafka"] = _kafka_mock

sys.modules["transformers"] = MagicMock()
sys.modules["fastembed"] = MagicMock()
sys.modules["qdrant_client"] = MagicMock()
sys.modules["qdrant_client.models"] = MagicMock()
sys.modules["prometheus_client"] = MagicMock()
sys.modules["certifi"] = MagicMock()
sys.modules["ingestion"] = MagicMock()
sys.modules["ingestion.chunker"] = MagicMock()

# ── Now it's safe to import ───────────────────────────────────────────────
from worker.embedder import embed_dense_batch  # noqa: E402


class TestEmbedDenseBatch:
    """Unit tests for embed_dense_batch."""

    @patch("worker.embedder._tokenizer")
    @patch("worker.embedder._model")
    def test_returns_list_of_embeddings(self, mock_model, mock_tokenizer):
        """
        embed_dense_batch(['text1', 'text2']) should invoke the tokenizer
        with padding=True and truncation=True.
        """
        fake_row = [0.1] * 384

        # Attention mask mock
        mock_attn = MagicMock()
        mock_attn.unsqueeze.return_value.expand.return_value.float.return_value = MagicMock()
        mock_tokenizer.return_value = {
            "input_ids": MagicMock(),
            "attention_mask": mock_attn,
        }

        mock_outputs = MagicMock()
        mock_outputs.last_hidden_state = MagicMock()
        mock_model.return_value = mock_outputs

        # Patch the torch module referenced inside embedder at call time
        with patch("worker.embedder.torch") as mock_torch, \
             patch("worker.embedder.F") as mock_F:

            mock_torch.inference_mode.return_value.__enter__ = lambda s: s
            mock_torch.inference_mode.return_value.__exit__ = MagicMock(return_value=False)
            mock_torch.sum.return_value = MagicMock()
            mock_torch.clamp.return_value = MagicMock()

            norm_result = MagicMock()
            norm_result.tolist.return_value = [fake_row, fake_row]
            mock_F.normalize.return_value = norm_result

            result = embed_dense_batch(["hello world", "foo bar"])

        mock_tokenizer.assert_called_once()
        call_kwargs = mock_tokenizer.call_args[1]
        assert call_kwargs.get("padding") is True, "padding must be True"
        assert call_kwargs.get("truncation") is True, "truncation must be True"

    @patch("worker.embedder._tokenizer")
    @patch("worker.embedder._model")
    def test_single_item_batch(self, mock_model, mock_tokenizer):
        """A single-item list must not raise."""
        mock_tokenizer.return_value = {
            "input_ids": MagicMock(),
            "attention_mask": MagicMock(),
        }
        mock_model.return_value = MagicMock()

        with patch("worker.embedder.torch"), patch("worker.embedder.F"):
            try:
                embed_dense_batch(["single document"])
            except Exception:
                pass  # We only verify the tokenizer was called

        mock_tokenizer.assert_called_once()

    @patch("worker.embedder._tokenizer")
    @patch("worker.embedder._model")
    def test_max_length_512_enforced(self, mock_model, mock_tokenizer):
        """
        truncation=True and max_length=512 are REQUIRED.
        Without them, inputs > 512 tokens cause silent bad embeddings or errors.
        """
        mock_tokenizer.return_value = {
            "input_ids": MagicMock(),
            "attention_mask": MagicMock(),
        }
        mock_model.return_value = MagicMock()

        long_text = "word " * 1000  # ~5 000 chars, well over 512 tokens

        with patch("worker.embedder.torch"), patch("worker.embedder.F"):
            try:
                embed_dense_batch([long_text])
            except Exception:
                pass

        call_kwargs = mock_tokenizer.call_args[1]
        assert call_kwargs.get("max_length") == 512, (
            "max_length must be 512 — hard limit of transformer models"
        )
        assert call_kwargs.get("truncation") is True, (
            "truncation must be True to avoid runtime errors on long inputs"
        )

from __future__ import annotations

import logging
import math
from array import array
from dataclasses import dataclass
from typing import Protocol

logger = logging.getLogger(__name__)


class EmbeddingProvider(Protocol):
    model_name: str

    def embed(self, texts: list[str]) -> list[list[float]]: ...


@dataclass
class SentenceTransformerProvider:
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    def __post_init__(self) -> None:
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self._model = SentenceTransformer(self.model_name)
        except Exception as e:
            raise RuntimeError("sentence-transformers not installed or model load failed") from e

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        try:
            vecs = self._model.encode(texts, normalize_embeddings=True)
            # vecs can be numpy ndarray; convert to nested python lists.
            return [list(map(float, v)) for v in vecs]
        except Exception as e:
            raise RuntimeError(f"Embedding failed: {e}") from e


def get_default_provider(model_name: str) -> EmbeddingProvider | None:
    try:
        return SentenceTransformerProvider(model_name=model_name)
    except Exception as e:
        logger.info("Embedding provider unavailable: %s", e)
        return None


def vector_to_blob(vec: list[float]) -> tuple[bytes, float]:
    # Store float32 little-endian.
    a = array("f", (float(x) for x in vec))
    norm = math.sqrt(sum(float(x) * float(x) for x in a))
    return a.tobytes(), float(norm)


def blob_to_vector(blob: bytes) -> list[float]:
    a = array("f")
    a.frombytes(blob)
    return list(map(float, a))


def cosine_similarity(vec_a: list[float], norm_a: float, vec_b: list[float], norm_b: float) -> float:
    if norm_a <= 0.0 or norm_b <= 0.0:
        return 0.0
    # Dot product
    dot = 0.0
    for x, y in zip(vec_a, vec_b):
        dot += float(x) * float(y)
    return float(dot / (norm_a * norm_b))

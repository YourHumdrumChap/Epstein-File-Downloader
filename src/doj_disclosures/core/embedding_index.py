from __future__ import annotations

import logging
from dataclasses import dataclass

from doj_disclosures.core.embeddings import EmbeddingProvider, vector_to_blob
from doj_disclosures.core.utils import chunk_text

logger = logging.getLogger(__name__)


@dataclass
class EmbeddedChunk:
    chunk_index: int
    start_offset: int
    end_offset: int
    vector: bytes
    norm: float


def build_embeddings_for_text(
    text: str,
    *,
    provider: EmbeddingProvider,
    max_chars: int = 2500,
    overlap: int = 250,
) -> list[dict]:
    if not text.strip():
        return []

    # Chunk with offsets.
    chunks: list[tuple[int, int, str]] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        chunks.append((start, end, text[start:end]))
        if end == len(text):
            break
        start = max(0, end - overlap)

    texts = [c[2] for c in chunks]
    vecs = provider.embed(texts)
    if len(vecs) != len(chunks):
        logger.warning("Embedding count mismatch: %s != %s", len(vecs), len(chunks))

    out: list[dict] = []
    for idx, (st, en, _t) in enumerate(chunks):
        if idx >= len(vecs):
            break
        blob, norm = vector_to_blob(vecs[idx])
        out.append(
            {
                "chunk_index": idx,
                "start_offset": st,
                "end_offset": en,
                "model_name": getattr(provider, "model_name", ""),
                "vector": blob,
                "norm": norm,
            }
        )
    return out

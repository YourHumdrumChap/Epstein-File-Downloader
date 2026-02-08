from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from doj_disclosures.core.embeddings import EmbeddingProvider, cosine_similarity, vector_to_blob, blob_to_vector

logger = logging.getLogger(__name__)


TOPIC_PHRASES: list[str] = [
    "flight log",
    "passenger manifest",
    "contact book",
    "deposition transcript",
    "travel itinerary",
]


@dataclass(frozen=True)
class TopicVector:
    vec: list[float]
    norm: float


@dataclass(frozen=True)
class RelevanceResult:
    topic_similarity: float
    feedback_similarity_boost: float
    url_penalty: float
    entity_density: float
    relevance_score: float


def embed_text(provider: EmbeddingProvider, text: str, *, max_chars: int = 12000) -> tuple[list[float], float]:
    t = (text or "").strip()
    if not t:
        return [], 0.0
    vec = provider.embed([t[:max_chars]])[0]
    blob, norm = vector_to_blob(vec)
    # round-trip ensures float32-like normalization consistency
    return blob_to_vector(blob), norm


def build_topic_vector(provider: EmbeddingProvider, phrases: list[str] | None = None) -> TopicVector:
    phrases = phrases or TOPIC_PHRASES
    vecs = provider.embed([p.strip() for p in phrases if p.strip()])
    if not vecs:
        return TopicVector(vec=[], norm=0.0)
    dim = len(vecs[0])
    avg = [0.0] * dim
    for v in vecs:
        for i, x in enumerate(v):
            avg[i] += float(x)
    n = float(len(vecs))
    avg = [x / n for x in avg]
    blob, norm = vector_to_blob(avg)
    return TopicVector(vec=blob_to_vector(blob), norm=norm)


def compute_entity_density(*, total_entity_mentions: int, total_words: int) -> float:
    if total_words <= 0:
        return 0.0
    return float(total_entity_mentions / total_words)


def hostname(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().strip()
    except Exception:
        return ""


def load_url_penalties(raw_json: str | None) -> dict[str, float]:
    if not raw_json:
        return {}
    try:
        data = json.loads(raw_json)
        if isinstance(data, dict):
            out: dict[str, float] = {}
            for k, v in data.items():
                if not k:
                    continue
                try:
                    out[str(k)] = float(v)
                except Exception:
                    continue
            return out
    except Exception:
        return {}
    return {}


def dump_url_penalties(penalties: dict[str, float]) -> str:
    return json.dumps(penalties, sort_keys=True)


def compute_relevance(
    *,
    doc_vec: list[float],
    doc_norm: float,
    topic: TopicVector,
    hv_centroid: tuple[list[float], float] | None,
    ir_centroid: tuple[list[float], float] | None,
    url_penalty: float,
    entity_density: float,
) -> RelevanceResult:
    topic_sim = cosine_similarity(doc_vec, doc_norm, topic.vec, topic.norm) if topic.norm > 0 else 0.0

    hv_sim = cosine_similarity(doc_vec, doc_norm, hv_centroid[0], hv_centroid[1]) if hv_centroid else 0.0
    ir_sim = cosine_similarity(doc_vec, doc_norm, ir_centroid[0], ir_centroid[1]) if ir_centroid else 0.0
    feedback_boost = float(hv_sim - ir_sim)

    # Keep scoring conservative; topic similarity dominates.
    rel = 0.75 * float(topic_sim) + 0.25 * float(feedback_boost)
    rel -= float(url_penalty)

    # Very low entity density is usually procedural/boilerplate.
    if entity_density <= 0.0:
        rel *= 0.75

    return RelevanceResult(
        topic_similarity=float(topic_sim),
        feedback_similarity_boost=float(feedback_boost),
        url_penalty=float(url_penalty),
        entity_density=float(entity_density),
        relevance_score=float(rel),
    )

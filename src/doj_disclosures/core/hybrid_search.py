from __future__ import annotations

import logging
import math

from doj_disclosures.core.db import Database
from doj_disclosures.core.embeddings import blob_to_vector, cosine_similarity, get_default_provider

logger = logging.getLogger(__name__)


def _tanh(x: float) -> float:
    try:
        return float(math.tanh(float(x)))
    except Exception:
        return 0.0


def _review_bias(status: str) -> float:
    s = (status or "").strip().lower()
    if s == "high_value":
        return 0.35
    if s == "irrelevant":
        return -0.60
    return 0.0


class HybridSearcher:
    def __init__(
        self,
        *,
        db: Database,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        semantic_weight: float = 2.0,
        feedback_weight: float = 1.4,
        prior_weight: float = 0.7,
        url_penalty_weight: float = 0.35,
        keyword_rank_weight: float = 1.0,
    ) -> None:
        self._db = db
        self._model_name = str(model_name)
        self._semantic_weight = float(semantic_weight)
        self._feedback_weight = float(feedback_weight)
        self._prior_weight = float(prior_weight)
        self._url_penalty_weight = float(url_penalty_weight)
        self._keyword_rank_weight = float(keyword_rank_weight)
        self._provider = None

    def _provider_or_none(self):
        if self._provider is not None:
            return self._provider
        self._provider = get_default_provider(self._model_name)
        return self._provider

    async def _get_feedback_centroids(self):
        try:
            hv = await self._db.get_feedback_centroid(label="high_value", model_name=self._model_name)
            ir = await self._db.get_feedback_centroid(label="irrelevant", model_name=self._model_name)
            hv_out = (list(hv.vec), float(hv.norm)) if hv is not None and float(hv.norm) > 0 else None
            ir_out = (list(ir.vec), float(ir.norm)) if ir is not None and float(ir.norm) > 0 else None
            return hv_out, ir_out
        except Exception:
            return None, None

    @staticmethod
    def _keyword_rank_scores(rows: list[dict]) -> dict[int, float]:
        n = max(1, len(rows))
        out: dict[int, float] = {}
        for i, r in enumerate(rows):
            doc_id = int(r.get("doc_id") or 0)
            if doc_id <= 0:
                continue
            out[doc_id] = float((n - i) / n)
        return out

    async def search(self, query: str, *, limit: int = 200, candidate_limit: int = 250) -> list[dict]:
        q = (query or "").strip()
        if not q:
            return []

        if hasattr(self._db, "fts_search_with_metrics"):
            fts = await self._db.fts_search_with_metrics(query=q, limit=candidate_limit)  # type: ignore[attr-defined]
        else:
            fts = await self._db.fts_search(query=q, limit=candidate_limit)

        if not fts:
            return []

        keyword_rank = self._keyword_rank_scores(fts)
        hv_centroid, ir_centroid = await self._get_feedback_centroids()

        provider = self._provider_or_none()
        qvec = None
        qnorm = 0.0
        if provider is not None:
            try:
                qvec = provider.embed([q])[0]
                qnorm = sum(float(x) * float(x) for x in qvec) ** 0.5
            except Exception as e:
                logger.info("Query embedding failed; continuing without query semantic: %s", e)
                qvec = None
                qnorm = 0.0

        reranked: list[dict] = []
        for r in fts:
            doc_id = int(r.get("doc_id") or 0)
            if doc_id <= 0:
                continue

            prior = 0.0
            try:
                rs = r.get("relevance_score")
                prior = float(rs) if rs is not None else 0.0
            except Exception:
                prior = 0.0
            prior_scaled = _tanh(1.5 * prior)

            try:
                url_pen = float(r.get("url_penalty") or 0.0)
            except Exception:
                url_pen = 0.0

            review_status = str(r.get("review_status") or "new")
            bias = _review_bias(review_status)

            best_query_sem = 0.0
            best_hv = 0.0
            best_ir = 0.0

            if provider is not None and (qvec is not None or hv_centroid or ir_centroid):
                try:
                    embs = await self._db.query_embeddings_for_doc(doc_id=doc_id, model_name=self._model_name)
                except Exception:
                    embs = []

                for e in embs:
                    dvec = blob_to_vector(e["vector"])
                    dnorm = float(e.get("norm") or 0.0)

                    if qvec is not None and qnorm > 0 and dnorm > 0:
                        sim = cosine_similarity(qvec, qnorm, dvec, dnorm)
                        if sim > best_query_sem:
                            best_query_sem = float(sim)

                    if hv_centroid is not None and dnorm > 0:
                        hv_sim = cosine_similarity(dvec, dnorm, hv_centroid[0], hv_centroid[1])
                        if hv_sim > best_hv:
                            best_hv = float(hv_sim)

                    if ir_centroid is not None and dnorm > 0:
                        ir_sim = cosine_similarity(dvec, dnorm, ir_centroid[0], ir_centroid[1])
                        if ir_sim > best_ir:
                            best_ir = float(ir_sim)

            feedback_boost = float(best_hv - best_ir)
            kw_rank = float(keyword_rank.get(doc_id, 0.0))

            score = 0.0
            score += self._keyword_rank_weight * kw_rank
            score += self._semantic_weight * float(best_query_sem)
            score += self._feedback_weight * float(feedback_boost)
            score += self._prior_weight * float(prior_scaled)
            score -= self._url_penalty_weight * float(url_pen)
            score += float(bias)

            reranked.append(
                {
                    **r,
                    "semantic": float(best_query_sem),
                    "feedback_boost": float(feedback_boost),
                    "keyword_rank": float(kw_rank),
                    "prior": float(prior),
                    "score": float(score),
                }
            )

        reranked.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
        return reranked[:limit]

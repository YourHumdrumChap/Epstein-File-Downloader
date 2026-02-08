from __future__ import annotations

import logging

from doj_disclosures.core.db import Database
from doj_disclosures.core.embeddings import blob_to_vector, cosine_similarity, get_default_provider

logger = logging.getLogger(__name__)


class HybridSearcher:
    def __init__(
        self,
        *,
        db: Database,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        semantic_weight: float = 2.0,
    ) -> None:
        self._db = db
        self._model_name = model_name
        self._semantic_weight = float(semantic_weight)
        self._provider = None

    def _provider_or_none(self):
        if self._provider is not None:
            return self._provider
        self._provider = get_default_provider(self._model_name)
        return self._provider

    async def search(self, query: str, *, limit: int = 200, candidate_limit: int = 250) -> list[dict]:
        q = (query or "").strip()
        if not q:
            return []

        fts = await self._db.fts_search(query=q, limit=candidate_limit)
        if not fts:
            return []

        provider = self._provider_or_none()
        if provider is None:
            # FTS-only fallback.
            out = []
            for r in fts[:limit]:
                out.append({**r, "semantic": 0.0, "score": float(-r["bm25"])})
            return out

        try:
            qvec = provider.embed([q])[0]
        except Exception as e:
            logger.info("Query embedding failed; using FTS only: %s", e)
            out = []
            for r in fts[:limit]:
                out.append({**r, "semantic": 0.0, "score": float(-r["bm25"])})
            return out

        qnorm = sum(float(x) * float(x) for x in qvec) ** 0.5

        reranked: list[dict] = []
        for r in fts:
            doc_id = int(r["doc_id"])
            embs = await self._db.query_embeddings_for_doc(doc_id=doc_id, model_name=self._model_name)
            best = 0.0
            for e in embs:
                dvec = blob_to_vector(e["vector"])
                dnorm = float(e.get("norm") or 0.0)
                sim = cosine_similarity(qvec, qnorm, dvec, dnorm)
                if sim > best:
                    best = sim
            # Convert bm25 (lower better) to a positive-ish keyword score.
            keyword = float(-r["bm25"])
            score = keyword + self._semantic_weight * float(best)
            reranked.append({**r, "semantic": float(best), "score": float(score)})

        reranked.sort(key=lambda x: x["score"], reverse=True)
        return reranked[:limit]

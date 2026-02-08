from __future__ import annotations

import logging
from dataclasses import dataclass

from doj_disclosures.core.matching import MatchHit

logger = logging.getLogger(__name__)


@dataclass
class SemanticMatcher:
    threshold: float = 0.62
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"

    def __post_init__(self) -> None:
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self._model = SentenceTransformer(self.model_name)
        except Exception as e:
            raise RuntimeError("sentence-transformers not installed or model load failed") from e

    def match(self, text: str, keywords: list[str]) -> list[MatchHit]:
        if not text.strip() or not keywords:
            return []
        import numpy as np  # type: ignore

        tvec = self._model.encode([text], normalize_embeddings=True)
        kvec = self._model.encode(keywords, normalize_embeddings=True)
        sims = (kvec @ tvec[0]).astype(float)
        hits: list[MatchHit] = []
        for kw, sim in zip(keywords, sims):
            score = float(sim)
            if score >= self.threshold:
                hits.append(MatchHit(method="semantic", pattern=kw, score=score, snippet=text[:350]))
        hits.sort(key=lambda h: h.score, reverse=True)
        return hits[:30]

    def suggest_related(self, keywords: list[str], *, k: int = 12) -> list[str]:
        if len(keywords) < 2:
            return []
        import numpy as np  # type: ignore

        vecs = self._model.encode(keywords, normalize_embeddings=True)
        sims = vecs @ vecs.T
        out: set[str] = set()
        for i in range(len(keywords)):
            idx = np.argsort(-sims[i])
            for j in idx[1:]:
                out.add(keywords[int(j)])
                if len(out) >= k:
                    return list(out)
        return list(out)

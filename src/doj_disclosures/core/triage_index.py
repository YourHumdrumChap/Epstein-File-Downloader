from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class IndexRow:
    local_path: str
    relevance_score: float | None
    topic_similarity: float | None
    entity_density: float | None
    review_status: str
    url: str
    title: str


def _score_key(x: IndexRow) -> float:
    if x.relevance_score is None:
        return float("-inf")
    return float(x.relevance_score)


def write_semantic_sorted_index(*, out_dir: Path, rows: list[dict[str, Any]], filename: str = "semantic_sorted.txt") -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    normalized: list[IndexRow] = []
    for r in rows:
        normalized.append(
            IndexRow(
                local_path=str(r.get("local_path") or ""),
                relevance_score=(float(r["relevance_score"]) if r.get("relevance_score") is not None else None),
                topic_similarity=(float(r["topic_similarity"]) if r.get("topic_similarity") is not None else None),
                entity_density=(float(r["entity_density"]) if r.get("entity_density") is not None else None),
                review_status=str(r.get("review_status") or "new"),
                url=str(r.get("url") or ""),
                title=str(r.get("title") or ""),
            )
        )

    normalized.sort(key=_score_key, reverse=True)
    lines: list[str] = []
    lines.append("relevance_score\ttopic_similarity\tentity_density\treview_status\tlocal_path\turl\ttitle")
    for x in normalized:
        lines.append(
            f"{'' if x.relevance_score is None else f'{x.relevance_score:.4f}'}\t"
            f"{'' if x.topic_similarity is None else f'{x.topic_similarity:.4f}'}\t"
            f"{'' if x.entity_density is None else f'{x.entity_density:.6f}'}\t"
            f"{x.review_status}\t{x.local_path}\t{x.url}\t{x.title}"
        )

    p = out_dir / filename
    p.write_text("\n".join(lines), encoding="utf-8")
    return p

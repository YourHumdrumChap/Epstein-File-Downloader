from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DocumentMeta:
    url: str
    final_url: str
    title: str
    content_type: str
    file_size: int | None
    sha256: str
    local_path: str
    fetched_at_iso: str


@dataclass(frozen=True)
class MatchResult:
    doc_id: int
    url: str
    title: str
    method: str
    pattern: str
    score: float
    snippet: str

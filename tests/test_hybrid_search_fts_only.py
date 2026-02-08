from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from doj_disclosures.core.db import Database
from doj_disclosures.core.hybrid_search import HybridSearcher


@pytest.mark.asyncio
async def test_hybrid_search_falls_back_to_fts_only(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    db = Database(path=db_path)
    db.initialize_sync()

    now = datetime.now(timezone.utc).isoformat()
    doc_id = await db.add_document(
        url="https://example.com/a.txt",
        final_url="https://example.com/a.txt",
        title="Example A",
        content_type="text/plain",
        file_size=10,
        sha256="2" * 64,
        local_path=str(tmp_path / "a.txt"),
        fetched_at=now,
    )
    await db.add_fts_content(doc_id=doc_id, url="https://example.com/a.txt", title="Example A", content="hello world")

    searcher = HybridSearcher(db=db)
    rows = await searcher.search("hello", limit=10)
    assert rows
    assert rows[0]["doc_id"] == doc_id

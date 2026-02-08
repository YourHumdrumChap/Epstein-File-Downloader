from __future__ import annotations

import pytest

from doj_disclosures.core.db import Database


@pytest.mark.asyncio
async def test_db_init_and_fts_insert(tmp_db_path) -> None:
    db = Database(tmp_db_path)
    db.initialize_sync()
    doc_id = await db.add_document(
        url="u",
        final_url="u",
        title="t",
        content_type="text/plain",
        file_size=None,
        sha256="a" * 64,
        local_path="/tmp/x",
        fetched_at="2020-01-01T00:00:00Z",
    )
    await db.add_fts_content(doc_id=doc_id, url="u", title="t", content="hello world")
    await db.add_matches(doc_id=doc_id, matches=[("keyword", "hello", 1.0, "hello")], created_at="2020-01-01T00:00:00Z")
    rows = await db.query_flagged(limit=10)
    assert rows and rows[0]["doc_id"] == doc_id

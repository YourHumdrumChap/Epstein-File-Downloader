from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from doj_disclosures.core.db import Database


@pytest.mark.asyncio
async def test_store_and_query_entities(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    db = Database(path=db_path)
    db.initialize_sync()

    now = datetime.now(timezone.utc).isoformat()

    doc_id = await db.add_document(
        url="https://example.com/a.pdf",
        final_url="https://example.com/a.pdf",
        title="A",
        content_type="application/pdf",
        file_size=123,
        sha256="1" * 64,
        local_path=str(tmp_path / "a.pdf"),
        fetched_at=now,
    )

    entities = [
        {
            "label": "EMAIL",
            "canonical": "john@example.com",
            "display": "john@example.com",
            "count": 2,
            "variants": ["John@Example.com", "john@example.com"],
            "page_nos": [1],
        }
    ]

    await db.add_entities(doc_id=doc_id, entities=entities, created_at=now)
    got = await db.query_entities_for_doc(doc_id)

    assert len(got) == 1
    assert got[0]["label"] == "EMAIL"
    assert got[0]["canonical"] == "john@example.com"
    assert got[0]["count"] == 2
    assert 1 in got[0]["page_nos"]


def test_schema_contains_doc_entities(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    db = Database(path=db_path)
    db.initialize_sync()

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='doc_entities'"
        ).fetchall()
        assert rows and rows[0][0] == "doc_entities"
    finally:
        conn.close()

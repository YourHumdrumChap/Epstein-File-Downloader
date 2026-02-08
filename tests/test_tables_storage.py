from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from doj_disclosures.core.db import Database


@pytest.mark.asyncio
async def test_store_and_query_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    db = Database(path=db_path)
    db.initialize_sync()

    now = datetime.now(timezone.utc).isoformat()

    # Create a document row so we have a doc_id.
    doc_id = await db.add_document(
        url="https://example.com/a.pdf",
        final_url="https://example.com/a.pdf",
        title="A",
        content_type="application/pdf",
        file_size=123,
        sha256="0" * 64,
        local_path=str(tmp_path / "a.pdf"),
        fetched_at=now,
    )

    tables = [
        {
            "page_no": 1,
            "table_index": 0,
            "format": "rows",
            "data": [["A1", "B1"], ["A2", "B2"]],
            "bbox": [0.0, 0.0, 100.0, 100.0],
        }
    ]

    await db.add_tables(doc_id=doc_id, tables=tables, created_at=now)
    got = await db.query_tables_for_doc(doc_id)

    assert len(got) == 1
    assert got[0]["page_no"] == 1
    assert got[0]["table_index"] == 0
    assert got[0]["format"] == "rows"
    assert got[0]["data"][0][0] == "A1"
    assert got[0]["bbox"][2] == 100.0


def test_schema_contains_doc_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    db = Database(path=db_path)
    db.initialize_sync()

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='doc_tables'"
        ).fetchall()
        assert rows and rows[0][0] == "doc_tables"
    finally:
        conn.close()

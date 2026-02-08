from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import aiosqlite

logger = logging.getLogger(__name__)


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS urls (
  url TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  discovered_at TEXT NOT NULL,
  last_attempt_at TEXT,
  http_status INTEGER,
  error TEXT,
  content_type TEXT,
  title TEXT,
  final_url TEXT,
  local_path TEXT,
    sha256 TEXT,
    etag TEXT,
    last_modified TEXT
);

CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT NOT NULL,
  final_url TEXT NOT NULL,
  title TEXT,
  content_type TEXT,
  file_size INTEGER,
  sha256 TEXT NOT NULL,
  local_path TEXT NOT NULL,
  fetched_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_documents_sha256 ON documents(sha256);
CREATE INDEX IF NOT EXISTS idx_documents_url ON documents(url);

CREATE TABLE IF NOT EXISTS matches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id INTEGER NOT NULL,
  method TEXT NOT NULL,
  pattern TEXT NOT NULL,
  score REAL NOT NULL,
  snippet TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(doc_id) REFERENCES documents(id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS fts_docs USING fts5(
  doc_id UNINDEXED,
  url UNINDEXED,
  title,
  content,
  tokenize = 'unicode61'
);

CREATE TABLE IF NOT EXISTS doc_tables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id INTEGER NOT NULL,
    page_no INTEGER NOT NULL,
    table_index INTEGER NOT NULL,
    format TEXT NOT NULL,
    data_json TEXT NOT NULL,
    bbox_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(doc_id) REFERENCES documents(id)
);

CREATE INDEX IF NOT EXISTS idx_doc_tables_doc_id ON doc_tables(doc_id);

CREATE TABLE IF NOT EXISTS doc_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id INTEGER NOT NULL,
    label TEXT NOT NULL,
    canonical TEXT NOT NULL,
    display TEXT NOT NULL,
    count INTEGER NOT NULL,
    variants_json TEXT NOT NULL,
    page_nos_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(doc_id) REFERENCES documents(id),
    UNIQUE(doc_id, label, canonical)
);

CREATE INDEX IF NOT EXISTS idx_doc_entities_doc_id ON doc_entities(doc_id);

CREATE TABLE IF NOT EXISTS doc_embeddings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    start_offset INTEGER,
    end_offset INTEGER,
    model_name TEXT NOT NULL,
    vector BLOB NOT NULL,
    norm REAL NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(doc_id) REFERENCES documents(id),
    UNIQUE(doc_id, model_name, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_doc_embeddings_doc_id ON doc_embeddings(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_embeddings_model ON doc_embeddings(model_name);

CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS doc_page_flags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id INTEGER NOT NULL,
    page_no INTEGER NOT NULL,
    flag TEXT NOT NULL,
    score REAL NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(doc_id) REFERENCES documents(id)
);

CREATE INDEX IF NOT EXISTS idx_doc_page_flags_doc_id ON doc_page_flags(doc_id);
CREATE INDEX IF NOT EXISTS idx_doc_page_flags_flag ON doc_page_flags(flag);

CREATE TABLE IF NOT EXISTS doc_reviews (
    doc_id INTEGER PRIMARY KEY,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(doc_id) REFERENCES documents(id)
);

CREATE TABLE IF NOT EXISTS feedback_centroids (
    label TEXT NOT NULL,
    model_name TEXT NOT NULL,
    vector BLOB NOT NULL,
    norm REAL NOT NULL,
    count INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(label, model_name)
);
"""


@dataclass(frozen=True)
class UrlCachedRecord:
    url: str
    local_path: str | None
    content_type: str | None
    sha256: str | None
    final_url: str | None
    title: str | None


@dataclass(frozen=True)
class Database:
    path: Path

    @staticmethod
    def _ensure_columns_sync(conn: sqlite3.Connection, *, table: str, columns: dict[str, str]) -> None:
        existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        for name, col_type in columns.items():
            if name in existing:
                continue
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")

    def initialize_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        try:
            conn.executescript(SCHEMA_SQL)
            # Schema migration for existing DBs.
            self._ensure_columns_sync(
                conn,
                table="urls",
                columns={
                    "etag": "TEXT",
                    "last_modified": "TEXT",
                },
            )
            self._ensure_columns_sync(
                conn,
                table="documents",
                columns={
                    "relevance_score": "REAL",
                    "topic_similarity": "REAL",
                    "entity_density": "REAL",
                    "url_penalty": "REAL",
                },
            )
            conn.commit()
        finally:
            conn.close()

    async def _connect(self) -> aiosqlite.Connection:
        """Create and initialize an aiosqlite connection.

        Important: callers should NOT use `async with conn` on the returned connection,
        because the connection is already awaited/started. Use try/finally + close.
        """

        conn = await aiosqlite.connect(self.path)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    async def upsert_url(self, *, url: str, status: str, discovered_at: str, preserve_done: bool = True) -> None:
        await self.upsert_urls(urls=[url], status=status, discovered_at=discovered_at, preserve_done=preserve_done)

    async def upsert_urls(self, *, urls: Iterable[str], status: str, discovered_at: str, preserve_done: bool = True) -> None:
        url_list = [u for u in urls if u]
        if not url_list:
            return

        conn = await self._connect()
        try:
            if preserve_done:
                # Preserve completed downloads, but allow re-queueing of incomplete/incorrectly-marked rows.
                # In particular, PDFs marked done without a local_path/sha256 should not be treated as completed.
                sql = (
                    "INSERT INTO urls(url,status,discovered_at) VALUES(?,?,?) "
                    "ON CONFLICT(url) DO UPDATE SET status=CASE "
                    "WHEN urls.status='done' AND (urls.url NOT LIKE '%.pdf' OR (COALESCE(urls.local_path,'')<>'' AND COALESCE(urls.sha256,'')<>'')) "
                    "THEN 'done' "
                    "ELSE excluded.status END"
                )
            else:
                sql = (
                    "INSERT INTO urls(url,status,discovered_at) VALUES(?,?,?) "
                    "ON CONFLICT(url) DO UPDATE SET status=excluded.status, discovered_at=excluded.discovered_at"
                )
            await conn.executemany(sql, [(u, status, discovered_at) for u in url_list])
            await conn.commit()
        finally:
            await conn.close()

    async def update_url_attempt(
        self,
        *,
        url: str,
        status: str,
        last_attempt_at: str,
        http_status: int | None,
        error: str | None,
        content_type: str | None = None,
        title: str | None = None,
        final_url: str | None = None,
        local_path: str | None = None,
        sha256: str | None = None,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                "UPDATE urls SET status=?, last_attempt_at=?, http_status=?, error=?, content_type=?, title=?, final_url=?, local_path=?, sha256=?, etag=?, last_modified=? WHERE url=?",
                (
                    status,
                    last_attempt_at,
                    http_status,
                    error,
                    content_type,
                    title,
                    final_url,
                    local_path,
                    sha256,
                    etag,
                    last_modified,
                    url,
                ),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def get_url_cache_headers(self, *, url: str) -> tuple[str | None, str | None]:
        conn = await self._connect()
        try:
            async with conn.execute("SELECT etag, last_modified FROM urls WHERE url=?", (url,)) as cur:
                row = await cur.fetchone()
                if not row:
                    return None, None
                return (row[0] or None), (row[1] or None)
        finally:
            await conn.close()

    async def get_url_cached_record(self, *, url: str) -> UrlCachedRecord | None:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT local_path, content_type, sha256, final_url, title FROM urls WHERE url=?",
                (url,),
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                return UrlCachedRecord(
                    url=url,
                    local_path=(row[0] or None),
                    content_type=(row[1] or None),
                    sha256=(row[2] or None),
                    final_url=(row[3] or None),
                    title=(row[4] or None),
                )
        finally:
            await conn.close()

    async def get_url_debug_info(self, *, url: str) -> tuple[str, int | None, str | None] | None:
        """Return (status, http_status, error) for a URL (for logging/debugging)."""

        conn = await self._connect()
        try:
            async with conn.execute("SELECT status, http_status, error FROM urls WHERE url=?", (url,)) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                return str(row[0]), (int(row[1]) if row[1] is not None else None), (row[2] or None)
        finally:
            await conn.close()

    async def purge_derived_for_doc(self, *, doc_id: int) -> None:
        """Remove derived/indexed rows for a document so it can be reprocessed cleanly."""

        conn = await self._connect()
        try:
            await conn.execute("DELETE FROM matches WHERE doc_id=?", (doc_id,))
            await conn.execute("DELETE FROM doc_tables WHERE doc_id=?", (doc_id,))
            await conn.execute("DELETE FROM doc_entities WHERE doc_id=?", (doc_id,))
            await conn.execute("DELETE FROM doc_embeddings WHERE doc_id=?", (doc_id,))
            await conn.execute("DELETE FROM doc_page_flags WHERE doc_id=?", (doc_id,))
            await conn.execute("DELETE FROM fts_docs WHERE doc_id=?", (doc_id,))
            await conn.commit()
        finally:
            await conn.close()

    async def update_document_storage(self, *, doc_id: int, local_path: str, title: str | None, content_type: str | None) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                "UPDATE documents SET local_path=COALESCE(?, local_path), title=COALESCE(?, title), content_type=COALESCE(?, content_type) WHERE id=?",
                (local_path, title, content_type, doc_id),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def get_pending_urls(self, limit: int = 500) -> list[tuple[str, str | None]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT url, content_type FROM urls WHERE status IN ('queued','retry') "
                "ORDER BY "
                "CASE "
                "WHEN lower(url) LIKE '%.pdf' OR lower(url) LIKE '%.doc' OR lower(url) LIKE '%.docx' OR lower(url) LIKE '%.txt' OR lower(url) LIKE '%.html' OR lower(url) LIKE '%.htm' "
                "THEN 1 ELSE 0 END ASC, "
                "discovered_at ASC LIMIT ?",
                (limit,),
            ) as cur:
                rows = await cur.fetchall()
                return [(r[0], r[1]) for r in rows]
        finally:
            await conn.close()

    async def clear_pending_urls(self) -> None:
        """Abandon any queued/retry/processing URLs.

        This keeps the DB history but prevents a new run from automatically continuing
        a previous queue when the user provides new seed URLs.
        """

        conn = await self._connect()
        try:
            await conn.execute(
                "UPDATE urls SET status='abandoned' WHERE status IN ('queued','retry','processing')"
            )
            await conn.commit()
        finally:
            await conn.close()

    async def add_document(
        self,
        *,
        url: str,
        final_url: str,
        title: str,
        content_type: str,
        file_size: int | None,
        sha256: str,
        local_path: str,
        fetched_at: str,
    ) -> int:
        conn = await self._connect()
        try:
            async with conn.execute("SELECT id FROM documents WHERE sha256=?", (sha256,)) as cur:
                row = await cur.fetchone()
                if row:
                    return int(row[0])
            cur = await conn.execute(
                "INSERT INTO documents(url,final_url,title,content_type,file_size,sha256,local_path,fetched_at) VALUES(?,?,?,?,?,?,?,?)",
                (url, final_url, title, content_type, file_size, sha256, local_path, fetched_at),
            )
            await conn.commit()
            return int(cur.lastrowid)
        finally:
            await conn.close()

    async def add_fts_content(self, *, doc_id: int, url: str, title: str, content: str) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                "INSERT INTO fts_docs(doc_id,url,title,content) VALUES(?,?,?,?)",
                (doc_id, url, title, content),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def kv_get(self, key: str) -> str | None:
        conn = await self._connect()
        try:
            async with conn.execute("SELECT value FROM kv WHERE key=?", (key,)) as cur:
                row = await cur.fetchone()
                return (str(row[0]) if row else None)
        finally:
            await conn.close()

    async def kv_set(self, key: str, value: str) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                "INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def add_page_flags(
        self,
        *,
        doc_id: int,
        flags: list[dict[str, Any]],
        created_at: str,
    ) -> None:
        rows: list[tuple[int, int, str, float, str | None, str]] = []
        for f in flags:
            page_no = int(f.get("page_no") or 0)
            flag = str(f.get("flag") or "")
            score = float(f.get("score") or 0.0)
            details = f.get("details")
            details_json = json.dumps(details) if details is not None else None
            if page_no <= 0 or not flag:
                continue
            rows.append((doc_id, page_no, flag, score, details_json, created_at))
        if not rows:
            return
        conn = await self._connect()
        try:
            await conn.executemany(
                "INSERT INTO doc_page_flags(doc_id,page_no,flag,score,details_json,created_at) VALUES(?,?,?,?,?,?)",
                rows,
            )
            await conn.commit()
        finally:
            await conn.close()

    async def query_page_flags_for_doc(self, *, doc_id: int, flag: str | None = None) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            if flag:
                sql = (
                    "SELECT page_no,flag,score,details_json,created_at FROM doc_page_flags WHERE doc_id=? AND flag=? ORDER BY score DESC, page_no ASC"
                )
                params = (doc_id, flag)
            else:
                sql = (
                    "SELECT page_no,flag,score,details_json,created_at FROM doc_page_flags WHERE doc_id=? ORDER BY score DESC, page_no ASC"
                )
                params = (doc_id,)
            async with conn.execute(sql, params) as cur:
                rows = await cur.fetchall()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "page_no": int(r[0]),
                            "flag": r[1],
                            "score": float(r[2]),
                            "details": json.loads(r[3]) if r[3] else None,
                            "created_at": r[4],
                        }
                    )
                return out
        finally:
            await conn.close()

    async def set_review_status(self, *, doc_id: int, status: str, updated_at: str) -> None:
        st = (status or "new").strip().lower()
        if st not in {"new", "reviewed", "ignored", "irrelevant", "high_value"}:
            st = "new"
        conn = await self._connect()
        try:
            if st == "new":
                await conn.execute("DELETE FROM doc_reviews WHERE doc_id=?", (doc_id,))
            else:
                await conn.execute(
                    "INSERT INTO doc_reviews(doc_id,status,updated_at) VALUES(?,?,?) "
                    "ON CONFLICT(doc_id) DO UPDATE SET status=excluded.status, updated_at=excluded.updated_at",
                    (doc_id, st, updated_at),
                )
            await conn.commit()
        finally:
            await conn.close()

    async def get_review_status(self, *, doc_id: int) -> str:
        conn = await self._connect()
        try:
            async with conn.execute("SELECT status FROM doc_reviews WHERE doc_id=?", (doc_id,)) as cur:
                row = await cur.fetchone()
                return str(row[0]) if row and row[0] else "new"
        finally:
            await conn.close()

    async def get_document(self, *, doc_id: int) -> dict[str, Any]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT id,url,final_url,title,content_type,file_size,sha256,local_path,fetched_at,relevance_score,topic_similarity,entity_density,url_penalty "
                "FROM documents WHERE id=?",
                (int(doc_id),),
            ) as cur:
                r = await cur.fetchone()
                if not r:
                    return {}
                return {
                    "doc_id": int(r[0]),
                    "url": r[1],
                    "final_url": r[2],
                    "title": r[3] or "",
                    "content_type": r[4] or "",
                    "file_size": (int(r[5]) if r[5] is not None else None),
                    "sha256": r[6] or "",
                    "local_path": r[7] or "",
                    "fetched_at": r[8] or "",
                    "relevance_score": (float(r[9]) if r[9] is not None else None),
                    "topic_similarity": (float(r[10]) if r[10] is not None else None),
                    "entity_density": (float(r[11]) if r[11] is not None else None),
                    "url_penalty": (float(r[12]) if r[12] is not None else None),
                }
        finally:
            await conn.close()

    async def update_document_metrics(
        self,
        *,
        doc_id: int,
        relevance_score: float | None,
        topic_similarity: float | None,
        entity_density: float | None,
        url_penalty: float | None,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                "UPDATE documents SET relevance_score=?, topic_similarity=?, entity_density=?, url_penalty=? WHERE id=?",
                (
                    relevance_score,
                    topic_similarity,
                    entity_density,
                    url_penalty,
                    int(doc_id),
                ),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def update_paths_for_sha256(self, *, sha256: str, local_path: str) -> None:
        sha = (sha256 or "").strip().lower()
        if not sha:
            return
        conn = await self._connect()
        try:
            await conn.execute("UPDATE documents SET local_path=? WHERE sha256=?", (local_path, sha))
            await conn.execute("UPDATE urls SET local_path=? WHERE sha256=?", (local_path, sha))
            await conn.commit()
        finally:
            await conn.close()

    async def query_flagged_with_metrics(self, *, limit: int = 5000) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT d.id,d.url,d.title,d.local_path,d.fetched_at,COUNT(m.id) AS match_count,"
                "d.relevance_score,d.topic_similarity,d.entity_density,d.url_penalty,COALESCE(r.status,'new') as review_status "
                "FROM documents d JOIN matches m ON m.doc_id=d.id "
                "LEFT JOIN doc_reviews r ON r.doc_id=d.id "
                "GROUP BY d.id ORDER BY d.fetched_at DESC LIMIT ?",
                (int(limit),),
            ) as cur:
                rows = await cur.fetchall()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "doc_id": int(r[0]),
                            "url": r[1],
                            "title": r[2] or "",
                            "local_path": r[3] or "",
                            "fetched_at": r[4] or "",
                            "match_count": int(r[5]),
                            "relevance_score": (float(r[6]) if r[6] is not None else None),
                            "topic_similarity": (float(r[7]) if r[7] is not None else None),
                            "entity_density": (float(r[8]) if r[8] is not None else None),
                            "url_penalty": (float(r[9]) if r[9] is not None else None),
                            "review_status": str(r[10]) if r[10] else "new",
                        }
                    )
                return out
        finally:
            await conn.close()

    async def get_feedback_centroid(self, *, label: str, model_name: str):
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT vector,norm,count FROM feedback_centroids WHERE label=? AND model_name=?",
                (str(label), str(model_name)),
            ) as cur:
                r = await cur.fetchone()
                if not r:
                    return None
                from doj_disclosures.core.embeddings import blob_to_vector
                from doj_disclosures.core.feedback import Centroid

                return Centroid(vec=blob_to_vector(bytes(r[0])), norm=float(r[1]), count=int(r[2]))
        finally:
            await conn.close()

    async def set_feedback_centroid(self, *, label: str, model_name: str, centroid) -> None:
        # centroid: doj_disclosures.core.feedback.Centroid
        from doj_disclosures.core.embeddings import vector_to_blob

        blob, norm = vector_to_blob(list(centroid.vec))
        conn = await self._connect()
        try:
            await conn.execute(
                "INSERT INTO feedback_centroids(label,model_name,vector,norm,count,updated_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(label,model_name) DO UPDATE SET vector=excluded.vector,norm=excluded.norm,count=excluded.count,updated_at=excluded.updated_at",
                (str(label), str(model_name), blob, float(norm), int(centroid.count), datetime.now(timezone.utc).isoformat()),
            )
            await conn.commit()
        finally:
            await conn.close()

    async def get_review_status_map(self, *, doc_ids: list[int]) -> dict[int, str]:
        ids = [int(x) for x in doc_ids if int(x) > 0]
        if not ids:
            return {}
        conn = await self._connect()
        try:
            ph = ",".join(["?"] * len(ids))
            async with conn.execute(f"SELECT doc_id,status FROM doc_reviews WHERE doc_id IN ({ph})", tuple(ids)) as cur:
                rows = await cur.fetchall()
                return {int(r[0]): (str(r[1]) if r[1] else "new") for r in rows}
        finally:
            await conn.close()

    async def get_known_document_urls(self) -> list[str]:
        # Heuristic: known downloadable suffixes.
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT url FROM urls WHERE status <> 'abandoned' AND ("
                "lower(url) LIKE '%.pdf' OR lower(url) LIKE '%.doc' OR lower(url) LIKE '%.docx' OR lower(url) LIKE '%.txt' OR lower(url) LIKE '%.html' OR lower(url) LIKE '%.htm'"
                ")"
            ) as cur:
                rows = await cur.fetchall()
                return [str(r[0]) for r in rows if r and r[0]]
        finally:
            await conn.close()

    async def get_release_snapshot_rows(self) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT url,status,http_status,content_type,title,final_url,local_path,sha256,etag,last_modified,last_attempt_at,discovered_at "
                "FROM urls WHERE status <> 'abandoned'"
            ) as cur:
                rows = await cur.fetchall()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "url": r[0],
                            "status": r[1],
                            "http_status": int(r[2]) if r[2] is not None else None,
                            "content_type": r[3],
                            "title": r[4],
                            "final_url": r[5],
                            "local_path": r[6],
                            "sha256": r[7],
                            "etag": r[8],
                            "last_modified": r[9],
                            "last_attempt_at": r[10],
                            "discovered_at": r[11],
                        }
                    )
                return out
        finally:
            await conn.close()

    async def get_redaction_max_map(self, *, doc_ids: list[int]) -> dict[int, float]:
        ids = [int(x) for x in doc_ids if int(x) > 0]
        if not ids:
            return {}
        conn = await self._connect()
        try:
            ph = ",".join(["?"] * len(ids))
            async with conn.execute(
                f"SELECT doc_id, MAX(score) FROM doc_page_flags WHERE flag='redaction' AND doc_id IN ({ph}) GROUP BY doc_id",
                tuple(ids),
            ) as cur:
                rows = await cur.fetchall()
                return {int(r[0]): float(r[1]) for r in rows if r and r[1] is not None}
        finally:
            await conn.close()

    async def get_fts_content(self, *, doc_id: int) -> str | None:
        conn = await self._connect()
        try:
            async with conn.execute("SELECT content FROM fts_docs WHERE doc_id=?", (doc_id,)) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                return str(row[0]) if row[0] is not None else None
        finally:
            await conn.close()

    async def fts_search(self, *, query: str, limit: int = 200) -> list[dict[str, Any]]:
        q = (query or "").strip()
        if not q:
            return []
        conn = await self._connect()
        try:
            # FTS5: lower bm25() is better; we'll return it as-is.
            async with conn.execute(
                "SELECT doc_id, url, title, bm25(fts_docs) as bm25 FROM fts_docs WHERE fts_docs MATCH ? ORDER BY bm25 ASC LIMIT ?",
                (q, int(limit)),
            ) as cur:
                rows = await cur.fetchall()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "doc_id": int(r[0]),
                            "url": r[1],
                            "title": r[2] or "",
                            "bm25": float(r[3]) if r[3] is not None else 0.0,
                        }
                    )
                return out
        except Exception:
            # Defensive fallback: if MATCH query syntax is invalid, do a simple LIKE.
            like = f"%{q}%"
            async with conn.execute(
                "SELECT doc_id, url, title, 0.0 as bm25 FROM fts_docs WHERE title LIKE ? OR content LIKE ? LIMIT ?",
                (like, like, int(limit)),
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {"doc_id": int(r[0]), "url": r[1], "title": r[2] or "", "bm25": 0.0} for r in rows
                ]
        finally:
            await conn.close()

    async def add_matches(self, *, doc_id: int, matches: Iterable[tuple[str, str, float, str]], created_at: str) -> None:
        conn = await self._connect()
        try:
            await conn.executemany(
                "INSERT INTO matches(doc_id,method,pattern,score,snippet,created_at) VALUES(?,?,?,?,?,?)",
                [(doc_id, m, p, s, sn, created_at) for (m, p, s, sn) in matches],
            )
            await conn.commit()
        finally:
            await conn.close()

    async def add_tables(
        self,
        *,
        doc_id: int,
        tables: Iterable[dict[str, Any]],
        created_at: str,
    ) -> None:
        rows: list[tuple[int, int, int, str, str, str | None, str]] = []
        for t in tables:
            page_no = int(t.get("page_no") or 0)
            table_index = int(t.get("table_index") or 0)
            fmt = str(t.get("format") or "rows")
            data_json = json.dumps(t.get("data") or [])
            bbox = t.get("bbox")
            bbox_json = json.dumps(bbox) if bbox is not None else None
            rows.append((doc_id, page_no, table_index, fmt, data_json, bbox_json, created_at))
        if not rows:
            return

        conn = await self._connect()
        try:
            await conn.executemany(
                "INSERT INTO doc_tables(doc_id,page_no,table_index,format,data_json,bbox_json,created_at) VALUES(?,?,?,?,?,?,?)",
                rows,
            )
            await conn.commit()
        finally:
            await conn.close()

    async def query_tables_for_doc(self, doc_id: int) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT page_no,table_index,format,data_json,bbox_json,created_at FROM doc_tables WHERE doc_id=? ORDER BY page_no ASC, table_index ASC",
                (doc_id,),
            ) as cur:
                rows = await cur.fetchall()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "page_no": int(r[0]),
                            "table_index": int(r[1]),
                            "format": r[2],
                            "data": json.loads(r[3]) if r[3] else [],
                            "bbox": (json.loads(r[4]) if r[4] else None),
                            "created_at": r[5],
                        }
                    )
                return out
        finally:
            await conn.close()

    async def add_entities(
        self,
        *,
        doc_id: int,
        entities: Iterable[dict[str, Any]],
        created_at: str,
    ) -> None:
        rows: list[tuple[int, str, str, str, int, str, str | None, str]] = []
        for e in entities:
            label = str(e.get("label") or "")
            canonical = str(e.get("canonical") or "")
            display = str(e.get("display") or canonical)
            count = int(e.get("count") or 1)
            variants_json = json.dumps(sorted(set(e.get("variants") or [display])))
            page_nos = e.get("page_nos")
            page_nos_json = json.dumps(sorted(set(int(x) for x in page_nos))) if page_nos else None
            if not (label and canonical):
                continue
            rows.append((doc_id, label, canonical, display, count, variants_json, page_nos_json, created_at))

        if not rows:
            return

        conn = await self._connect()
        try:
            await conn.executemany(
                "INSERT INTO doc_entities(doc_id,label,canonical,display,count,variants_json,page_nos_json,created_at) "
                "VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(doc_id,label,canonical) DO UPDATE SET "
                "display=excluded.display, count=excluded.count, variants_json=excluded.variants_json, page_nos_json=excluded.page_nos_json, created_at=excluded.created_at",
                rows,
            )
            await conn.commit()
        finally:
            await conn.close()

    async def add_embeddings(
        self,
        *,
        doc_id: int,
        embeddings: Iterable[dict[str, Any]],
        created_at: str,
    ) -> None:
        rows: list[tuple[int, int, int | None, int | None, str, bytes, float, str]] = []
        for e in embeddings:
            chunk_index = int(e.get("chunk_index") or 0)
            start_offset = e.get("start_offset")
            end_offset = e.get("end_offset")
            model_name = str(e.get("model_name") or "")
            vector = e.get("vector")
            norm = float(e.get("norm") or 0.0)
            if not model_name or not isinstance(vector, (bytes, bytearray)):
                continue
            rows.append(
                (
                    doc_id,
                    chunk_index,
                    int(start_offset) if start_offset is not None else None,
                    int(end_offset) if end_offset is not None else None,
                    model_name,
                    bytes(vector),
                    norm,
                    created_at,
                )
            )
        if not rows:
            return
        conn = await self._connect()
        try:
            await conn.executemany(
                "INSERT INTO doc_embeddings(doc_id,chunk_index,start_offset,end_offset,model_name,vector,norm,created_at) "
                "VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(doc_id, model_name, chunk_index) DO UPDATE SET "
                "start_offset=excluded.start_offset, end_offset=excluded.end_offset, vector=excluded.vector, norm=excluded.norm, created_at=excluded.created_at",
                rows,
            )
            await conn.commit()
        finally:
            await conn.close()

    async def query_embeddings_for_doc(self, *, doc_id: int, model_name: str) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT chunk_index,start_offset,end_offset,vector,norm FROM doc_embeddings WHERE doc_id=? AND model_name=? ORDER BY chunk_index ASC",
                (doc_id, model_name),
            ) as cur:
                rows = await cur.fetchall()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "chunk_index": int(r[0]),
                            "start_offset": (int(r[1]) if r[1] is not None else None),
                            "end_offset": (int(r[2]) if r[2] is not None else None),
                            "vector": bytes(r[3]),
                            "norm": float(r[4]) if r[4] is not None else 0.0,
                        }
                    )
                return out
        finally:
            await conn.close()

    async def query_entities_for_doc(self, doc_id: int) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT label,canonical,display,count,variants_json,page_nos_json,created_at "
                "FROM doc_entities WHERE doc_id=? ORDER BY label ASC, count DESC, display ASC",
                (doc_id,),
            ) as cur:
                rows = await cur.fetchall()
                out: list[dict[str, Any]] = []
                for r in rows:
                    out.append(
                        {
                            "label": r[0],
                            "canonical": r[1],
                            "display": r[2],
                            "count": int(r[3]),
                            "variants": json.loads(r[4]) if r[4] else [],
                            "page_nos": json.loads(r[5]) if r[5] else [],
                            "created_at": r[6],
                        }
                    )
                return out
        finally:
            await conn.close()

    async def query_flagged(self, limit: int = 500) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT d.id,d.url,d.title,d.local_path,d.fetched_at,COUNT(m.id) AS match_count "
                "FROM documents d JOIN matches m ON m.doc_id=d.id "
                "GROUP BY d.id ORDER BY d.fetched_at DESC LIMIT ?",
                (limit,),
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {
                        "doc_id": int(r[0]),
                        "url": r[1],
                        "title": r[2] or "",
                        "local_path": r[3],
                        "fetched_at": r[4],
                        "match_count": int(r[5]),
                    }
                    for r in rows
                ]
        finally:
            await conn.close()

    async def query_matches_for_doc(self, doc_id: int) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            async with conn.execute(
                "SELECT method,pattern,score,snippet,created_at FROM matches WHERE doc_id=? ORDER BY score DESC",
                (doc_id,),
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {
                        "method": r[0],
                        "pattern": r[1],
                        "score": float(r[2]),
                        "snippet": r[3],
                        "created_at": r[4],
                    }
                    for r in rows
                ]
        finally:
            await conn.close()

    async def export_flagged_json(self, limit: int = 5000) -> list[dict[str, Any]]:
        docs = await self.query_flagged(limit=limit)
        out: list[dict[str, Any]] = []
        for d in docs:
            matches = await self.query_matches_for_doc(int(d["doc_id"]))
            tables = await self.query_tables_for_doc(int(d["doc_id"]))
            entities = await self.query_entities_for_doc(int(d["doc_id"]))
            redactions = await self.query_page_flags_for_doc(doc_id=int(d["doc_id"]), flag="redaction")
            review_status = await self.get_review_status(doc_id=int(d["doc_id"]))
            out.append(
                {
                    **d,
                    "matches": matches,
                    "tables": tables,
                    "entities": entities,
                    "redactions": redactions,
                    "review_status": review_status,
                }
            )
        return out

    async def clear_results(self) -> None:
        """Clear extracted text + match results.

        This removes:
        - `matches`
        - `documents`
        - `fts_docs`

        It does not delete files on disk.
        """

        conn = await self._connect()
        try:
            await conn.execute("DELETE FROM matches")
            await conn.execute("DELETE FROM documents")
            await conn.execute("DELETE FROM fts_docs")
            await conn.execute("DELETE FROM doc_tables")
            await conn.execute("DELETE FROM doc_entities")
            await conn.execute("DELETE FROM doc_embeddings")
            await conn.execute("DELETE FROM doc_page_flags")
            await conn.execute("DELETE FROM doc_reviews")
            await conn.commit()
        finally:
            await conn.close()

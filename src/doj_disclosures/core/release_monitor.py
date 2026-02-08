from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from doj_disclosures.core.db import Database


SNAPSHOT_KEY = "release_snapshot_v1"
LAST_DIFF_KEY = "release_last_diff_v1"


@dataclass(frozen=True)
class ReleaseDiff:
    created_at: str
    added: list[dict[str, Any]]
    removed: list[dict[str, Any]]
    changed: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at,
            "added": self.added,
            "removed": self.removed,
            "changed": self.changed,
        }


def _key_fields(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("sha256"),
        row.get("etag"),
        row.get("last_modified"),
        row.get("final_url"),
        row.get("content_type"),
        row.get("http_status"),
    )


def compute_release_diff(prev_rows: list[dict[str, Any]], cur_rows: list[dict[str, Any]]) -> ReleaseDiff:
    now = datetime.now(timezone.utc).isoformat()
    prev = {str(r.get("url")): r for r in prev_rows if r.get("url")}
    cur = {str(r.get("url")): r for r in cur_rows if r.get("url")}

    added: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []

    for url, r in cur.items():
        if url not in prev:
            added.append(r)
        else:
            if _key_fields(prev[url]) != _key_fields(r):
                changed.append({"url": url, "before": prev[url], "after": r})

    for url, r in prev.items():
        if url not in cur:
            removed.append(r)

    return ReleaseDiff(created_at=now, added=added, removed=removed, changed=changed)


async def load_previous_snapshot(db: Database) -> list[dict[str, Any]]:
    raw = await db.kv_get(SNAPSHOT_KEY)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


async def store_snapshot_and_diff(db: Database) -> ReleaseDiff:
    prev_rows = await load_previous_snapshot(db)
    cur_rows = await db.get_release_snapshot_rows()
    diff = compute_release_diff(prev_rows, cur_rows)

    await db.kv_set(LAST_DIFF_KEY, json.dumps(diff.to_dict()))
    await db.kv_set(SNAPSHOT_KEY, json.dumps(cur_rows))
    return diff


async def load_last_diff(db: Database) -> dict[str, Any] | None:
    raw = await db.kv_get(LAST_DIFF_KEY)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None

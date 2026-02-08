from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from doj_disclosures.core.embeddings import EmbeddingProvider, blob_to_vector, cosine_similarity, vector_to_blob
from doj_disclosures.core.relevance import hostname, load_url_penalties, dump_url_penalties
from doj_disclosures.core.storage_gating import compute_flagged_path, move_to, plan_storage

logger = logging.getLogger(__name__)


URL_PENALTIES_KEY = "url_penalties"
PHRASE_BLACKLIST_KEY = "phrase_blacklist"


@dataclass(frozen=True)
class Centroid:
    vec: list[float]
    norm: float
    count: int


def _update_centroid(old: Centroid | None, new_vec: list[float]) -> Centroid:
    if not new_vec:
        return old or Centroid(vec=[], norm=0.0, count=0)
    if old is None or old.count <= 0 or not old.vec:
        blob, norm = vector_to_blob(new_vec)
        return Centroid(vec=blob_to_vector(blob), norm=norm, count=1)

    # Online mean
    dim = min(len(old.vec), len(new_vec))
    count = int(old.count)
    avg = [(old.vec[i] * count + float(new_vec[i])) / (count + 1) for i in range(dim)]
    blob, norm = vector_to_blob(avg)
    return Centroid(vec=blob_to_vector(blob), norm=norm, count=count + 1)


async def apply_feedback(
    *,
    db,
    doc_id: int,
    label: str,
    provider: EmbeddingProvider | None,
    model_name: str,
    output_dir: Path,
    storage_layout: str = "flat",
) -> None:
    """Apply human feedback.

    - label: "irrelevant" or "high_value"
    - Updates doc_reviews, URL penalties, phrase blacklist, and online centroids.

    This is intentionally lightweight (no heavy classifier dependency).
    """

    lb = (label or "").strip().lower()
    if lb not in {"irrelevant", "high_value"}:
        return

    now = datetime.now(timezone.utc).isoformat()
    await db.set_review_status(doc_id=doc_id, status=lb, updated_at=now)

    # Move file into the appropriate Flagged subfolder.
    try:
        doc = await db.get_document(doc_id=doc_id)
        local_path = str(doc.get("local_path") or "")
        sha = str(doc.get("sha256") or "")
        if local_path and sha:
            src = Path(local_path)
            if src.exists():
                storage = plan_storage(output_dir)
                bucket_dir = storage.flagged_dir / ("high_value" if lb == "high_value" else "irrelevant")
                title = str(doc.get("title") or "").strip()
                dst = compute_flagged_path(
                    flagged_dir=bucket_dir,
                    sha256=sha,
                    suffix=src.suffix,
                    storage_layout=storage_layout,
                    display_name=(title or src.stem),
                )
                try:
                    final = move_to(dst, src)
                    await db.update_paths_for_sha256(sha256=sha, local_path=str(final))
                except Exception:
                    pass
    except Exception:
        pass

    # URL penalties (per hostname)
    doc = await db.get_document(doc_id=doc_id)
    host = hostname(doc.get("url", ""))
    raw = await db.kv_get(URL_PENALTIES_KEY)
    penalties = load_url_penalties(raw)
    cur = float(penalties.get(host, 0.0) or 0.0)
    if host:
        if lb == "irrelevant":
            cur = min(0.60, cur + 0.05)
        else:
            cur = max(0.0, cur - 0.03)
        penalties[host] = float(cur)
        await db.kv_set(URL_PENALTIES_KEY, dump_url_penalties(penalties))

    # Phrase blacklist: only blacklist single-hit patterns.
    try:
        matches = await db.query_matches_for_doc(doc_id)
        if lb == "irrelevant" and len(matches) == 1:
            pat = str(matches[0].get("pattern") or "").strip()
            if pat:
                raw_bl = await db.kv_get(PHRASE_BLACKLIST_KEY)
                bl: list[str] = []
                try:
                    data = json.loads(raw_bl) if raw_bl else []
                    if isinstance(data, list):
                        bl = [str(x) for x in data if str(x).strip()]
                except Exception:
                    bl = []
                if pat not in bl:
                    bl.append(pat)
                    # cap size
                    bl = bl[-500:]
                    await db.kv_set(PHRASE_BLACKLIST_KEY, json.dumps(bl))
    except Exception:
        pass

    # Online centroid model update.
    if provider is None:
        return

    try:
        text = await db.get_fts_content(doc_id=doc_id) or ""
        if not text.strip():
            return
        vec = provider.embed([text[:12000]])[0]

        old = await db.get_feedback_centroid(label=lb, model_name=model_name)
        updated = _update_centroid(old, vec)
        await db.set_feedback_centroid(label=lb, model_name=model_name, centroid=updated)
    except Exception as e:
        logger.info("feedback centroid update skipped: %s", e)

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable

logger = logging.getLogger(__name__)


_PAGE_RE = re.compile(r"\[PAGE\s+(\d+)\]", flags=re.IGNORECASE)


@dataclass(frozen=True)
class EntityHit:
    label: str
    text: str
    start: int
    end: int
    page_no: int | None


def _page_no_for_offset(text: str, offset: int) -> int | None:
    # Find the nearest preceding [PAGE N] marker.
    last = None
    for m in _PAGE_RE.finditer(text):
        if m.start() > offset:
            break
        try:
            last = int(m.group(1))
        except Exception:
            last = None
    return last


def canonicalize_entity(text: str, *, label: str) -> str:
    t = unicodedata.normalize("NFKC", text).strip()
    t = re.sub(r"\s+", " ", t)
    t = t.strip(" \t\r\n\"'`.,;:()[]{}<>")

    low = t.lower()
    # Normalize common honorifics for people.
    if label.upper() == "PERSON":
        low = re.sub(r"^(mr|mrs|ms|miss|dr|prof|sir|madam)\.?\s+", "", low)
        low = re.sub(r"\s+", " ", low).strip()

    # For emails/urls/phones, keep minimal normalization.
    if label.upper() in {"EMAIL", "URL"}:
        return low

    if label.upper() in {"PHONE", "SSN"}:
        return re.sub(r"\D+", "", low)

    # Generic: drop repeated punctuation.
    low = re.sub(r"[^\w\s\-./@]", "", low)
    low = re.sub(r"\s+", " ", low).strip()
    return low


def _regex_entities(text: str) -> list[EntityHit]:
    hits: list[EntityHit] = []

    patterns: list[tuple[str, re.Pattern[str]]] = [
        ("EMAIL", re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)),
        ("URL", re.compile(r"\bhttps?://[^\s)\]}>'\"]+", re.IGNORECASE)),
        (
            "PHONE",
            re.compile(
                r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}(?!\d)"
            ),
        ),
        ("SSN", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ]

    for label, rx in patterns:
        for m in rx.finditer(text):
            page_no = _page_no_for_offset(text, m.start())
            hits.append(EntityHit(label=label, text=m.group(0), start=m.start(), end=m.end(), page_no=page_no))
    return hits


def _spacy_entities(text: str, model: str) -> list[EntityHit]:
    try:
        import spacy  # type: ignore

        try:
            nlp = spacy.load(model)
        except Exception as e:
            logger.warning("spaCy model load failed (%s): %s", model, e)
            return []

        doc = nlp(text)
        hits: list[EntityHit] = []
        for ent in doc.ents:
            label = ent.label_.upper()
            if not ent.text or not ent.text.strip():
                continue
            page_no = _page_no_for_offset(text, ent.start_char)
            hits.append(EntityHit(label=label, text=ent.text, start=ent.start_char, end=ent.end_char, page_no=page_no))
        return hits
    except Exception as e:
        logger.info("spaCy unavailable: %s", e)
        return []


def extract_entities(
    text: str,
    *,
    enabled: bool = True,
    engine: str = "spacy",
    spacy_model: str = "en_core_web_sm",
) -> list[dict[str, Any]]:
    """Extract entities and return a deduped, alias-merged list.

    Output entries:
    - label
    - canonical
    - display
    - count
    - variants
    - page_nos
    """

    if not enabled or not text.strip():
        return []

    engine = (engine or "spacy").strip().lower()

    hits: list[EntityHit] = []
    # Always run regex extraction (cheap and useful).
    hits.extend(_regex_entities(text))

    if engine == "spacy":
        hits.extend(_spacy_entities(text, spacy_model))
    elif engine == "regex":
        pass
    else:
        logger.warning("Unknown NER engine %r; falling back to regex only", engine)

    # Dedupe/alias merge by (label, canonical).
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for h in hits:
        label = (h.label or "").upper().strip()
        if not label:
            continue
        canon = canonicalize_entity(h.text, label=label)
        if not canon:
            continue

        key = (label, canon)
        entry = merged.get(key)
        if entry is None:
            entry = {
                "label": label,
                "canonical": canon,
                "display": h.text.strip(),
                "count": 0,
                "variants": set(),
                "page_nos": set(),
            }
            merged[key] = entry

        entry["count"] += 1
        entry["variants"].add(h.text.strip())
        if h.page_no is not None:
            entry["page_nos"].add(int(h.page_no))

        # Prefer the longest variant as display (often most informative).
        if len(h.text.strip()) > len(str(entry["display"])):
            entry["display"] = h.text.strip()

    out: list[dict[str, Any]] = []
    for e in merged.values():
        out.append(
            {
                "label": e["label"],
                "canonical": e["canonical"],
                "display": e["display"],
                "count": int(e["count"]),
                "variants": sorted(e["variants"]),
                "page_nos": sorted(e["page_nos"]),
            }
        )

    out.sort(key=lambda x: (x["label"], -x["count"], x["display"]))
    return out

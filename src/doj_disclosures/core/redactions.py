from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RedactionFinding:
    page_no: int
    score: float
    details: dict[str, Any]


def _text_redaction_score(page_text: str) -> float:
    t = (page_text or "").lower()
    if not t.strip():
        return 0.0

    score = 0.0
    if "redacted" in t or "redaction" in t:
        score += 0.25

    # Common black-block characters in extracted text.
    blocks = t.count("█") + t.count("■") + t.count("▮")
    if blocks >= 20:
        score += min(0.5, blocks / 400.0)

    # Repeated placeholder sequences.
    if t.count("[redacted]") >= 1:
        score += 0.3

    return min(1.0, score)


def _drawing_black_area_ratio(page: fitz.Page) -> tuple[float, int]:
    # Best-effort heuristic: count large dark filled shapes.
    page_area = float(page.rect.get_area()) if hasattr(page.rect, "get_area") else float(page.rect.width * page.rect.height)
    if page_area <= 0:
        return 0.0, 0

    black_area = 0.0
    big_rects = 0
    try:
        drawings = page.get_drawings()
    except Exception:
        return 0.0, 0

    for d in drawings or []:
        fill = d.get("fill")
        rect = d.get("rect")
        if fill is None or rect is None:
            continue

        try:
            # fill is often RGB floats 0..1
            if isinstance(fill, (tuple, list)) and len(fill) >= 3:
                darkness = float(fill[0]) + float(fill[1]) + float(fill[2])
            else:
                continue
            if darkness > 0.45:
                continue

            r = fitz.Rect(rect)
            area = float(r.get_area()) if hasattr(r, "get_area") else float(r.width * r.height)
            if area <= 0:
                continue
            # Consider only reasonably large filled boxes.
            if area >= page_area * 0.01:
                black_area += area
                big_rects += 1
        except Exception:
            continue

    return min(1.0, black_area / page_area), big_rects


def _dark_pixel_ratio(page: fitz.Page, *, dpi: int = 50) -> float:
    # Rasterize low DPI and count very dark pixels; useful for black-box redactions embedded as images.
    try:
        pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csGRAY, alpha=False)
        samples = pix.samples
        if not samples:
            return 0.0
        # samples are bytes 0..255.
        dark = 0
        total = len(samples)
        for b in samples:
            if b < 40:
                dark += 1
        return float(dark / max(1, total))
    except Exception:
        return 0.0


def analyze_pdf_redactions(path: Path, *, extracted_text: str | None = None) -> list[dict[str, Any]]:
    """Return per-page redaction findings.

    Output dicts include:
    - page_no
    - score
    - details

    This is heuristic and intended for triage, not certainty.
    """

    doc = fitz.open(str(path))
    try:
        # Optional per-page text slices if caller provides [PAGE N] markers.
        page_texts: dict[int, str] = {}
        if extracted_text:
            current = None
            buf: list[str] = []
            for line in extracted_text.splitlines():
                if line.strip().startswith("[PAGE ") and line.strip().endswith("]"):
                    if current is not None:
                        page_texts[current] = "\n".join(buf)
                    buf = []
                    try:
                        current = int(line.strip().split()[1].rstrip("]"))
                    except Exception:
                        current = None
                else:
                    buf.append(line)
            if current is not None:
                page_texts[current] = "\n".join(buf)

        findings: list[dict[str, Any]] = []
        for page_no, page in enumerate(doc, start=1):
            txt_score = _text_redaction_score(page_texts.get(page_no, ""))
            area_ratio, big_rects = _drawing_black_area_ratio(page)

            # Only use pixel ratio when extracted text is sparse; avoids flagging normal scanned pages.
            use_pixels = len((page_texts.get(page_no, "") or "").strip()) < 60
            px_ratio = _dark_pixel_ratio(page) if use_pixels else 0.0

            score = 0.0
            score = max(score, min(1.0, txt_score))
            score = max(score, min(1.0, area_ratio * 3.0))
            if use_pixels:
                score = max(score, min(1.0, max(0.0, (px_ratio - 0.25) * 2.0)))
            score = min(1.0, score)

            if score > 0.0:
                findings.append(
                    {
                        "page_no": page_no,
                        "score": float(score),
                        "details": {
                            "text_score": float(txt_score),
                            "black_area_ratio": float(area_ratio),
                            "big_black_rects": int(big_rects),
                            "dark_pixel_ratio": float(px_ratio),
                            "pixel_check_used": bool(use_pixels),
                        },
                    }
                )
        return findings
    finally:
        doc.close()

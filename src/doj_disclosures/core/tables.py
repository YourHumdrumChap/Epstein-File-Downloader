from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


def extract_tables_from_pdf(path: Path) -> list[dict[str, Any]]:
    """Extract tables from a PDF using PyMuPDF's built-in table finder.

    Returns a list of dicts:
    - page_no: 1-based page number
    - table_index: 0-based index within the page
    - format: "rows"
    - data: list[list[str]]
    - bbox: [x0,y0,x1,y1] if available

    If the installed PyMuPDF version does not support table finding, returns [].
    """

    doc = fitz.open(str(path))
    try:
        tables_out: list[dict[str, Any]] = []
        for page_no, page in enumerate(doc, start=1):
            if not hasattr(page, "find_tables"):
                return []
            try:
                finder = page.find_tables()  # type: ignore[attr-defined]
                page_tables = getattr(finder, "tables", None) or []
                for idx, tbl in enumerate(page_tables):
                    try:
                        data = tbl.extract()  # type: ignore[attr-defined]
                    except Exception:
                        data = []

                    bbox = getattr(tbl, "bbox", None)
                    if bbox is not None:
                        try:
                            bbox_json = [float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])]
                        except Exception:
                            bbox_json = None
                    else:
                        bbox_json = None

                    # Normalize cells to strings.
                    norm: list[list[str]] = []
                    if isinstance(data, list):
                        for row in data:
                            if not isinstance(row, list):
                                continue
                            norm.append(["" if c is None else str(c) for c in row])

                    if norm:
                        tables_out.append(
                            {
                                "page_no": page_no,
                                "table_index": idx,
                                "format": "rows",
                                "data": norm,
                                "bbox": bbox_json,
                            }
                        )
            except Exception as e:
                logger.debug("Table extraction failed on %s page %s: %s", path.name, page_no, e)
                continue
        return tables_out
    finally:
        doc.close()

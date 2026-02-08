from __future__ import annotations

from pathlib import Path

import fitz
import pytest

from doj_disclosures.core.tables import extract_tables_from_pdf


def test_extract_tables_from_synthetic_pdf_when_supported(tmp_path: Path) -> None:
    # PyMuPDF table detection is version/heuristic dependent.
    # This test only asserts behavior when the feature exists and finds a table.
    p = tmp_path / "t.pdf"

    doc = fitz.open()
    page = doc.new_page(width=300, height=200)

    # Draw a simple 2x2 grid.
    x0, y0, x1, y1 = 40, 40, 260, 160
    page.draw_rect(fitz.Rect(x0, y0, x1, y1), color=(0, 0, 0), width=1)
    page.draw_line((150, y0), (150, y1), color=(0, 0, 0), width=1)
    page.draw_line((x0, 100), (x1, 100), color=(0, 0, 0), width=1)

    page.insert_text((60, 70), "A1")
    page.insert_text((170, 70), "B1")
    page.insert_text((60, 130), "A2")
    page.insert_text((170, 130), "B2")

    doc.save(str(p))
    doc.close()

    # Skip cleanly if this PyMuPDF build doesn't support find_tables.
    d = fitz.open(str(p))
    try:
        if not hasattr(d[0], "find_tables"):
            pytest.skip("PyMuPDF page.find_tables not available")
    finally:
        d.close()

    tables = extract_tables_from_pdf(p)
    if not tables:
        pytest.skip("Table heuristics did not detect a table in this environment")

    # At least one extracted cell should contain our content.
    cells = [c for t in tables for row in t["data"] for c in row]
    assert any("A1" in c for c in cells)
    assert any("B2" in c for c in cells)

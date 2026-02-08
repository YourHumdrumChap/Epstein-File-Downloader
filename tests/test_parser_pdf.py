from __future__ import annotations

from pathlib import Path

import fitz

from doj_disclosures.core.parser import DocumentParser


def test_pdf_text_extraction(tmp_path: Path) -> None:
    p = tmp_path / "a.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Hello DOJ")
    doc.save(str(p))
    doc.close()

    parser = DocumentParser(ocr_enabled=False)
    parsed = parser.parse(p, "application/pdf")
    assert "[PAGE 1]" in parsed.text
    assert "Hello DOJ" in parsed.text

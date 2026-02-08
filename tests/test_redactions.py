from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.skipif(
    pytest.importorskip("fitz") is None,  # pragma: no cover
    reason="PyMuPDF (fitz) not installed",
)
def test_redaction_heuristic_flags_black_rect(tmp_path: Path) -> None:
    import fitz

    pdf_path = tmp_path / "redacted.pdf"
    doc = fitz.open()
    page = doc.new_page(width=600, height=800)
    # Draw a large black rectangle (typical redaction block)
    page.draw_rect(fitz.Rect(50, 100, 550, 200), fill=(0, 0, 0), color=(0, 0, 0))
    doc.save(str(pdf_path))
    doc.close()

    from doj_disclosures.core.redactions import analyze_pdf_redactions

    findings = analyze_pdf_redactions(pdf_path, extracted_text="")
    assert findings
    top = max(findings, key=lambda f: float(f.get("score") or 0.0))
    assert int(top.get("page_no") or 0) == 1
    assert float(top.get("score") or 0.0) >= 0.25

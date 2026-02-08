from __future__ import annotations

import pytest

from doj_disclosures.core.parser import DocumentParser


def test_ocr_preprocess_binarizes_when_pillow_available() -> None:
    PIL = pytest.importorskip("PIL")
    Image = PIL.Image

    # Simple synthetic grayscale gradient image.
    img = Image.new("L", (32, 32))
    for y in range(32):
        for x in range(32):
            img.putpixel((x, y), int((x / 31) * 255))

    p = DocumentParser(
        ocr_enabled=True,
        ocr_engine="tesseract",
        ocr_preprocess=True,
        ocr_median_filter=False,
        ocr_threshold=128,
    )
    out = p._preprocess_for_ocr(img)

    # Output should be binarized to only 0/255.
    px = list(out.getdata())
    assert set(px).issubset({0, 255})

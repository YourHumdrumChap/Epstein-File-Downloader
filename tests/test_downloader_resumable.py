from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import aiohttp
import pytest
from aioresponses import aioresponses
from yarl import URL

from doj_disclosures.core.config import CrawlSettings
from doj_disclosures.core.downloader import Downloader, NotModifiedError


@pytest.mark.asyncio
async def test_resumable_download(tmp_path: Path) -> None:
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings()

    url = "https://example.com/file.txt"
    body1 = b"hello "
    body2 = b"world"

    with aioresponses() as m:
        m.get(url, status=200, body=body1, headers={"Content-Type": "text/plain", "Content-Length": str(len(body1))})
        async with aiohttp.ClientSession() as s:
            d = Downloader(settings=settings, session=s, output_dir=tmp_path, pause_event=pause, stop_event=stop)
            r1 = await d.download(url, title="file.txt")
            assert r1.local_path.read_bytes().startswith(body1)

    part = tmp_path / ".parts" / "file.txt.part"
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(body1)
    with aioresponses() as m:
        m.get(url, status=206, body=body2, headers={"Content-Type": "text/plain", "Content-Length": str(len(body2))})
        async with aiohttp.ClientSession() as s:
            d = Downloader(settings=settings, session=s, output_dir=tmp_path, pause_event=pause, stop_event=stop)
            r2 = await d.download(url, title="file.txt")
            assert r2.local_path.read_bytes() == body1 + body2


@pytest.mark.asyncio
async def test_hashed_storage_layout_places_file_under_sha_prefix_dirs(tmp_path: Path) -> None:
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings(storage_layout="hashed")

    url = "https://example.com/file.txt"
    body = b"hello hashed"
    sha = hashlib.sha256(body).hexdigest()

    with aioresponses() as m:
        m.get(url, status=200, body=body, headers={"Content-Type": "text/plain", "Content-Length": str(len(body))})
        async with aiohttp.ClientSession() as s:
            d = Downloader(settings=settings, session=s, output_dir=tmp_path, pause_event=pause, stop_event=stop)
            r = await d.download(url, title="file.txt")
            assert r.sha256 == sha
            assert r.local_path.exists()
            assert r.local_path.read_bytes() == body
            # output/ab/cd/<sha>.txt
            rel = r.local_path.relative_to(tmp_path)
            assert rel.parts[0] == sha[:2]
            assert rel.parts[1] == sha[2:4]
            assert rel.name == f"{sha}.txt"


@pytest.mark.asyncio
async def test_age_verify_opt_in_retries_and_downloads_pdf(tmp_path: Path) -> None:
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings(age_verify_opt_in=True)

    url = "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000024.pdf"
    html = b"<!doctype html><html><head><link rel=\"canonical\" href=\"https://www.justice.gov/age-verify\" /></head><body>Are you 18 years of age or older?</body></html>"
    pdf = b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF"

    with aioresponses() as m:
        m.get(url, status=200, body=html, headers={"Content-Type": "text/html; charset=UTF-8"})
        m.get(url, status=200, body=pdf, headers={"Content-Type": "application/pdf"})
        async with aiohttp.ClientSession() as s:
            d = Downloader(settings=settings, session=s, output_dir=tmp_path, pause_event=pause, stop_event=stop)
            r = await d.download(url, title="EFTA00000024.pdf")
            assert r.local_path.read_bytes().startswith(b"%PDF-")
        # Verify we attempted twice (age gate then PDF)
        assert len(m.requests[("GET", URL(url))]) == 2


@pytest.mark.asyncio
async def test_age_verify_without_opt_in_saves_diagnostic_html(tmp_path: Path) -> None:
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings(age_verify_opt_in=False, max_retries=0)

    url = "https://www.justice.gov/epstein/files/DataSet%201/EFTA00000024.pdf"
    html = b"<!doctype html><html><head><link rel=\"canonical\" href=\"https://www.justice.gov/age-verify\" /></head><body>Are you 18 years of age or older?</body></html>"

    with aioresponses() as m:
        m.get(url, status=200, body=html, headers={"Content-Type": "text/html; charset=UTF-8"})
        async with aiohttp.ClientSession() as s:
            d = Downloader(settings=settings, session=s, output_dir=tmp_path, pause_event=pause, stop_event=stop)
            with pytest.raises(RuntimeError, match=r"Age Verification"):
                await d.download(url, title="EFTA00000024.pdf")

    diag = tmp_path / "EFTA00000024.pdf.not_pdf.html"
    assert diag.exists()
    assert b"age-verify" in diag.read_bytes().lower()


@pytest.mark.asyncio
async def test_conditional_get_304_not_modified(tmp_path: Path) -> None:
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings(max_retries=0)

    url = "https://example.com/file.pdf"
    with aioresponses() as m:
        m.get(url, status=304, body=b"", headers={"Content-Type": "application/pdf"})
        async with aiohttp.ClientSession() as s:
            d = Downloader(settings=settings, session=s, output_dir=tmp_path, pause_event=pause, stop_event=stop)
            with pytest.raises(NotModifiedError):
                await d.download(url, title="file.pdf", cache_headers={"If-None-Match": '"abc"'})

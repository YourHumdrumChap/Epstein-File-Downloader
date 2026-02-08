from __future__ import annotations

import asyncio
from pathlib import Path

import aiohttp
import pytest
from aioresponses import aioresponses

from doj_disclosures.core.config import CrawlSettings
from doj_disclosures.core.crawler import Crawler
from doj_disclosures.core.db import Database
from doj_disclosures.core.downloader import Downloader
from doj_disclosures.core.matching import KeywordMatcher
from doj_disclosures.core.parser import DocumentParser


@pytest.mark.asyncio
async def test_end_to_end_mocked(tmp_path: Path, tmp_db_path: Path) -> None:
    db = Database(tmp_db_path)
    db.initialize_sync()
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings(start_url="https://example.com/start", allow_offsite=False, max_concurrency=2)

    html = "<html><head><title>T</title></head><body><a href='/doc.txt'>doc</a></body></html>"
    doc = b"This is a flight log."

    with aioresponses() as m:
        m.get("https://example.com/robots.txt", status=200, body="User-agent: *\nDisallow:\n")
        m.get("https://example.com/start", status=200, body=html, headers={"Content-Type": "text/html"})
        m.get("https://example.com/doc.txt", status=200, body=doc, headers={"Content-Type": "text/plain"})

        async with aiohttp.ClientSession() as session:
            crawler = Crawler(db=db, settings=settings, session=session, pause_event=pause, stop_event=stop)
            await crawler.initialize()
            await crawler.process_page("https://example.com/start")

            downloader = Downloader(settings=settings, session=session, output_dir=tmp_path, pause_event=pause, stop_event=stop)
            dl = await downloader.download("https://example.com/doc.txt", title="doc.txt")
            parser = DocumentParser(ocr_enabled=False)
            parsed = parser.parse(dl.local_path, dl.content_type)
            matcher = KeywordMatcher(keywords=["flight log"], fuzzy_enabled=False)
            hits = matcher.match(parsed.text)
            assert hits

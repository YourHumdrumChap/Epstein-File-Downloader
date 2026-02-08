from __future__ import annotations

import asyncio

import aiohttp
import pytest
from aioresponses import aioresponses

from doj_disclosures.core.config import CrawlSettings
from doj_disclosures.core.crawler import Crawler
from doj_disclosures.core.db import Database


@pytest.mark.asyncio
async def test_crawler_discovers_links(tmp_db_path) -> None:
    db = Database(tmp_db_path)
    db.initialize_sync()
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings(start_url="https://example.com/start", allow_offsite=False, follow_discovered_pages=False)

    with aioresponses() as m:
        m.get("https://example.com/robots.txt", status=200, body="User-agent: *\nDisallow:\n")
        m.get(
            "https://example.com/start",
            status=200,
            body=(
                "<html><head><title>T</title></head><body>"
                "<a href='/a.pdf'>pdf</a>"
                "<a href='/page'>p</a>"
                "<a href='/start?page=1'>next</a>"
                "</body></html>"
            ),
            headers={"Content-Type": "text/html"},
        )

        async with aiohttp.ClientSession() as session:
            c = Crawler(db=db, settings=settings, session=session, pause_event=pause, stop_event=stop)
            await c.initialize()
            links = await c.process_page("https://example.com/start")
            assert "https://example.com/a.pdf" in links
            pending = await db.get_pending_urls(limit=10)
            assert any(u == "https://example.com/a.pdf" for u, _ in pending)
            assert not any(u == "https://example.com/page" for u, _ in pending)
            assert any(u == "https://example.com/start?page=1" for u, _ in pending)


@pytest.mark.asyncio
async def test_crawler_discovers_links_recursive_mode(tmp_db_path) -> None:
    db = Database(tmp_db_path)
    db.initialize_sync()
    pause = asyncio.Event(); pause.set()
    stop = asyncio.Event()
    settings = CrawlSettings(start_url="https://example.com/start", allow_offsite=False, follow_discovered_pages=True)

    with aioresponses() as m:
        m.get("https://example.com/robots.txt", status=200, body="User-agent: *\nDisallow:\n")
        m.get(
            "https://example.com/start",
            status=200,
            body="<html><head><title>T</title></head><body><a href='/a.pdf'>pdf</a><a href='/start/page'>p</a></body></html>",
            headers={"Content-Type": "text/html"},
        )

        async with aiohttp.ClientSession() as session:
            c = Crawler(db=db, settings=settings, session=session, pause_event=pause, stop_event=stop)
            await c.initialize()
            await c.process_page("https://example.com/start")
            pending = await db.get_pending_urls(limit=10)
            assert any(u == "https://example.com/a.pdf" for u, _ in pending)
            assert any(u == "https://example.com/start/page" for u, _ in pending)

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup

from doj_disclosures.core.config import CrawlSettings
from doj_disclosures.core.db import Database
from doj_disclosures.core.robots import RobotsPolicy, fetch_robots
from doj_disclosures.core.utils import async_backoff_sleep, is_same_site, normalize_url
from doj_disclosures.core.browser_fetch import fetch_html_with_playwright

logger = logging.getLogger(__name__)


DOWNLOAD_EXTS = {".pdf", ".doc", ".docx", ".txt", ".html", ".htm"}


def looks_downloadable(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").lower()
    return any(path.endswith(ext) for ext in DOWNLOAD_EXTS)


@dataclass
class CrawlItem:
    url: str
    kind: str  # "page" or "document"


class Crawler:
    def __init__(
        self,
        *,
        db: Database,
        settings: CrawlSettings,
        session: aiohttp.ClientSession,
        pause_event: asyncio.Event,
        stop_event: asyncio.Event,
    ) -> None:
        self._db = db
        self._settings = settings
        self._session = session
        self._pause = pause_event
        self._stop = stop_event
        # aiolimiter acquires 1 "token" per request by default.
        # If requests_per_second < 1, we must stretch the time period instead of using a
        # fractional max_rate, otherwise aiolimiter raises:
        # "Can't acquire more than the maximum capacity".
        rps = max(0.1, float(settings.requests_per_second))
        if rps >= 1.0:
            self._limiter = AsyncLimiter(max_rate=rps, time_period=1.0)
        else:
            self._limiter = AsyncLimiter(max_rate=1.0, time_period=1.0 / rps)
        self._robots: RobotsPolicy | None = None
        self._seed_urls: list[str] = []
        self._seed_path_prefixes: list[str] = []

    async def initialize(self, *, seed_urls: list[str] | None = None) -> None:
        seeds_raw = [u.strip() for u in (seed_urls or [self._settings.start_url]) if u and u.strip()]
        if not seeds_raw:
            seeds_raw = [self._settings.start_url]

        # Workaround: DOJ dataset listing pages served from cache may be accessible at page=0,
        # while page>=1 may be blocked by Akamai for non-browser clients.
        seeds = [self._normalize_dataset_seed(u) for u in seeds_raw]

        self._seed_urls = seeds
        self._seed_path_prefixes = []
        for s in seeds:
            p = urlparse(s)
            path = p.path or "/"
            exact = path.rstrip("/") or "/"
            prefix = exact if exact.endswith("/") else (exact + "/")
            # Store both the exact path (for the seed itself) and a directory-like prefix.
            self._seed_path_prefixes.append(exact)
            self._seed_path_prefixes.append(prefix)
        self._robots = await fetch_robots(self._session, seeds[0], self._settings.user_agent)
        now = datetime.now(timezone.utc).isoformat()
        # Seed URLs should always be re-queued for a new run, even if they were previously "done".
        await self._db.upsert_urls(urls=seeds, status="queued", discovered_at=now, preserve_done=False)

    @staticmethod
    def _normalize_dataset_seed(url: str) -> str:
        """Rewrite dataset listing seeds from page>=1 to page=0.

        This avoids immediate 403 loops when the user pastes ?page=1 links.
        """

        try:
            p = urlparse(url)
            if "/epstein/doj-disclosures/data-set-" not in (p.path or ""):
                return url
            if "-files" not in (p.path or ""):
                return url
            qs = parse_qs(p.query or "", keep_blank_values=True)
            page_vals = qs.get("page")
            if not page_vals:
                return url
            try:
                page_int = int(str(page_vals[0]))
            except Exception:
                return url
            if page_int <= 0:
                return url
            qs["page"] = ["0"]
            new_query = urlencode(qs, doseq=True)
            return urlunparse(p._replace(query=new_query))
        except Exception:
            return url

    @staticmethod
    def _looks_like_akamai_access_denied(status: int, html: str) -> bool:
        if status != 403:
            return False
        t = (html or "").lower()
        if "access denied" in t and "errors.edgesuite.net" in t:
            return True
        return False

    def _is_allowed_site(self, url: str) -> bool:
        if self._settings.allow_offsite:
            return True
        bases = self._seed_urls or [self._settings.start_url]
        return any(is_same_site(url, b) for b in bases)

    def _page_in_scope(self, url: str) -> bool:
        # Documents can be hosted elsewhere on the same site (e.g., /sites/default/files/...)
        # but page crawling is restricted to the seed path prefix to prevent site-wide discovery.
        if not self._seed_path_prefixes:
            return True
        path = urlparse(url).path or "/"
        for pref in self._seed_path_prefixes:
            if pref == "/":
                return True
            if path == pref:
                return True
            if pref.endswith("/") and path.startswith(pref):
                return True
        return False

    @staticmethod
    def _looks_like_pagination(url: str) -> bool:
        # Heuristic for Drupal-style pager links (common on justice.gov).
        u = url.lower()
        if "?page=" in u or "&page=" in u:
            return True
        if "?p=" in u or "&p=" in u:
            return True
        if "pager" in u:
            return True
        return False

    def _allowed(self, url: str) -> bool:
        if self._robots is None:
            return True
        return self._robots.can_fetch(self._settings.user_agent, url)

    async def _fetch_html(self, url: str) -> tuple[int, str, str]:
        headers = {
            "User-Agent": self._settings.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
        }
        cookie = str(getattr(self._settings, "cookie_header", "") or "").strip()
        if cookie:
            headers["Cookie"] = cookie
        async with self._limiter:
            async with self._session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=40), allow_redirects=True) as resp:
                text = await resp.text(errors="ignore")
                status = int(resp.status)
                final_url = str(resp.url)

                if bool(getattr(self._settings, "use_browser_for_blocked_pages", False)) and (
                    self._looks_like_akamai_access_denied(status, text) or status == 403
                ):
                    # Fallback to a real browser for protected pages.
                    try:
                        res = await fetch_html_with_playwright(final_url, user_agent=self._settings.user_agent)
                        return 200, res.final_url, res.html
                    except Exception:
                        # If Playwright isn't usable (missing browser binaries, etc.), keep the original status.
                        pass

                return status, final_url, text

    async def iter_discovered(self) -> AsyncIterator[CrawlItem]:
        while not self._stop.is_set():
            await self._pause.wait()
            pending = await self._db.get_pending_urls(limit=400)
            if not pending:
                await asyncio.sleep(0.5)
                continue
            for url, _ct in pending:
                if self._stop.is_set():
                    break
                await self._pause.wait()
                yield CrawlItem(url=url, kind="document" if looks_downloadable(url) else "page")

    async def process_page(self, url: str) -> list[str]:
        if not self._is_allowed_site(url):
            return []
        if not self._allowed(url):
            return []

        now = datetime.now(timezone.utc).isoformat()
        await self._db.update_url_attempt(url=url, status="processing", last_attempt_at=now, http_status=None, error=None)

        attempts = 0
        while attempts <= self._settings.max_retries and not self._stop.is_set():
            try:
                status, final_url, html = await self._fetch_html(url)
                if status >= 400:
                    raise RuntimeError(f"HTTP {status}")

                soup = BeautifulSoup(html, "lxml")
                title = (soup.title.text.strip() if soup.title and soup.title.text else "")
                await self._db.update_url_attempt(
                    url=url,
                    status="done",
                    last_attempt_at=now,
                    http_status=status,
                    error=None,
                    content_type="text/html",
                    title=title,
                    final_url=final_url,
                )

                links: list[str] = []
                for a in soup.find_all("a"):
                    href = a.get("href")
                    if not href:
                        continue
                    candidate = normalize_url(href, base=final_url)
                    if candidate.startswith("mailto:") or candidate.startswith("javascript:"):
                        continue
                    if not self._is_allowed_site(candidate):
                        continue
                    if not self._allowed(candidate):
                        continue
                    links.append(candidate)

                unique_links = set(links)
                doc_links = [link for link in unique_links if looks_downloadable(link)]
                if doc_links:
                    await self._db.upsert_urls(urls=doc_links, status="queued", discovered_at=now, preserve_done=True)

                # Page crawling is scope-limited. Even in seed-only mode we still follow pagination links
                # so we can reach all documents on multi-page listings.
                page_links_all = [
                    link
                    for link in unique_links
                    if (not looks_downloadable(link)) and self._page_in_scope(link)
                ]

                if self._settings.follow_discovered_pages:
                    page_links = page_links_all
                else:
                    page_links = [link for link in page_links_all if self._looks_like_pagination(link)]

                if page_links:
                    # Always allow re-visiting pages on a new run (pages are lightweight and may change).
                    await self._db.upsert_urls(urls=page_links, status="queued", discovered_at=now, preserve_done=False)
                return links
            except Exception as e:
                attempts += 1
                if attempts > self._settings.max_retries:
                    await self._db.update_url_attempt(
                        url=url,
                        status="error",
                        last_attempt_at=now,
                        http_status=None,
                        error=str(e),
                    )
                    return []
                await self._db.update_url_attempt(
                    url=url,
                    status="retry",
                    last_attempt_at=now,
                    http_status=None,
                    error=str(e),
                )
                await async_backoff_sleep(attempts, self._settings.backoff_base_seconds)

        return []

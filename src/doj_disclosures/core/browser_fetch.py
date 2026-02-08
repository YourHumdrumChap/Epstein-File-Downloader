from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class BrowserFetchResult:
    final_url: str
    html: str


async def fetch_html_with_playwright(
    url: str,
    *,
    user_agent: str,
    timeout_seconds: float = 45.0,
) -> BrowserFetchResult:
    """Fetch fully-rendered HTML using Playwright.

    This is intended as a fallback for pages that block non-browser HTTP clients.
    Import is lazy so Playwright remains an optional dependency.
    """

    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Playwright is not installed. Install with: pip install playwright ; playwright install chromium"
        ) from e

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(user_agent=user_agent)
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=int(timeout_seconds * 1000))
            except Exception:
                # Some pages never reach networkidle; fall back to a shorter load wait.
                await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_seconds * 1000))
                await asyncio.sleep(1.0)

            final_url = page.url
            html = await page.content()
            return BrowserFetchResult(final_url=final_url, html=html)
        finally:
            await browser.close()

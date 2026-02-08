from __future__ import annotations

import aiohttp
import pytest
from aioresponses import aioresponses

from doj_disclosures.core.robots import fetch_robots


@pytest.mark.asyncio
async def test_fetch_robots() -> None:
    with aioresponses() as m:
        m.get("https://example.com/robots.txt", status=200, body="User-agent: *\nDisallow: /private\n")
        async with aiohttp.ClientSession() as s:
            pol = await fetch_robots(s, "https://example.com/start", "UA")
            assert pol.can_fetch("UA", "https://example.com/public")
            assert not pol.can_fetch("UA", "https://example.com/private/thing")

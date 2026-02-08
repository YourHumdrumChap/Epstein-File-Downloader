from __future__ import annotations

import logging
from dataclasses import dataclass
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class RobotsPolicy:
    parser: RobotFileParser

    def can_fetch(self, user_agent: str, url: str) -> bool:
        return self.parser.can_fetch(user_agent, url)


async def fetch_robots(session: aiohttp.ClientSession, start_url: str, user_agent: str) -> RobotsPolicy:
    parsed = urlparse(start_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots_url)
    try:
        async with session.get(robots_url, headers={"User-Agent": user_agent}, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status >= 400:
                logger.warning("robots.txt fetch failed: %s status=%s", robots_url, resp.status)
                parser.parse([])
                return RobotsPolicy(parser)
            body = await resp.text(errors="ignore")
            parser.parse(body.splitlines())
            return RobotsPolicy(parser)
    except Exception as e:
        logger.warning("robots.txt fetch error: %s (%s)", robots_url, e)
        parser.parse([])
        return RobotsPolicy(parser)

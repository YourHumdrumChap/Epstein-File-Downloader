from __future__ import annotations

import asyncio
import hashlib
import os
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
from urllib.parse import urljoin, urlparse, urlunparse


def normalize_url(url: str, base: str | None = None) -> str:
    if base:
        url = urljoin(base, url)
    parsed = urlparse(url)
    parsed = parsed._replace(fragment="")
    netloc = parsed.netloc.lower()
    scheme = parsed.scheme.lower() if parsed.scheme else "https"
    path = re.sub(r"//+", "/", parsed.path)
    return urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))


def is_same_site(url: str, start_url: str) -> bool:
    def canon(netloc: str) -> str:
        n = (netloc or "").lower().strip()
        if n.startswith("www."):
            n = n[4:]
        return n

    return canon(urlparse(url).netloc) == canon(urlparse(start_url).netloc)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def safe_filename(name: str, max_len: int = 160) -> str:
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    if not name:
        name = "file"
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name


async def async_backoff_sleep(attempt: int, base_seconds: float) -> None:
    delay = base_seconds * (2 ** max(0, attempt - 1))
    delay *= random.uniform(0.85, 1.15)
    await asyncio.sleep(min(delay, 30.0))


def chunk_text(text: str, max_chars: int = 4000, overlap: int = 200) -> Iterator[str]:
    if not text:
        return
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        yield text[start:end]
        if end == len(text):
            break
        start = max(0, end - overlap)


@dataclass(frozen=True)
class MatchSnippet:
    snippet: str
    start: int
    end: int


def snippet_around(text: str, start: int, end: int, context: int = 90) -> MatchSnippet:
    left = max(0, start - context)
    right = min(len(text), end + context)
    return MatchSnippet(snippet=text[left:right], start=left, end=right)


def atomic_rename(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src, dst)

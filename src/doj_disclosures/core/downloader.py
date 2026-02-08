from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from urllib.parse import urlparse

import aiohttp
from aiolimiter import AsyncLimiter
from yarl import URL

from doj_disclosures.core.config import CrawlSettings
from doj_disclosures.core.utils import async_backoff_sleep, atomic_rename, safe_filename

logger = logging.getLogger(__name__)


class NotModifiedError(RuntimeError):
    def __init__(self, url: str) -> None:
        super().__init__(f"Not modified: {url}")
        self.url = url


@dataclass(frozen=True)
class DownloadResult:
    url: str
    final_url: str
    local_path: Path
    content_type: str
    file_size: int | None
    sha256: str
    fetched_at: str
    etag: str | None = None
    last_modified: str | None = None


@dataclass
class _HostThrottleState:
    next_allowed_at: float = 0.0
    penalty: float = 1.0


class Downloader:
    def __init__(
        self,
        *,
        settings: CrawlSettings,
        session: aiohttp.ClientSession,
        output_dir: Path,
        pause_event: asyncio.Event,
        stop_event: asyncio.Event,
    ) -> None:
        self._settings = settings
        self._session = session
        self._output_dir = output_dir
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
        self._host_throttle: dict[str, _HostThrottleState] = {}
        self._host_throttle_lock = asyncio.Lock()

    async def _await_host_slot(self, url: str) -> None:
        """Adaptive per-host pacing.

        Keeps request spacing roughly at 1/rps per host, and backs off on 429/5xx.
        """

        base_rps = max(0.1, float(self._settings.requests_per_second))
        host = (urlparse(url).netloc or "").lower()
        if not host:
            return

        while True:
            await self._pause.wait()
            if self._stop.is_set():
                return

            async with self._host_throttle_lock:
                st = self._host_throttle.setdefault(host, _HostThrottleState())
                now = monotonic()
                if now >= st.next_allowed_at:
                    effective_rps = base_rps * max(0.2, min(1.0, st.penalty))
                    spacing = 1.0 / max(0.1, effective_rps)
                    st.next_allowed_at = now + spacing
                    return
                sleep_for = max(0.0, st.next_allowed_at - now)
            await asyncio.sleep(min(0.5, sleep_for))

    async def _note_host_result(self, url: str, *, http_status: int) -> None:
        host = (urlparse(url).netloc or "").lower()
        if not host:
            return
        async with self._host_throttle_lock:
            st = self._host_throttle.setdefault(host, _HostThrottleState())
            if http_status in {429, 502, 503}:
                st.penalty = max(0.2, st.penalty * 0.5)
                st.next_allowed_at = max(st.next_allowed_at, monotonic() + 2.0)
            elif 200 <= http_status < 400:
                st.penalty = min(1.0, st.penalty * 1.05)

    async def download(
        self,
        url: str,
        title: str | None = None,
        *,
        cache_headers: dict[str, str] | None = None,
    ) -> DownloadResult:
        headers = {
            "User-Agent": self._settings.user_agent,
            # Some servers vary response based on Accept; be explicit that we want documents.
            "Accept": "application/pdf,application/octet-stream,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document,text/plain,*/*",
        }
        cookie = str(getattr(self._settings, "cookie_header", "") or "").strip()
        if cookie:
            headers["Cookie"] = cookie
        if cache_headers:
            headers.update(cache_headers)
        attempts = 0
        last_exc: Exception | None = None

        while attempts <= self._settings.max_retries and not self._stop.is_set():
            await self._pause.wait()
            try:
                await self._await_host_slot(url)
                async with self._limiter:
                    return await self._download_once(url, headers=headers, title=title)
            except Exception as e:
                last_exc = e
                attempts += 1
                if attempts > self._settings.max_retries:
                    break
                await async_backoff_sleep(attempts, self._settings.backoff_base_seconds)

        assert last_exc is not None
        raise last_exc

    async def _download_once(self, url: str, headers: dict[str, str], title: str | None) -> DownloadResult:
        base_name = safe_filename(title or Path(url).name or "document")
        parts_dir = self._output_dir / ".parts"
        part_path = parts_dir / f"{base_name}.part"
        final_path = self._output_dir / base_name

        parsed = urlparse(url)
        expect_pdf_by_url = (parsed.path or "").lower().endswith(".pdf")

        # Optional justice.gov age gate: clicking "Yes" on /age-verify sets a short-lived cookie.
        # We only set this cookie if the user explicitly opts in.
        host = (parsed.netloc or "").lower()
        is_justice = host == "www.justice.gov" or host.endswith(".justice.gov")
        justice_base_url = URL(f"{parsed.scheme or 'https'}://{parsed.netloc or 'www.justice.gov'}/")
        cookie_name = "justiceGovAgeVerified"
        retry_after_age_verify = is_justice and bool(getattr(self._settings, "age_verify_opt_in", False))

        existing_size = part_path.stat().st_size if part_path.exists() else 0
        range_headers = dict(headers)
        if existing_size > 0:
            range_headers["Range"] = f"bytes={existing_size}-"

        # Conditional GET is only safe when requesting full content (no Range).
        if existing_size > 0:
            range_headers.pop("If-None-Match", None)
            range_headers.pop("If-Modified-Since", None)

        timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=60)
        async with self._session.get(url, headers=range_headers, timeout=timeout, allow_redirects=True) as resp:
            await self._note_host_result(url, http_status=resp.status)
            final_url = str(resp.url)

            if resp.status == 304:
                # Nothing changed. Caller can decide whether to skip parsing.
                raise NotModifiedError(url)
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}")

            if existing_size and resp.status == 200:
                try:
                    part_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass
                existing_size = 0

            content_type = resp.headers.get("Content-Type", "")
            expect_pdf = expect_pdf_by_url or ("pdf" in content_type.lower())
            content_length = resp.headers.get("Content-Length")
            file_size = int(content_length) + existing_size if content_length and content_length.isdigit() else None

            etag = resp.headers.get("ETag") or None
            last_modified = resp.headers.get("Last-Modified") or None

            digest = hashlib.sha256()
            if existing_size:
                with part_path.open("rb") as f:
                    while True:
                        chunk = f.read(1024 * 1024)
                        if not chunk:
                            break
                        digest.update(chunk)

            part_path.parent.mkdir(parents=True, exist_ok=True)
            first_bytes = b""
            with part_path.open("ab") as f:
                async for chunk in resp.content.iter_chunked(256 * 1024):
                    if self._stop.is_set():
                        break
                    await self._pause.wait()
                    if not chunk:
                        continue
                    if len(first_bytes) < 2048:
                        need = 2048 - len(first_bytes)
                        first_bytes += chunk[:need]
                    f.write(chunk)
                    digest.update(chunk)

            if self._stop.is_set():
                raise asyncio.CancelledError("Stopped")

            sha256 = digest.hexdigest()

            # Guardrail: justice.gov sometimes returns HTML (or an interstitial) for a .pdf URL.
            # If we write that to disk with a .pdf extension, PDF viewers will fail to load it.
            if expect_pdf:
                head = (first_bytes or b"")
                # Strip common whitespace; PDFs should start with %PDF-
                head_stripped = head.lstrip(b"\r\n\t ")
                looks_like_pdf = head_stripped.startswith(b"%PDF-")
                looks_like_html = b"<html" in head.lower() or b"<!doctype html" in head.lower()
                is_htmlish = ("text/html" in content_type.lower()) or looks_like_html
                # Heuristic for the justice.gov age gate interstitial.
                head_low = head.lower()
                looks_like_age_gate = (
                    b"/age-verify" in head_low
                    or b"are you 18" in head_low
                    or b"age verification" in head_low
                    or b"age-verify-block" in head_low
                    or "/age-verify" in final_url
                    or final_url.rstrip("/").endswith("/age-verify")
                )

                if is_htmlish and looks_like_age_gate and retry_after_age_verify:
                    # If cookie isn't present yet, set it and retry once.
                    existing = self._session.cookie_jar.filter_cookies(justice_base_url)
                    if cookie_name not in existing:
                        logger.warning(
                            "Age verification gate detected; setting %s cookie (opted in) and retrying once.",
                            cookie_name,
                        )
                        self._session.cookie_jar.update_cookies({cookie_name: "true"}, response_url=justice_base_url)
                        try:
                            part_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                        except Exception:
                            pass
                        # Retry without counting as a separate outer attempt.
                        return await self._download_once(url, headers=headers, title=title)

                if is_htmlish and looks_like_age_gate and (not retry_after_age_verify):
                    bad_path = self._output_dir / f"{final_path.name}.not_pdf.html"
                    try:
                        atomic_rename(part_path, bad_path)
                    except Exception:
                        pass
                    raise RuntimeError(
                        "justice.gov returned an Age Verification page instead of a PDF. "
                        "Enable Settings → 'I am 18+; allow age-gated downloads (justice.gov)' and retry. "
                        f"(content_type={content_type!r}, http_status={resp.status}, saved={bad_path.name!r})."
                    )

                if is_htmlish or (not looks_like_pdf):
                    bad_path = self._output_dir / f"{final_path.name}.not_pdf.html"
                    try:
                        atomic_rename(part_path, bad_path)
                    except Exception:
                        # Best effort: keep whatever we downloaded for diagnosis.
                        pass
                    raise RuntimeError(
                        "Expected PDF but received non-PDF content "
                        f"(content_type={content_type!r}, http_status={resp.status}, saved={bad_path.name!r})."
                    )

            # Decide extension after we know content-type.
            if final_path.suffix.lower() == "" and "pdf" in content_type.lower():
                final_path = final_path.with_suffix(".pdf")

            storage_layout = str(getattr(self._settings, "storage_layout", "flat") or "flat").lower().strip()
            if storage_layout == "hashed":
                # Store as output/ab/cd/<sha256>.<ext> to avoid huge single directories.
                subdir = self._output_dir / sha256[:2] / sha256[2:4]
                subdir.mkdir(parents=True, exist_ok=True)
                hashed_path = subdir / f"{sha256}{final_path.suffix}"
                # If already present (same content), discard the part file.
                if hashed_path.exists():
                    try:
                        part_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass
                    final_path = hashed_path
                else:
                    atomic_rename(part_path, hashed_path)
                    final_path = hashed_path
            else:
                # Flat (legacy) layout.
                if final_path.exists():
                    final_path = self._output_dir / f"{final_path.stem}-{sha256[:8]}{final_path.suffix}"
                atomic_rename(part_path, final_path)

            # Best-effort cleanup for any leftover .parts dir.
            try:
                if parts_dir.exists() and not any(parts_dir.iterdir()):
                    parts_dir.rmdir()
            except Exception:
                pass
            fetched_at = datetime.now(timezone.utc).isoformat()
            return DownloadResult(
                url=url,
                final_url=final_url,
                local_path=final_path,
                content_type=content_type,
                file_size=file_size,
                sha256=sha256,
                fetched_at=fetched_at,
                etag=etag,
                last_modified=last_modified,
            )

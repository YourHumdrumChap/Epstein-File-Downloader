from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import aiohttp
from PySide6.QtCore import QObject, Signal

from doj_disclosures.core.config import AppConfig
from doj_disclosures.core.crawler import Crawler, looks_downloadable
from doj_disclosures.core.db import Database
from doj_disclosures.core.downloader import Downloader, NotModifiedError
from doj_disclosures.core.feedback import PHRASE_BLACKLIST_KEY, URL_PENALTIES_KEY
from doj_disclosures.core.matching import KeywordMatcher
from doj_disclosures.core.parser import DocumentParser
from doj_disclosures.core.pipeline import PipelineDeps, PipelineInput, build_semantic_context_async, process_document
from doj_disclosures.core.relevance import load_url_penalties
from doj_disclosures.core.storage_gating import plan_storage
from doj_disclosures.core.release_monitor import store_snapshot_and_diff
from doj_disclosures.core.triage_index import write_semantic_sorted_index
from doj_disclosures.core.utils import sha256_file

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerStats:
    queued: int = 0
    processed: int = 0
    downloaded: int = 0
    matched_docs: int = 0


class CrawlWorker(QObject):
    log = Signal(str)
    status = Signal(str, str)  # url, status
    progress = Signal(int, int)  # processed, queued
    finished = Signal()
    error = Signal(str)

    def __init__(self, *, config: AppConfig, db: Database, seed_urls: list[str]) -> None:
        super().__init__()
        self._config = config
        self._db = db
        self._seed_urls = seed_urls
        # These asyncio primitives must be created/used on the worker thread's event loop.
        # Pause/Resume is triggered from the GUI thread, so we mutate them via
        # loop.call_soon_threadsafe once the loop is running.
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pause: asyncio.Event | None = None
        self._stop: asyncio.Event | None = None
        self._desired_paused: bool = False
        self._stats = WorkerStats()

    def pause(self) -> None:
        if self._stop is not None and self._stop.is_set():
            return
        self._desired_paused = True
        if self._pause is None or self._loop is None:
            # Loop not ready yet; will pause as soon as the worker starts.
            self.log.emit("Pause requested...")
            return
        if not self._pause.is_set():
            return
        self._loop.call_soon_threadsafe(self._pause.clear)
        self.log.emit("Paused")

    def resume(self) -> None:
        self._desired_paused = False
        if self._pause is None or self._loop is None:
            self.log.emit("Resume requested...")
            return
        if self._stop is not None and self._stop.is_set():
            return
        if self._pause.is_set():
            return
        self._loop.call_soon_threadsafe(self._pause.set)
        self.log.emit("Resumed")

    def stop(self) -> None:
        if self._stop is None or self._loop is None:
            return
        if self._stop.is_set():
            return
        self._loop.call_soon_threadsafe(self._stop.set)
        # Unpause to allow tasks to observe stop quickly.
        if self._pause is not None:
            self._loop.call_soon_threadsafe(self._pause.set)
        self.log.emit("Stopping...")

    def run(self) -> None:
        try:
            asyncio.run(self._run_async())
        except Exception as e:
            self.error.emit(str(e))
        finally:
            self.finished.emit()

    async def _run_async(self) -> None:
        self._loop = asyncio.get_running_loop()
        # Create events on the running loop (thread affinity).
        self._pause = asyncio.Event()
        self._pause.set()
        self._stop = asyncio.Event()
        if self._desired_paused:
            self._pause.clear()
        s = self._config.crawl
        timeout = aiohttp.ClientTimeout(total=None)
        connector = aiohttp.TCPConnector(limit=s.max_concurrency)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            crawler = Crawler(db=self._db, settings=s, session=session, pause_event=self._pause, stop_event=self._stop)
            await self._db.clear_pending_urls()
            await crawler.initialize(seed_urls=self._seed_urls)

            storage = plan_storage(self._config.paths.output_dir)
            downloader = Downloader(
                settings=s,
                session=session,
                output_dir=storage.raw_dir,
                pause_event=self._pause,
                stop_event=self._stop,
            )

            penalties = load_url_penalties(await self._db.kv_get(URL_PENALTIES_KEY))
            semantic = await build_semantic_context_async(settings=s, db=self._db)

            keywords = await self._load_keywords(self._config.paths.keywords_path)
            # Phrase blacklist learned from feedback.
            blacklist: set[str] = set()
            raw_bl = await self._db.kv_get(PHRASE_BLACKLIST_KEY)
            try:
                data = json.loads(raw_bl) if raw_bl else []
                if isinstance(data, list):
                    blacklist = {str(x).strip() for x in data if str(x).strip()}
            except Exception:
                blacklist = set()
            if blacklist:
                keywords = [k for k in keywords if str(k).strip() and str(k).strip() not in blacklist]
            matcher = KeywordMatcher(
                keywords=keywords,
                query=s.query,
                fuzzy_enabled=True,
                semantic_enabled=s.semantic_enabled,
                semantic_threshold=s.semantic_threshold,
                stopwords={w.strip().lower() for w in s.stopwords.split(",") if w.strip()},
            )
            parser = DocumentParser(
                ocr_enabled=s.ocr_enabled,
                ocr_engine=getattr(s, "ocr_engine", "tesseract"),
                ocr_dpi=int(getattr(s, "ocr_dpi", 200)),
                ocr_preprocess=bool(getattr(s, "ocr_preprocess", True)),
                ocr_median_filter=bool(getattr(s, "ocr_median_filter", True)),
                ocr_threshold=getattr(s, "ocr_threshold", None),
            )

            pipeline_deps = PipelineDeps(
                settings=s,
                db=self._db,
                storage=storage,
                parser=parser,
                matcher=matcher,
                penalties=penalties,
                semantic=semantic,
            )

            sem = asyncio.Semaphore(s.max_concurrency)

            async def _reprocess_cached_document(url: str, *, now: str) -> bool:
                rec = await self._db.get_url_cached_record(url=url)
                if rec is None or not rec.local_path:
                    return False
                local_path = Path(rec.local_path)
                if not local_path.exists():
                    return False

                content_type = rec.content_type or "application/octet-stream"
                sha = rec.sha256
                if not sha:
                    try:
                        sha = sha256_file(local_path)
                    except Exception:
                        return False

                out = await process_document(
                    deps=pipeline_deps,
                    inp=PipelineInput(
                        url=url,
                        final_url=rec.final_url or url,
                        local_path=local_path,
                        content_type=content_type,
                        file_size=(local_path.stat().st_size if local_path.exists() else None),
                        sha256=sha,
                        fetched_at=now,
                    ),
                    now=now,
                    allow_move=False,
                    reprocess_existing=True,
                    log=self.log.emit,
                )

                if out.hits:
                    self._stats = WorkerStats(
                        queued=self._stats.queued,
                        processed=self._stats.processed,
                        downloaded=self._stats.downloaded,
                        matched_docs=self._stats.matched_docs + 1,
                    )
                    self.log.emit(f"FLAGGED (cached reprocess): {local_path.name}")
                return True

            async def handle(item_url: str) -> None:
                async with sem:
                    if self._stop.is_set():
                        return
                    await self._pause.wait()

                    kind = "document" if looks_downloadable(item_url) else "page"
                    self.status.emit(item_url, f"processing ({kind})")
                    now = datetime.now(timezone.utc).isoformat()
                    await self._db.update_url_attempt(
                        url=item_url,
                        status="processing",
                        last_attempt_at=now,
                        http_status=None,
                        error=None,
                    )

                    try:
                        if kind == "page":
                            await self._pause.wait()
                            self.log.emit(f"Crawl page: {item_url}")
                            links = await crawler.process_page(item_url)
                            doc_links = [u for u in links if looks_downloadable(u)]
                            # Note: even in seed-only mode we may enqueue pagination page links; don't
                            # mislead by only reporting downloadable docs.
                            if s.follow_discovered_pages:
                                self.log.emit(f"Discovered {len(links)} links on page")
                            else:
                                self.log.emit(f"Discovered {len(links)} link(s) ({len(doc_links)} document link(s)) on page")

                            if not links:
                                try:
                                    info = await self._db.get_url_debug_info(url=item_url)
                                    if info is not None:
                                        st, hs, err = info
                                        if hs or err:
                                            self.log.emit(f"WARN: page crawl yielded 0 links; url_status={st} http_status={hs} error={err}")
                                except Exception:
                                    pass
                        else:
                            await self._pause.wait()
                            self.log.emit(f"Download: {item_url}")
                            etag, last_modified = await self._db.get_url_cache_headers(url=item_url)
                            cache_headers: dict[str, str] = {}
                            if etag:
                                cache_headers["If-None-Match"] = etag
                            if last_modified:
                                cache_headers["If-Modified-Since"] = last_modified

                            dl = await downloader.download(item_url, cache_headers=(cache_headers or None))
                            self._stats = WorkerStats(
                                queued=self._stats.queued,
                                processed=self._stats.processed,
                                downloaded=self._stats.downloaded + 1,
                                matched_docs=self._stats.matched_docs,
                            )

                            # If the user pauses right after the download completes, don't start
                            # parsing/OCR/DB work until resumed.
                            await self._pause.wait()

                            out = await process_document(
                                deps=pipeline_deps,
                                inp=PipelineInput(
                                    url=item_url,
                                    final_url=dl.final_url,
                                    local_path=Path(str(dl.local_path)),
                                    content_type=dl.content_type,
                                    file_size=dl.file_size,
                                    sha256=dl.sha256,
                                    fetched_at=dl.fetched_at,
                                    etag=dl.etag,
                                    last_modified=dl.last_modified,
                                ),
                                now=now,
                                allow_move=True,
                                reprocess_existing=False,
                                log=self.log.emit,
                            )

                            if out.passes_relevance:
                                try:
                                    self.log.emit(f"Flagged: {Path(str(out.final_path)).name}")
                                except Exception:
                                    pass

                            if out.hits:
                                self._stats = WorkerStats(
                                    queued=self._stats.queued,
                                    processed=self._stats.processed,
                                    downloaded=self._stats.downloaded,
                                    matched_docs=self._stats.matched_docs + 1,
                                )
                                self.log.emit(f"FLAGGED ({len(out.hits)} hits): {dl.local_path.name}")

                            await self._db.update_url_attempt(
                                url=item_url,
                                status="done",
                                last_attempt_at=now,
                                http_status=200,
                                error=None,
                                content_type=dl.content_type,
                                title=out.parsed.title,
                                final_url=dl.final_url,
                                local_path=str(out.final_path),
                                sha256=dl.sha256,
                                etag=dl.etag,
                                last_modified=dl.last_modified,
                            )

                        self._stats = WorkerStats(
                            queued=self._stats.queued,
                            processed=self._stats.processed + 1,
                            downloaded=self._stats.downloaded,
                            matched_docs=self._stats.matched_docs,
                        )
                        self.status.emit(item_url, "done")
                    except asyncio.CancelledError:
                        raise
                    except NotModifiedError:
                        reprocessed = False
                        if bool(getattr(s, "reprocess_cached_on_not_modified", False)):
                            try:
                                reprocessed = await _reprocess_cached_document(item_url, now=now)
                            except Exception as e:
                                self.log.emit(f"WARN: cached reprocess failed: {e}")
                                reprocessed = False

                        await self._db.update_url_attempt(
                            url=item_url,
                            status="done",
                            last_attempt_at=now,
                            http_status=304,
                            error=None,
                        )
                        self._stats = WorkerStats(
                            queued=self._stats.queued,
                            processed=self._stats.processed + 1,
                            downloaded=self._stats.downloaded,
                            matched_docs=self._stats.matched_docs,
                        )
                        self.status.emit(item_url, "done (reprocessed cached)" if reprocessed else "done (not modified)")
                    except Exception as e:
                        await self._db.update_url_attempt(
                            url=item_url,
                            status="retry",
                            last_attempt_at=now,
                            http_status=None,
                            error=str(e),
                        )
                        self.status.emit(item_url, f"error: {e}")
                        self.log.emit(f"ERROR: {item_url} ({e})")
                    finally:
                        self.progress.emit(self._stats.processed, self._stats.queued)

            tasks: set[asyncio.Task[None]] = set()
            async for item in crawler.iter_discovered():
                if self._stop.is_set():
                    break

                # Pausing should freeze both processing *and* queue growth; otherwise the UI
                # keeps updating and it feels like Pause doesn't work.
                await self._pause.wait()
                if self._stop.is_set():
                    break

                self._stats = WorkerStats(
                    queued=self._stats.queued + 1,
                    processed=self._stats.processed,
                    downloaded=self._stats.downloaded,
                    matched_docs=self._stats.matched_docs,
                )
                self.progress.emit(self._stats.processed, self._stats.queued)
                t = asyncio.create_task(handle(item.url))
                tasks.add(t)
                t.add_done_callback(lambda tt: tasks.discard(tt))
                while len(tasks) >= s.max_concurrency * 2:
                    await asyncio.sleep(0.05)

            if self._stop.is_set() and tasks:
                for t in list(tasks):
                    try:
                        t.cancel()
                    except Exception:
                        pass

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            # Write semantic sort indices for convenience.
            try:
                rows = await self._db.query_flagged_with_metrics(limit=100000)
                write_semantic_sorted_index(out_dir=storage.flagged_dir, rows=rows)
                hv = [r for r in rows if str(r.get("review_status") or "").lower() == "high_value"]
                ir = [r for r in rows if str(r.get("review_status") or "").lower() == "irrelevant"]
                write_semantic_sorted_index(out_dir=storage.flagged_dir / "high_value", rows=hv)
                write_semantic_sorted_index(out_dir=storage.flagged_dir / "irrelevant", rows=ir)
            except Exception as e:
                self.log.emit(f"WARN: semantic index write failed: {e}")

            # Release snapshot + diff (best-effort). Stored in DB kv and also logged.
            try:
                diff = await store_snapshot_and_diff(self._db)
                if diff.added or diff.changed or diff.removed:
                    self.log.emit(
                        f"Release diff: +{len(diff.added)} / ~{len(diff.changed)} / -{len(diff.removed)} (vs last snapshot)"
                    )
            except Exception as e:
                self.log.emit(f"WARN: release diff failed: {e}")

    async def _load_keywords(self, path: Path) -> list[str]:
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict) and "seed_keywords" in data:
                    return [str(x) for x in (data.get("seed_keywords") or [])]
                if isinstance(data, list):
                    return [str(x) for x in data]
            except Exception:
                return []
        try:
            import importlib.resources as res

            with res.files("doj_disclosures.resources").joinpath("default_keywords.json").open("r", encoding="utf-8") as f:
                data = json.load(f)
                return [str(x) for x in (data.get("seed_keywords") or [])]
        except Exception:
            return []

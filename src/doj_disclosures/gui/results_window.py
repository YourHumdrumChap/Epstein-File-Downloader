from __future__ import annotations

import asyncio
import html
import re
import fnmatch
from pathlib import Path

from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
)

from doj_disclosures.core.db import Database
from doj_disclosures.core.config import AppConfig
from doj_disclosures.core.embeddings import get_default_provider
from doj_disclosures.core.feedback import apply_feedback
from doj_disclosures.core.hybrid_search import HybridSearcher


class ResultsWindow(QDialog):
    def __init__(self, *, db: Database, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Results")
        self.resize(900, 600)
        self._db = db
        self._doc_map: dict[int, dict] = {}
        self._searcher = HybridSearcher(db=db)

        self.list = QListWidget()
        self.details = QTextEdit()
        self.details.setReadOnly(True)

        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search (FTS + optional semantic)")
        self.search_btn = QPushButton("Search")
        self.clear_search_btn = QPushButton("Clear Search")
        self.search_btn.clicked.connect(self._do_search)
        self.clear_search_btn.clicked.connect(self._clear_search)

        self.mark_irrelevant_btn = QPushButton("Mark as irrelevant")
        self.mark_high_value_btn = QPushButton("Mark as high value")
        self.mark_irrelevant_btn.clicked.connect(lambda: self._apply_feedback("irrelevant"))
        self.mark_high_value_btn.clicked.connect(lambda: self._apply_feedback("high_value"))
        self.mark_irrelevant_btn.setEnabled(False)
        self.mark_high_value_btn.setEnabled(False)

        self.open_folder_btn = QPushButton("Open Containing Folder")
        self.open_folder_btn.clicked.connect(self._open_folder)
        self.open_folder_btn.setEnabled(False)

        self.clear_btn = QPushButton("Clear Results")
        self.clear_btn.clicked.connect(self._clear_results)

        splitter = QSplitter()
        splitter.addWidget(self.list)
        splitter.addWidget(self.details)
        splitter.setStretchFactor(1, 1)

        bottom = QHBoxLayout()
        bottom.addWidget(self.open_folder_btn)
        bottom.addWidget(self.mark_irrelevant_btn)
        bottom.addWidget(self.mark_high_value_btn)
        bottom.addWidget(self.clear_btn)
        bottom.addStretch(1)

        layout = QVBoxLayout()
        top = QHBoxLayout()
        top.addWidget(self.search_box)
        top.addWidget(self.search_btn)
        top.addWidget(self.clear_search_btn)
        layout.addLayout(top)
        layout.addWidget(splitter)
        layout.addLayout(bottom)
        self.setLayout(layout)

        self.list.currentItemChanged.connect(self._on_select)
        self._reload_flagged()

    @staticmethod
    def _build_preview_html(*, text: str, matches: list[dict], max_chars: int = 15000) -> str:
        preview = (text or "")[:max_chars]
        if not preview:
            return ""

        spans: list[tuple[int, int]] = []

        def add_span(a: int, b: int) -> None:
            if a < 0 or b <= a:
                return
            a = max(0, min(len(preview), int(a)))
            b = max(0, min(len(preview), int(b)))
            if b <= a:
                return
            spans.append((a, b))

        def find_all(substr: str) -> None:
            s = (substr or "")
            if not s:
                return
            start = 0
            while True:
                i = preview.find(s, start)
                if i < 0:
                    break
                add_span(i, i + len(s))
                start = i + max(1, len(s))

        def keyword_regex(kw: str) -> re.Pattern[str] | None:
            k = (kw or "").strip()
            if not k:
                return None
            tokens = re.findall(r"\w+", k, flags=re.UNICODE)
            if not tokens:
                return None
            if len(tokens) == 1:
                pat = rf"(?<!\w){re.escape(tokens[0])}(?!\w)"
            else:
                pat = rf"(?<!\w){r'\s+'.join(re.escape(t) for t in tokens)}(?!\w)"
            try:
                return re.compile(pat, flags=re.IGNORECASE | re.UNICODE)
            except re.error:
                return None

        word_re = re.compile(r"\b[\w\-']+\b", flags=re.UNICODE)

        # Prefer highlighting the actual matched term/pattern. If we can't, fall back to
        # highlighting the stored snippet text.
        for m in (matches or [])[:300]:
            method = str(m.get("method") or "").strip().lower()
            pattern = str(m.get("pattern") or "")
            snippet = str(m.get("snippet") or "").strip()

            before = len(spans)
            if method in {"keyword", "fuzzy", "semantic"}:
                rx = keyword_regex(pattern)
                if rx is not None:
                    for mm in rx.finditer(preview):
                        add_span(*mm.span())
                        if len(spans) >= 600:
                            break
            elif method == "regex":
                raw = pattern
                if raw.startswith("re:"):
                    raw = raw[3:]
                try:
                    rx = re.compile(raw, flags=re.IGNORECASE | re.UNICODE)
                    for mm in rx.finditer(preview):
                        add_span(*mm.span())
                        if len(spans) >= 600:
                            break
                except re.error:
                    pass
            elif method == "wildcard":
                pat = (pattern or "").strip()
                if pat:
                    for mm in word_re.finditer(preview):
                        w = mm.group(0)
                        if fnmatch.fnmatch(w.lower(), pat.lower()):
                            add_span(*mm.span())
                            if len(spans) >= 600:
                                break

            # Fallback: highlight the snippet region if we didn't find any spans for this match.
            if len(spans) == before and snippet and len(snippet) >= 6:
                find_all(snippet)
                if len(spans) >= 600:
                    break

        if not spans:
            return html.escape(preview)

        # Merge overlaps.
        spans = sorted(spans)
        merged: list[tuple[int, int]] = []
        for a, b in spans:
            if not merged:
                merged.append((a, b))
                continue
            la, lb = merged[-1]
            if a <= lb:
                merged[-1] = (la, max(lb, b))
            else:
                merged.append((a, b))

        # Render: underline + bold (no custom colors).
        out: list[str] = []
        pos = 0
        for a, b in merged:
            if pos < a:
                out.append(html.escape(preview[pos:a]))
            seg = html.escape(preview[a:b])
            out.append("<span style='text-decoration: underline; font-weight: 600'>" + seg + "</span>")
            pos = b
        if pos < len(preview):
            out.append(html.escape(preview[pos:]))
        return "".join(out)

    def _do_search(self) -> None:
        q = self.search_box.text().strip()
        if not q:
            self._reload_flagged()
            return
        rows = asyncio.run(self._searcher.search(q, limit=500))
        self.list.clear()
        self._doc_map.clear()
        doc_ids = [int(r["doc_id"]) for r in rows]
        review_map = asyncio.run(self._db.get_review_status_map(doc_ids=doc_ids))
        redaction_map = asyncio.run(self._db.get_redaction_max_map(doc_ids=doc_ids))
        for r in rows:
            doc_id = int(r["doc_id"])
            r["review_status"] = review_map.get(doc_id, "new")
            r["redaction_max"] = float(redaction_map.get(doc_id, 0.0) or 0.0)
            self._doc_map[doc_id] = r
            score = float(r.get("score") or 0.0)
            status = str(r.get("review_status") or "new")
            red = float(r.get("redaction_max") or 0.0)
            tag = "" if status == "new" else f" ({status})"
            red_tag = f" red={red:.2f}" if red > 0 else ""
            item = QListWidgetItem(f"[{score:.3f}] {r.get('title') or '(untitled)'}{tag}{red_tag}")
            item.setData(256, doc_id)
            self.list.addItem(item)

    def _clear_search(self) -> None:
        self.search_box.setText("")
        self._reload_flagged()

    def _clear_results(self) -> None:
        ok = QMessageBox.question(
            self,
            "Clear Results",
            "This will clear all stored documents, extracted text, and match results from the database.\n"
            "Downloaded files on disk will NOT be deleted.\n\n"
            "Continue?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if ok != QMessageBox.Yes:
            return
        asyncio.run(self._db.clear_results())
        self.details.clear()
        self.open_folder_btn.setEnabled(False)
        self._reload_flagged()

    def _reload_flagged(self) -> None:
        rows = asyncio.run(self._db.query_flagged(limit=500))
        self.list.clear()
        self._doc_map.clear()
        doc_ids = [int(r["doc_id"]) for r in rows]
        review_map = asyncio.run(self._db.get_review_status_map(doc_ids=doc_ids))
        redaction_map = asyncio.run(self._db.get_redaction_max_map(doc_ids=doc_ids))

        for r in rows:
            doc_id = int(r["doc_id"])
            r["review_status"] = review_map.get(doc_id, "new")
            r["redaction_max"] = float(redaction_map.get(doc_id, 0.0) or 0.0)
            self._doc_map[doc_id] = r
            status = str(r.get("review_status") or "new")
            red = float(r.get("redaction_max") or 0.0)
            tag = "" if status == "new" else f" ({status})"
            red_tag = f" red={red:.2f}" if red > 0 else ""
            item = QListWidgetItem(f"[{r['match_count']}] {r['title'] or '(untitled)'}{tag}{red_tag}")
            item.setData(256, doc_id)
            self.list.addItem(item)

    def _on_select(self, current: QListWidgetItem | None, prev: QListWidgetItem | None) -> None:
        if not current:
            self.details.clear()
            self.open_folder_btn.setEnabled(False)
            self.mark_irrelevant_btn.setEnabled(False)
            self.mark_high_value_btn.setEnabled(False)
            return
        doc_id = int(current.data(256))
        doc = self._doc_map.get(doc_id)
        if not doc:
            return
        matches = asyncio.run(self._db.query_matches_for_doc(doc_id))
        status = asyncio.run(self._db.get_review_status(doc_id=doc_id))
        redactions = asyncio.run(self._db.query_page_flags_for_doc(doc_id=doc_id, flag="redaction"))
        content = asyncio.run(self._db.get_fts_content(doc_id=doc_id)) or ""

        # Build an HTML view: header + matches + preview.
        header = [
            f"<b>Title:</b> {html.escape(str(doc.get('title','')))}",
            f"<b>URL:</b> {html.escape(str(doc.get('url','')))}",
            f"<b>Local path:</b> {html.escape(str(doc.get('local_path','')))}",
            f"<b>Review status:</b> {html.escape(status)}",
        ]
        if redactions:
            top = sorted(redactions, key=lambda x: float(x.get("score") or 0.0), reverse=True)[:5]
            rline = ", ".join([f"p{int(x.get('page_no') or 0)}={float(x.get('score') or 0.0):.2f}" for x in top])
            header.append(f"<b>Redaction pages:</b> {html.escape(rline)}")

        match_lines: list[str] = ["<b>Matches:</b>"]
        for m in matches[:60]:
            match_lines.append(
                f"&nbsp;&nbsp;• {html.escape(str(m['method']))} score={float(m['score']):.2f} pattern={html.escape(str(m['pattern']))}<br/>"
                f"&nbsp;&nbsp;&nbsp;&nbsp;<span style='color:#444'>{html.escape(str(m['snippet']))}</span>"
            )

        escaped_preview = self._build_preview_html(text=content, matches=matches, max_chars=15000)

        body = "<br/>".join(header)
        body += "<hr/>" + "<br/>".join(match_lines)
        body += "<hr/><b>Preview (extracted text):</b><br/><pre style='white-space: pre-wrap'>" + escaped_preview + "</pre>"
        self.details.setHtml(body)
        self.open_folder_btn.setEnabled(bool(doc.get("local_path")))
        self.mark_irrelevant_btn.setEnabled(True)
        self.mark_high_value_btn.setEnabled(True)

    def _open_folder(self) -> None:
        item = self.list.currentItem()
        if not item:
            return
        doc_id = int(item.data(256))
        doc = self._doc_map.get(doc_id)
        if not doc:
            return
        p = Path(str(doc.get("local_path", "")))
        if not p.exists():
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(p.parent)))

    def _apply_feedback(self, label: str) -> None:
        item = self.list.currentItem()
        if not item:
            return
        doc_id = int(item.data(256))
        cfg = AppConfig.load()
        model_name = str(getattr(cfg.crawl, "embedding_model_name", "sentence-transformers/all-MiniLM-L6-v2"))
        provider = get_default_provider(model_name)
        asyncio.run(
            apply_feedback(
                db=self._db,
                doc_id=doc_id,
                label=label,
                provider=provider,
                model_name=model_name,
                output_dir=cfg.paths.output_dir,
                storage_layout=str(getattr(cfg.crawl, "storage_layout", "flat")),
            )
        )

        # Refresh label in UI
        st = asyncio.run(self._db.get_review_status(doc_id=doc_id))
        doc = self._doc_map.get(doc_id) or {}
        doc["review_status"] = st
        self._doc_map[doc_id] = doc
        title = str(doc.get("title") or "(untitled)")
        red = float(doc.get("redaction_max") or 0.0)
        red_tag = f" red={red:.2f}" if red > 0 else ""
        tag = "" if st == "new" else f" ({st})"
        if "match_count" in doc:
            item.setText(f"[{int(doc.get('match_count') or 0)}] {title}{tag}{red_tag}")
        else:
            score = float(doc.get("score") or 0.0)
            item.setText(f"[{score:.3f}] {title}{tag}{red_tag}")

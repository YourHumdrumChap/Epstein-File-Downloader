from __future__ import annotations

import asyncio
import csv
from pathlib import Path

from PySide6.QtCore import QThread, QTimer
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QSplitter,
    QTableView,
    QTextEdit,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from doj_disclosures.core.config import AppConfig
from doj_disclosures.core.db import Database
from doj_disclosures.gui.keywords_dialog import KeywordsDialog
from doj_disclosures.gui.models import StatusTableModel
from doj_disclosures.gui.results_window import ResultsWindow
from doj_disclosures.gui.settings_dialog import SettingsDialog
from doj_disclosures.gui.worker import CrawlWorker


ETHICS_NOTICE = (
    "This tool is intended for lawful research and automated triage of PUBLIC documents.\n\n"
    "Before crawling or downloading any content, confirm you have the right to access and store it.\n"
    "The crawler obeys robots.txt and rate limits, and is designed to minimize load.\n\n"
    "Continue only if you agree to use it responsibly."
)


class MainWindow(QMainWindow):
    def __init__(self, *, config: AppConfig, db: Database) -> None:
        super().__init__()
        self.setWindowTitle("DOJ Disclosures Crawler")
        self.resize(1150, 750)
        self._config = config
        self._db = db

        self._thread: QThread | None = None
        self._worker: CrawlWorker | None = None
        self._ethics_prompt_shown: bool = False

        # Buttons required by spec
        self.start_btn = QPushButton("Start Crawl")
        self.pause_btn = QPushButton("Pause")
        self.stop_btn = QPushButton("Stop")
        self.kw_btn = QPushButton("Manage Keywords")
        self.out_btn = QPushButton("Output Folder")
        self.view_btn = QPushButton("View Results")
        self.export_btn = QPushButton("Export CSV")

        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)

        for b in [
            self.start_btn,
            self.pause_btn,
            self.stop_btn,
            self.kw_btn,
            self.out_btn,
            self.view_btn,
            self.export_btn,
        ]:
            b.setAccessibleName(b.text())

        self.progress = QProgressBar()
        self.progress.setRange(0, 0)
        self.progress.setFormat("Idle")

        self.status_model = StatusTableModel()
        self.status_table = QTableView()
        self.status_table.setModel(self.status_model)
        self.status_table.horizontalHeader().setStretchLastSection(True)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)

        self.seed_urls = QTextEdit()
        self.seed_urls.setPlaceholderText(
            "Paste one or more seed URLs (one per line).\n"
            "The crawler will only process these URLs and any directly-linked documents."
        )
        self.seed_urls.setAcceptRichText(False)
        initial_seeds = list(self._config.last_seed_urls) if self._config.last_seed_urls else [self._config.crawl.start_url]
        self.seed_urls.setPlainText("\n".join(initial_seeds))

        # Toolbar shortcuts
        tb = QToolBar("Main")
        self.addToolBar(tb)
        settings_action = QAction("Settings", self)
        settings_action.setShortcut(QKeySequence("Ctrl+,"))
        settings_action.triggered.connect(self._open_settings)
        tb.addAction(settings_action)

        results_action = QAction("View Results", self)
        results_action.setShortcut(QKeySequence("Ctrl+R"))
        results_action.triggered.connect(self._view_results)
        tb.addAction(results_action)

        export_action = QAction("Export CSV", self)
        export_action.setShortcut(QKeySequence("Ctrl+E"))
        export_action.triggered.connect(self._export_csv)
        tb.addAction(export_action)

        # Layout
        top = QHBoxLayout()
        for b in [
            self.start_btn,
            self.pause_btn,
            self.stop_btn,
            self.kw_btn,
            self.out_btn,
            self.view_btn,
            self.export_btn,
        ]:
            top.addWidget(b)
        top.addStretch(1)

        splitter = QSplitter()
        splitter.addWidget(self.status_table)
        splitter.addWidget(self.log_view)
        splitter.setStretchFactor(1, 1)

        layout = QVBoxLayout()
        layout.addWidget(QLabel("Seed URL(s)"))
        layout.addWidget(self.seed_urls)
        layout.addLayout(top)
        layout.addWidget(QLabel("Progress"))
        layout.addWidget(self.progress)
        layout.addWidget(splitter)

        root = QWidget()
        root.setLayout(layout)
        self.setCentralWidget(root)

        # Handlers
        self.start_btn.clicked.connect(self._start)
        self.pause_btn.clicked.connect(self._pause_or_resume)
        self.stop_btn.clicked.connect(self._stop)
        self.kw_btn.clicked.connect(self._manage_keywords)
        self.out_btn.clicked.connect(self._choose_output_folder)
        self.view_btn.clicked.connect(self._view_results)
        self.export_btn.clicked.connect(self._export_csv)

        if not self._config.first_run_acknowledged:
            # Prevent a confusing "app doesn't open" situation where a modal dialog
            # appears before the window is visible (and may be hidden behind other windows).
            self.start_btn.setEnabled(False)
            QTimer.singleShot(0, self._show_ethics_notice)

    def _show_ethics_notice(self) -> None:
        if self._config.first_run_acknowledged or self._ethics_prompt_shown:
            return
        self._ethics_prompt_shown = True
        try:
            self.raise_()
            self.activateWindow()
        except Exception:
            pass

        ok = QMessageBox.question(self, "Legal / Ethics Notice", ETHICS_NOTICE, QMessageBox.Yes | QMessageBox.No)
        if ok == QMessageBox.Yes:
            self._config = AppConfig(
                paths=self._config.paths,
                crawl=self._config.crawl,
                first_run_acknowledged=True,
                last_seed_urls=self._config.last_seed_urls,
            )
            self._config.save()
            self.start_btn.setEnabled(True)
            self._append_log("Notice accepted.")
        else:
            self._append_log("Notice declined; crawling disabled until accepted.")
            self.start_btn.setEnabled(False)

    def _append_log(self, msg: str) -> None:
        self.log_view.append(msg)

    def _open_settings(self) -> None:
        dlg = SettingsDialog(config=self._config, parent=self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            self._config = dlg.updated_config()
            self._config.save()
            self._append_log("Settings saved")

    def _start(self) -> None:
        if self._thread is not None:
            return

        raw = self.seed_urls.toPlainText().strip()
        seeds = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        seeds = list(dict.fromkeys(seeds))
        if not seeds:
            QMessageBox.warning(self, "Seed URLs required", "Paste at least one URL to crawl before starting.")
            return
        bad = [u for u in seeds if not (u.startswith("http://") or u.startswith("https://"))]
        if bad:
            QMessageBox.warning(
                self,
                "Invalid URL",
                "These seed URLs are not valid (must start with http:// or https://):\n\n" + "\n".join(bad),
            )
            return

        self._append_log("Starting crawl...")
        self._append_log(f"Seeds: {len(seeds)}")

        # Persist the last used seed list so it restores on next launch.
        self._config = AppConfig(
            paths=self._config.paths,
            crawl=self._config.crawl,
            first_run_acknowledged=self._config.first_run_acknowledged,
            last_seed_urls=tuple(seeds),
        )
        self._config.save()

        self._thread = QThread()
        self._worker = CrawlWorker(config=self._config, db=self._db, seed_urls=seeds)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.log.connect(self._append_log)
        self._worker.status.connect(lambda url, st: self.status_model.upsert(url, st))
        self._worker.error.connect(lambda e: self._append_log(f"ERROR: {e}"))
        self._worker.progress.connect(self._on_progress)
        self._worker.finished.connect(self._on_finished)

        self._thread.start()
        self.start_btn.setEnabled(False)
        self.pause_btn.setEnabled(True)
        self.stop_btn.setEnabled(True)
        self.progress.setRange(0, 0)
        self.progress.setFormat("Running...")

    def _pause_or_resume(self) -> None:
        if not self._worker:
            return
        if self.pause_btn.text() == "Pause":
            self._worker.pause()
            self.pause_btn.setText("Resume")
            self.progress.setFormat("Paused")
        else:
            self._worker.resume()
            self.pause_btn.setText("Pause")
            self.progress.setFormat("Running...")

    def _stop(self) -> None:
        if self._worker:
            self._worker.stop()
        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.progress.setFormat("Stopping...")

    def _on_progress(self, processed: int, queued: int) -> None:
        # When paused, don't immediately overwrite the user's visible "Paused" state.
        if self.pause_btn.isEnabled() and self.pause_btn.text() == "Resume":
            self.progress.setFormat("Paused")
            return
        self.progress.setFormat(f"Processed: {processed} | Queued: {queued}")

    def _on_finished(self) -> None:
        self._append_log("Worker finished")
        if self._thread:
            self._thread.quit()
            self._thread.wait(2000)
        self._thread = None
        self._worker = None
        self.start_btn.setEnabled(True)
        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.pause_btn.setText("Pause")
        self.progress.setRange(0, 0)
        self.progress.setFormat("Idle")

    def _manage_keywords(self) -> None:
        dlg = KeywordsDialog(keywords_path=self._config.paths.keywords_path, parent=self)
        dlg.exec()
        self._append_log("Keywords updated")

    def _choose_output_folder(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Choose output folder", str(self._config.paths.output_dir))
        if not path:
            return
        out = Path(path)
        out.mkdir(parents=True, exist_ok=True)
        # Update config paths by rebuilding Paths
        paths = self._config.paths
        new_paths = paths.__class__(
            app_dir=paths.app_dir,
            db_path=paths.db_path,
            log_path=paths.log_path,
            output_dir=out,
            keywords_path=paths.keywords_path,
        )
        self._config = AppConfig(
            paths=new_paths,
            crawl=self._config.crawl,
            first_run_acknowledged=self._config.first_run_acknowledged,
            last_seed_urls=self._config.last_seed_urls,
        )
        self._config.save()
        self._append_log(f"Output folder set: {out}")

    def _view_results(self) -> None:
        dlg = ResultsWindow(db=self._db, parent=self)
        dlg.exec()

    def _export_csv(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Export CSV", "flagged_results.csv", "CSV (*.csv)")
        if not path:
            return
        rows = asyncio.run(self._db.query_flagged(limit=2000))
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["doc_id", "url", "title", "local_path", "fetched_at", "match_count"])
            w.writeheader()
            for r in rows:
                w.writerow(r)
        QMessageBox.information(self, "Export", f"Exported {len(rows)} rows")

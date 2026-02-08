from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
)

from doj_disclosures.core.config import AppConfig, CrawlSettings


class SettingsDialog(QDialog):
    def __init__(self, *, config: AppConfig, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self._config = config

        self.max_conc = QSpinBox()
        self.max_conc.setRange(1, 64)
        self.max_conc.setValue(config.crawl.max_concurrency)

        self.rps = QDoubleSpinBox()
        self.rps.setDecimals(2)
        self.rps.setRange(0.1, 30.0)
        self.rps.setValue(config.crawl.requests_per_second)

        self.ua = QLineEdit(config.crawl.user_agent)

        self.ocr = QCheckBox("Enable OCR (only when necessary)")
        self.ocr.setChecked(config.crawl.ocr_enabled)

        self.semantic = QCheckBox("Enable semantic matching (optional)")
        self.semantic.setChecked(config.crawl.semantic_enabled)

        self.threshold = QDoubleSpinBox()
        self.threshold.setRange(0.0, 1.0)
        self.threshold.setDecimals(2)
        self.threshold.setValue(config.crawl.semantic_threshold)

        self.auto_download = QCheckBox("Auto-download flagged files")
        self.auto_download.setChecked(config.crawl.auto_download)

        self.stopwords = QLineEdit(config.crawl.stopwords)
        self.query = QLineEdit(config.crawl.query)

        self.allow_offsite = QCheckBox("Allow off-site links")
        self.allow_offsite.setChecked(config.crawl.allow_offsite)

        self.follow_pages = QCheckBox("Follow discovered pages (recursive crawl)")
        self.follow_pages.setChecked(config.crawl.follow_discovered_pages)

        self.age_verify = QCheckBox("I am 18+; allow age-gated downloads (justice.gov)")
        self.age_verify.setChecked(getattr(config.crawl, "age_verify_opt_in", False))

        self.reprocess_cached = QCheckBox("Reprocess cached documents (even if not modified)")
        self.reprocess_cached.setChecked(getattr(config.crawl, "reprocess_cached_on_not_modified", False))

        self.browser_fallback = QCheckBox("Use browser fallback for blocked pages (Playwright)")
        self.browser_fallback.setChecked(getattr(config.crawl, "use_browser_for_blocked_pages", False))

        self.cookie_header = QLineEdit(getattr(config.crawl, "cookie_header", "") or "")
        self.cookie_header.setPlaceholderText("Optional: paste Cookie header from browser (for Akamai/403 pagination)")

        form = QFormLayout()
        form.addRow("Max concurrency", self.max_conc)
        form.addRow("Rate limit (req/s)", self.rps)
        form.addRow("User-Agent", self.ua)
        form.addRow(self.ocr)
        form.addRow(self.semantic)
        form.addRow("Semantic threshold", self.threshold)
        form.addRow(self.auto_download)
        form.addRow("Stopwords (comma-separated)", self.stopwords)
        form.addRow("Boolean/proximity query", self.query)
        form.addRow(self.allow_offsite)
        form.addRow(self.follow_pages)
        form.addRow(self.age_verify)
        form.addRow(self.reprocess_cached)
        form.addRow(self.browser_fallback)
        form.addRow("Cookie header", self.cookie_header)

        buttons = QHBoxLayout()
        ok = QPushButton("OK")
        cancel = QPushButton("Cancel")
        ok.clicked.connect(self.accept)
        cancel.clicked.connect(self.reject)
        buttons.addStretch(1)
        buttons.addWidget(ok)
        buttons.addWidget(cancel)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addLayout(buttons)
        self.setLayout(layout)

    def updated_config(self) -> AppConfig:
        crawl = CrawlSettings(
            start_url=self._config.crawl.start_url,
            allow_offsite=self.allow_offsite.isChecked(),
            follow_discovered_pages=self.follow_pages.isChecked(),
            max_concurrency=int(self.max_conc.value()),
            requests_per_second=float(self.rps.value()),
            user_agent=self.ua.text().strip(),
            max_retries=self._config.crawl.max_retries,
            backoff_base_seconds=self._config.crawl.backoff_base_seconds,
            ocr_enabled=self.ocr.isChecked(),
            ocr_engine=getattr(self._config.crawl, "ocr_engine", "tesseract"),
            ocr_dpi=int(getattr(self._config.crawl, "ocr_dpi", 200)),
            ocr_preprocess=bool(getattr(self._config.crawl, "ocr_preprocess", True)),
            ocr_median_filter=bool(getattr(self._config.crawl, "ocr_median_filter", True)),
            ocr_threshold=getattr(self._config.crawl, "ocr_threshold", None),
            ner_enabled=bool(getattr(self._config.crawl, "ner_enabled", True)),
            ner_engine=str(getattr(self._config.crawl, "ner_engine", "spacy")),
            ner_spacy_model=str(getattr(self._config.crawl, "ner_spacy_model", "en_core_web_sm")),
            embedding_index_enabled=bool(getattr(self._config.crawl, "embedding_index_enabled", False)),
            embedding_model_name=str(getattr(self._config.crawl, "embedding_model_name", "sentence-transformers/all-MiniLM-L6-v2")),
            storage_layout=str(getattr(self._config.crawl, "storage_layout", "flat")),
            redaction_detection_enabled=bool(getattr(self._config.crawl, "redaction_detection_enabled", True)),
            redaction_page_score_threshold=float(getattr(self._config.crawl, "redaction_page_score_threshold", 0.25)),
            semantic_enabled=self.semantic.isChecked(),
            semantic_threshold=float(self.threshold.value()),
            auto_download=self.auto_download.isChecked(),
            manual_review_only=not self.auto_download.isChecked(),
            age_verify_opt_in=self.age_verify.isChecked(),
            reprocess_cached_on_not_modified=self.reprocess_cached.isChecked(),
            use_browser_for_blocked_pages=self.browser_fallback.isChecked(),
            cookie_header=self.cookie_header.text().strip(),
            stopwords=self.stopwords.text().strip(),
            query=self.query.text().strip(),
            feedback_auto_flag_enabled=bool(getattr(self._config.crawl, "feedback_auto_flag_enabled", True)),
            feedback_auto_flag_threshold=float(getattr(self._config.crawl, "feedback_auto_flag_threshold", 0.22) or 0.22),
            feedback_auto_triage_threshold=float(getattr(self._config.crawl, "feedback_auto_triage_threshold", -0.22) or -0.22),
        )
        return AppConfig(
            paths=self._config.paths,
            crawl=crawl,
            first_run_acknowledged=self._config.first_run_acknowledged,
            last_seed_urls=self._config.last_seed_urls,
        )

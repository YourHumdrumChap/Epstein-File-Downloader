from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QLineEdit,
)


class KeywordsDialog(QDialog):
    def __init__(self, *, keywords_path: Path, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Manage Keywords")
        self._path = keywords_path

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search keywords...")
        self.search.textChanged.connect(self._apply_filter)

        self.list = QListWidget()
        self.list.setSelectionMode(QAbstractItemView.ExtendedSelection)

        self.input = QLineEdit()
        self.input.setPlaceholderText("Enter keyword, wildcard (* ?), or regex as re:<pattern>")

        add_btn = QPushButton("Add")
        remove_btn = QPushButton("Remove")
        import_btn = QPushButton("Import JSON")
        export_btn = QPushButton("Export JSON")
        open_btn = QPushButton("Open Keywords File")
        suggest_btn = QPushButton("Suggest related keywords")

        add_btn.clicked.connect(self._add)
        remove_btn.clicked.connect(self._remove)
        import_btn.clicked.connect(self._import)
        export_btn.clicked.connect(self._export)
        open_btn.clicked.connect(self._open_keywords_file)
        suggest_btn.clicked.connect(self._suggest)

        top = QHBoxLayout()
        top.addWidget(QLabel("Keyword:"))
        top.addWidget(self.input)
        top.addWidget(add_btn)

        buttons = QHBoxLayout()
        buttons.addWidget(remove_btn)
        buttons.addStretch(1)
        buttons.addWidget(suggest_btn)
        buttons.addWidget(open_btn)
        buttons.addWidget(import_btn)
        buttons.addWidget(export_btn)

        layout = QVBoxLayout()
        layout.addLayout(top)
        layout.addWidget(self.search)
        layout.addWidget(self.list)
        layout.addLayout(buttons)
        self.setLayout(layout)

        self._load()
        self._apply_filter(self.search.text())

    def keywords(self) -> list[str]:
        return [self.list.item(i).text() for i in range(self.list.count())]

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            kws = self._keywords_from_json(data)
            for kw in kws:
                self.list.addItem(QListWidgetItem(kw))
        except Exception:
            QMessageBox.warning(self, "Keywords", "Failed to load keywords file; starting empty.")

    def _keywords_from_json(self, data: object) -> list[str]:
        collected: list[tuple[str, bool]] = []

        def add_list(values: object, *, as_regex: bool = False) -> None:
            if not isinstance(values, list):
                return
            for v in values:
                if v is None:
                    continue
                s = str(v).strip()
                if not s:
                    continue
                collected.append((s, as_regex))

        if isinstance(data, list):
            add_list(data, as_regex=False)

        if isinstance(data, dict):
            # Simple/common shapes.
            add_list(data.get("seed_keywords"), as_regex=False)
            add_list(data.get("keywords"), as_regex=False)
            add_list(data.get("terms"), as_regex=False)

            # Keyword-pack shape: categories -> {category: ["term", ...]}
            cats = data.get("categories")
            if isinstance(cats, dict):
                for _name, values in cats.items():
                    add_list(values, as_regex=False)

            # Keyword-pack shape: regex_patterns -> {group: ["<regex>", ...]}
            rx = data.get("regex_patterns")
            if isinstance(rx, dict):
                for _name, values in rx.items():
                    add_list(values, as_regex=True)

            # Keyword-pack shape: wildcards_and_globs.examples -> ["*foo*", ...]
            wag = data.get("wildcards_and_globs")
            if isinstance(wag, dict):
                add_list(wag.get("examples"), as_regex=False)

            # Keyword-pack shape: semantic_hint_tokens -> ["token", ...]
            add_list(data.get("semantic_hint_tokens"), as_regex=False)

            # Keyword-pack shape: euphemism_and_codeword_expansion_rules.example_expansions -> {"seed": ["exp", ...]}
            exp = data.get("euphemism_and_codeword_expansion_rules")
            if isinstance(exp, dict):
                examples = exp.get("example_expansions")
                if isinstance(examples, dict):
                    for _seed, values in examples.items():
                        add_list(values, as_regex=False)

        out: list[str] = []
        seen: set[str] = set()
        for term, is_regex in collected:
            kw = term
            if is_regex:
                kw = kw if kw.startswith("re:") else ("re:" + kw)
            key = kw.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(kw)
        return out

    def _save(self) -> None:
        payload = {
            "version": "1.0",
            "description": "User-managed keyword list for automated triage.",
            "seed_keywords": self.keywords(),
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _apply_filter(self, text: str) -> None:
        q = (text or "").strip().lower()
        for i in range(self.list.count()):
            it = self.list.item(i)
            if not q:
                it.setHidden(False)
            else:
                it.setHidden(q not in it.text().lower())

    def _add(self) -> None:
        kw = self.input.text().strip()
        if not kw:
            return
        self.list.addItem(QListWidgetItem(kw))
        self.input.clear()
        self._save()
        self._apply_filter(self.search.text())

    def _remove(self) -> None:
        for item in self.list.selectedItems():
            self.list.takeItem(self.list.row(item))
        self._save()
        self._apply_filter(self.search.text())

    def _import(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(self, "Import keywords", str(self._path.parent), "JSON (*.json)")
        if not paths:
            return

        merged: list[str] = []
        seen: set[str] = set()
        failed: list[str] = []

        for p in paths:
            import_path = Path(p)
            try:
                data = json.loads(import_path.read_text(encoding="utf-8"))
                kws = self._keywords_from_json(data)
                for kw in kws:
                    key = kw.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    merged.append(kw)
            except Exception:
                failed.append(import_path.name)

        if not merged and failed:
            QMessageBox.warning(
                self,
                "Import",
                "No keywords were imported. The selected files may be invalid JSON or an unsupported format.\n\n"
                + "Failed: "
                + ", ".join(failed),
            )
            return

        self.list.clear()
        for kw in merged:
            self.list.addItem(QListWidgetItem(kw))
        self._save()
        self._apply_filter(self.search.text())

        msg = f"Imported {len(merged)} unique keywords from {len(paths)} file(s) into {self._path.name}."
        if failed:
            msg += "\n\nFailed to read: " + ", ".join(failed)
        QMessageBox.information(self, "Import", msg)

    def _export(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Export keywords", str(self._path.parent / "keywords.json"), "JSON (*.json)")
        if not path:
            return
        out = Path(path)
        payload = {
            "version": "1.0",
            "description": "Exported keyword list.",
            "seed_keywords": self.keywords(),
        }
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _open_keywords_file(self) -> None:
        # Ensure the file exists so VS Code can open it.
        if not self._path.exists():
            self._save()

        def candidates() -> list[str]:
            # 1) PATH lookup
            found: list[str] = []
            for name in ("code", "code.cmd", "code-insiders", "code-insiders.cmd"):
                p = shutil.which(name)
                if p:
                    found.append(p)

            # 2) Common Windows install locations
            localappdata = os.environ.get("LOCALAPPDATA", "")
            programfiles = os.environ.get("ProgramFiles", "")
            programfiles_x86 = os.environ.get("ProgramFiles(x86)", "")

            common = [
                os.path.join(localappdata, "Programs", "Microsoft VS Code", "bin", "code.cmd"),
                os.path.join(localappdata, "Programs", "Microsoft VS Code Insiders", "bin", "code-insiders.cmd"),
                os.path.join(programfiles, "Microsoft VS Code", "bin", "code.cmd"),
                os.path.join(programfiles_x86, "Microsoft VS Code", "bin", "code.cmd"),
            ]
            for p in common:
                if p and os.path.exists(p):
                    found.append(p)

            # De-dupe while preserving order
            out: list[str] = []
            seen: set[str] = set()
            for p in found:
                key = os.path.normcase(os.path.abspath(p))
                if key in seen:
                    continue
                seen.add(key)
                out.append(p)
            return out

        last_exc: Exception | None = None
        for exe in candidates():
            try:
                subprocess.Popen([exe, "--reuse-window", str(self._path)])
                return
            except Exception as e:
                last_exc = e
                continue

        # Fall back to the OS default opener (often VS Code if file associations are set).
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(self._path))  # type: ignore[attr-defined]
                return
            if sys.platform == "darwin":
                subprocess.Popen(["open", str(self._path)])
                return
            subprocess.Popen(["xdg-open", str(self._path)])
            return
        except Exception as e:
            last_exc = e

        msg = (
            "Could not launch VS Code from this app.\n\n"
            "Fix options on Windows:\n"
            "1) Re-run the VS Code installer and enable 'Add to PATH'\n"
            "2) Or run VS Code once and ensure the CLI is available\n\n"
            f"Keywords file is located at:\n{self._path}"
        )
        if last_exc is not None:
            msg += f"\n\nLast error: {last_exc}"
        QMessageBox.information(self, "Open in VS Code", msg)

    def _suggest(self) -> None:
        try:
            from doj_disclosures.core.semantic import SemanticMatcher

            sm = SemanticMatcher(threshold=0.0)
            suggestions = sm.suggest_related(self.keywords(), k=10)
            if not suggestions:
                QMessageBox.information(self, "Suggest", "No suggestions available (need more keywords).")
                return
            existing = {self.list.item(i).text().strip().lower() for i in range(self.list.count())}
            added = 0
            for kw in sorted(suggestions):
                s = str(kw).strip()
                if not s:
                    continue
                key = s.lower()
                if key in existing:
                    continue
                existing.add(key)
                self.list.addItem(QListWidgetItem(s))
                added += 1

            if added:
                self._save()
                QMessageBox.information(self, "Suggest", f"Added {added} suggested keyword(s) to the list.")
            else:
                QMessageBox.information(self, "Suggest", "All suggested keywords were already in the list.")
        except Exception:
            QMessageBox.information(
                self,
                "Suggest",
                "Semantic suggestions require optional semantic dependencies.\nInstall with: pip install -e '.[semantic]'.",
            )

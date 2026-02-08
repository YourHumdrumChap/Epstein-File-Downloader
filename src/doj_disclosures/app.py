from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication

from doj_disclosures.core.config import AppConfig
from doj_disclosures.core.db import Database
from doj_disclosures.core.logging_config import configure_logging
from doj_disclosures.gui.main_window import MainWindow


def _ensure_qt_plugins_without_accessibility(*, config: AppConfig) -> Path:
    """Create a Qt plugin root that omits the accessibility bridge plugins.

    On some Windows + Python 3.13 environments, Qt's accessibility bridge can
    trigger a fatal COM exception (0x8001010d) as the event loop starts.

    This copies PySide6 plugin subfolders into the app data directory, excluding
    `accessible` and `accessiblebridge`, and then we point Qt's library paths at
    that directory.
    """

    import PySide6

    src_root = Path(PySide6.__file__).resolve().parent / "plugins"
    dest_root = config.paths.app_dir / "qt_plugins" / f"pyside6-{PySide6.__version__}"
    marker = dest_root / ".ready"

    if marker.exists():
        return dest_root

    dest_root.mkdir(parents=True, exist_ok=True)

    include_dirs = [
        "platforms",
        "styles",
        "imageformats",
        "iconengines",
        "platformthemes",
        "tls",
    ]

    for name in include_dirs:
        src = src_root / name
        if not src.exists():
            continue
        dst = dest_root / name
        if dst.exists():
            shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)

    marker.write_text("ok", encoding="utf-8")
    return dest_root


def _configure_qt_library_paths(*, config: AppConfig) -> None:
    """Apply the plugin-path workaround before QApplication is created."""

    if os.name != "nt":
        return

    # Only do this on the known-bad Python 3.13 line.
    if sys.version_info[:2] != (3, 13):
        return

    try:
        from PySide6.QtCore import QCoreApplication

        plugin_root = _ensure_qt_plugins_without_accessibility(config=config)
        QCoreApplication.setLibraryPaths([str(plugin_root)])
    except Exception:
        # If anything goes wrong, fail open (Qt will use default plugin paths).
        return


def main() -> int:
    config = AppConfig.load()
    configure_logging(config)

    _configure_qt_library_paths(config=config)

    app = QApplication(sys.argv)
    app.setApplicationName("DOJ Disclosures Crawler")
    app.setOrganizationName("Local")

    db = Database(config.paths.db_path)
    db.initialize_sync()

    window = MainWindow(config=config, db=db)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

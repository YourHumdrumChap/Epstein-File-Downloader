from __future__ import annotations

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt


class StatusTableModel(QAbstractTableModel):
    def __init__(self) -> None:
        super().__init__()
        self._rows: list[tuple[str, str]] = []

    def rowCount(self, parent: QModelIndex | None = None) -> int:  # type: ignore[override]
        return len(self._rows)

    def columnCount(self, parent: QModelIndex | None = None) -> int:  # type: ignore[override]
        return 2

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):  # type: ignore[override]
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return ["URL", "Status"][section]
        return section + 1

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):  # type: ignore[override]
        if not index.isValid() or role != Qt.DisplayRole:
            return None
        url, st = self._rows[index.row()]
        return url if index.column() == 0 else st

    def upsert(self, url: str, status: str) -> None:
        for i, (u, _) in enumerate(self._rows):
            if u == url:
                self._rows[i] = (url, status)
                self.dataChanged.emit(self.index(i, 0), self.index(i, 1))
                return
        self.beginInsertRows(QModelIndex(), len(self._rows), len(self._rows))
        self._rows.append((url, status))
        self.endInsertRows()

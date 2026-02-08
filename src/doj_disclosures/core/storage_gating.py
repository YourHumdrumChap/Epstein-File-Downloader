from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from doj_disclosures.core.utils import atomic_rename, safe_filename


@dataclass(frozen=True)
class StoragePlan:
    raw_dir: Path
    triaged_dir: Path
    flagged_dir: Path


def plan_storage(output_dir: Path) -> StoragePlan:
    cache = output_dir / "cache"
    raw_dir = cache / "raw"
    triaged_dir = cache / "triaged"
    flagged_dir = output_dir / "flagged"
    raw_dir.mkdir(parents=True, exist_ok=True)
    triaged_dir.mkdir(parents=True, exist_ok=True)
    flagged_dir.mkdir(parents=True, exist_ok=True)
    (flagged_dir / "high_value").mkdir(parents=True, exist_ok=True)
    (flagged_dir / "irrelevant").mkdir(parents=True, exist_ok=True)
    return StoragePlan(raw_dir=raw_dir, triaged_dir=triaged_dir, flagged_dir=flagged_dir)


def compute_flagged_path(
    *,
    flagged_dir: Path,
    sha256: str,
    suffix: str,
    storage_layout: str,
    display_name: str | None = None,
) -> Path:
    """Compute the on-disk path for a flagged file.

    Historically we used the full SHA256 as the filename, which is safe but not
    human-friendly. We now prefer a readable name (title/original filename) plus
    a short SHA suffix for uniqueness.
    """

    suf = suffix if suffix.startswith(".") or suffix == "" else f".{suffix}"
    layout = (storage_layout or "flat").strip().lower()

    # Derive a friendly basename.
    raw = (display_name or "").strip()
    if raw:
        # If caller included an extension, strip it when it matches the target suffix.
        if suf and raw.lower().endswith(suf.lower()):
            raw = raw[: -len(suf)]
    else:
        raw = "file"

    short = (sha256 or "").strip()[:10]
    if short:
        base = safe_filename(f"{raw}__{short}", max_len=110)
    else:
        base = safe_filename(raw, max_len=110)

    filename = f"{base}{suf}"

    if layout == "hashed" and sha256:
        subdir = flagged_dir / sha256[:2] / sha256[2:4]
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir / filename
    return flagged_dir / filename


def move_to(dst: Path, src: Path) -> Path:
    if src.resolve() == dst.resolve():
        return dst
    atomic_rename(src, dst)
    return dst

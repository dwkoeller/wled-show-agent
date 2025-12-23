from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict


@dataclass(frozen=True)
class CacheEntry:
    path: Path
    size: int
    mtime: float


def _scan_cache(path: Path) -> List[CacheEntry]:
    entries: List[CacheEntry] = []
    try:
        if not path.exists():
            return entries
    except Exception:
        return entries
    try:
        for entry in os.scandir(path):
            if not entry.is_file():
                continue
            try:
                st = entry.stat()
            except Exception:
                continue
            entries.append(
                CacheEntry(
                    path=Path(entry.path),
                    size=int(st.st_size or 0),
                    mtime=float(st.st_mtime or 0.0),
                )
            )
    except FileNotFoundError:
        return entries
    except Exception:
        return entries
    return entries


def cache_stats(path: Path) -> Dict[str, int]:
    entries = _scan_cache(path)
    total_bytes = sum(int(e.size) for e in entries)
    return {"files": len(entries), "bytes": int(total_bytes)}


def cleanup_cache(
    path: Path,
    *,
    max_bytes: int | None = None,
    max_days: float | None = None,
    purge: bool = False,
) -> Dict[str, int]:
    entries = _scan_cache(path)
    before_bytes = sum(int(e.size) for e in entries)
    deleted_files = 0
    deleted_bytes = 0

    def _delete(entry: CacheEntry) -> None:
        nonlocal deleted_files, deleted_bytes
        try:
            entry.path.unlink()
            deleted_files += 1
            deleted_bytes += int(entry.size)
        except Exception:
            return

    if purge:
        for entry in entries:
            _delete(entry)
        after = cache_stats(path)
        return {
            "deleted_files": int(deleted_files),
            "deleted_bytes": int(deleted_bytes),
            "before_bytes": int(before_bytes),
            "after_bytes": int(after.get("bytes", 0)),
        }

    now = time.time()
    if max_days is not None and float(max_days) > 0:
        cutoff = now - (float(max_days) * 86400.0)
        for entry in list(entries):
            if entry.mtime and entry.mtime < cutoff:
                _delete(entry)

    entries = _scan_cache(path)
    total_bytes = sum(int(e.size) for e in entries)
    if max_bytes is not None and int(max_bytes) > 0 and total_bytes > int(max_bytes):
        entries.sort(key=lambda e: (e.mtime, e.path.name))
        target = int(max_bytes)
        for entry in entries:
            if total_bytes <= target:
                break
            _delete(entry)
            total_bytes -= int(entry.size)

    after = cache_stats(path)
    return {
        "deleted_files": int(deleted_files),
        "deleted_bytes": int(deleted_bytes),
        "before_bytes": int(before_bytes),
        "after_bytes": int(after.get("bytes", 0)),
    }

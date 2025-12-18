from __future__ import annotations

import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlmodel import Session

from pack_io import read_json
from services.state import AppState
from sql_store import AudioAnalysisRecord, PackIngestRecord, SequenceMetaRecord


def _now() -> float:
    return time.time()


def _clamp_limit(limit: int, *, default: int = 5000, max_limit: int = 200_000) -> int:
    try:
        n = int(limit)
    except Exception:
        n = default
    return max(1, min(int(max_limit), n))


@dataclass(frozen=True)
class _PackIngestMeta:
    dest_dir: str
    manifest_rel_path: str
    created_at: float
    updated_at: float
    uploaded_bytes: int
    unpacked_bytes: int
    file_count: int


@dataclass(frozen=True)
class _SequenceMeta:
    file: str
    created_at: float
    updated_at: float
    duration_s: float
    steps_total: int


@dataclass(frozen=True)
class _AudioMeta:
    analysis_id: str
    created_at: float
    updated_at: float
    beats_path: str
    bpm: float | None
    beat_count: int | None


def _safe_rel(root: Path, p: Path) -> str:
    try:
        return str(p.resolve().relative_to(root.resolve()))
    except Exception:
        return str(p)


def _scan_pack_manifests(root: Path, *, limit: int) -> List[_PackIngestMeta]:
    out: List[_PackIngestMeta] = []
    if not root.exists():
        return out

    # Pack ingestion writes `<dest_dir>/manifest.json`. Avoid following symlinks.
    seen = 0
    for dirpath, _dirnames, filenames in os.walk(root, followlinks=False):
        if seen >= limit:
            break
        if "manifest.json" not in filenames:
            continue
        p = Path(dirpath) / "manifest.json"
        if not p.is_file():
            continue
        try:
            raw = read_json(str(p))
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        if "ingest_id" not in raw:
            continue

        dest_dir = str(raw.get("dest_dir") or "").strip().strip("/")
        if not dest_dir:
            try:
                dest_dir = str(p.parent.resolve().relative_to(root.resolve()))
            except Exception:
                dest_dir = str(p.parent)

        try:
            st = p.stat()
            updated_at = float(getattr(st, "st_mtime", 0.0) or 0.0)
            created_at = float(getattr(st, "st_ctime", updated_at) or updated_at)
        except Exception:
            created_at = _now()
            updated_at = created_at

        manifest_created = raw.get("created_at")
        if manifest_created is not None:
            try:
                created_at = float(manifest_created)
            except Exception:
                pass

        uploaded_bytes = int(raw.get("uploaded_bytes") or 0)
        unpacked_bytes = int(raw.get("unpacked_bytes") or 0)
        files = raw.get("files")
        if isinstance(files, list):
            file_count = len([x for x in files if x is not None])
        else:
            file_count = int(raw.get("file_count") or 0)

        out.append(
            _PackIngestMeta(
                dest_dir=dest_dir,
                manifest_rel_path=_safe_rel(root, p),
                created_at=created_at,
                updated_at=updated_at,
                uploaded_bytes=uploaded_bytes,
                unpacked_bytes=unpacked_bytes,
                file_count=file_count,
            )
        )
        seen += 1

    return out


def _scan_sequences(root: Path, *, limit: int) -> List[_SequenceMeta]:
    out: List[_SequenceMeta] = []
    seq_dir = (root / "sequences").resolve()
    if not seq_dir.is_dir():
        return out

    seen = 0
    for dirpath, _dirnames, filenames in os.walk(seq_dir, followlinks=False):
        for name in filenames:
            if seen >= limit:
                break
            if not (name.startswith("sequence_") and name.endswith(".json")):
                continue
            p = Path(dirpath) / name
            if not p.is_file():
                continue

            try:
                st = p.stat()
                updated_at = float(getattr(st, "st_mtime", 0.0) or 0.0)
                created_at = float(getattr(st, "st_ctime", updated_at) or updated_at)
            except Exception:
                created_at = _now()
                updated_at = created_at

            duration_s = 0.0
            steps_total = 0
            try:
                raw = read_json(str(p))
                steps: Any = None
                if isinstance(raw, dict):
                    steps = raw.get("steps")
                elif isinstance(raw, list):
                    steps = raw
                if isinstance(steps, list):
                    steps_total = len([x for x in steps if x is not None])
                    for step in steps:
                        if not isinstance(step, dict):
                            continue
                        d = step.get("duration_s")
                        if d is None:
                            continue
                        try:
                            duration_s += float(d)
                        except Exception:
                            continue
            except Exception:
                pass

            try:
                rel = str(p.resolve().relative_to(seq_dir.resolve()))
            except Exception:
                rel = str(p.name)

            out.append(
                _SequenceMeta(
                    file=rel,
                    created_at=created_at,
                    updated_at=updated_at,
                    duration_s=float(duration_s),
                    steps_total=int(steps_total),
                )
            )
            seen += 1
        if seen >= limit:
            break

    return out


def _scan_audio_analyses(root: Path, *, limit: int) -> List[_AudioMeta]:
    out: List[_AudioMeta] = []
    audio_dir = (root / "audio").resolve()
    if not audio_dir.is_dir():
        return out

    seen = 0
    for dirpath, _dirnames, filenames in os.walk(audio_dir, followlinks=False):
        for name in filenames:
            if seen >= limit:
                break
            if not name.endswith(".json"):
                continue
            p = Path(dirpath) / name
            if not p.is_file():
                continue

            # Heuristic: beat analysis files contain beats_s + duration_s.
            bpm: float | None = None
            beat_count: int | None = None
            try:
                raw = read_json(str(p))
                if not isinstance(raw, dict):
                    continue
                beats = raw.get("beats_s")
                if not isinstance(beats, list):
                    continue
                if "duration_s" not in raw:
                    continue
                beat_count = len([x for x in beats if x is not None])
                if raw.get("bpm") is not None:
                    try:
                        bpm = float(raw.get("bpm"))
                    except Exception:
                        bpm = None
            except Exception:
                continue

            rel_path = _safe_rel(root, p)
            analysis_id = hashlib.sha1(rel_path.encode("utf-8")).hexdigest()[:32]
            try:
                st = p.stat()
                updated_at = float(getattr(st, "st_mtime", 0.0) or 0.0)
                created_at = float(getattr(st, "st_ctime", updated_at) or updated_at)
            except Exception:
                created_at = _now()
                updated_at = created_at

            out.append(
                _AudioMeta(
                    analysis_id=analysis_id,
                    created_at=created_at,
                    updated_at=updated_at,
                    beats_path=rel_path,
                    bpm=bpm,
                    beat_count=beat_count,
                )
            )
            seen += 1
        if seen >= limit:
            break

    return out


def _upsert_pack_ingests(
    session: Session, *, agent_id: str, items: List[_PackIngestMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.dest_dir))
        rec = session.get(PackIngestRecord, key)
        if rec is None:
            rec = PackIngestRecord(
                agent_id=agent_id,
                dest_dir=str(it.dest_dir),
                created_at=float(it.created_at),
                updated_at=float(it.updated_at),
                source_name=None,
                manifest_path=str(it.manifest_rel_path),
                uploaded_bytes=int(it.uploaded_bytes),
                unpacked_bytes=int(it.unpacked_bytes),
                file_count=int(it.file_count),
            )
            session.add(rec)
        else:
            rec.updated_at = float(it.updated_at) or rec.updated_at
            rec.manifest_path = str(it.manifest_rel_path)
            rec.uploaded_bytes = int(it.uploaded_bytes)
            rec.unpacked_bytes = int(it.unpacked_bytes)
            rec.file_count = int(it.file_count)
        upserted += 1
    return upserted


def _upsert_sequences(
    session: Session, *, agent_id: str, items: List[_SequenceMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.file))
        rec = session.get(SequenceMetaRecord, key)
        if rec is None:
            rec = SequenceMetaRecord(
                agent_id=agent_id,
                file=str(it.file),
                created_at=float(it.created_at),
                updated_at=float(it.updated_at),
                duration_s=float(it.duration_s),
                steps_total=int(it.steps_total),
            )
            session.add(rec)
        else:
            rec.updated_at = float(it.updated_at) or rec.updated_at
            rec.duration_s = float(it.duration_s)
            rec.steps_total = int(it.steps_total)
        upserted += 1
    return upserted


def _upsert_audio_analyses(
    session: Session, *, agent_id: str, items: List[_AudioMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.analysis_id))
        rec = session.get(AudioAnalysisRecord, key)
        if rec is None:
            rec = AudioAnalysisRecord(
                agent_id=agent_id,
                id=str(it.analysis_id),
                created_at=float(it.created_at),
                updated_at=float(it.updated_at),
                source_path=None,
                beats_path=str(it.beats_path),
                prefer_ffmpeg=False,
                bpm=float(it.bpm) if it.bpm is not None else None,
                beat_count=int(it.beat_count) if it.beat_count is not None else None,
                error=None,
            )
            session.add(rec)
        else:
            rec.updated_at = float(it.updated_at) or rec.updated_at
            rec.beats_path = str(it.beats_path)
            rec.bpm = float(it.bpm) if it.bpm is not None else rec.bpm
            rec.beat_count = (
                int(it.beat_count) if it.beat_count is not None else rec.beat_count
            )
        upserted += 1
    return upserted


async def reconcile_data_dir(
    state: AppState,
    *,
    packs: bool = True,
    sequences: bool = True,
    audio: bool = False,
    scan_limit: int = 5000,
) -> Dict[str, Any]:
    """
    Best-effort reconciliation: scan DATA_DIR and upsert SQL metadata for UI queries.
    """
    db = getattr(state, "db", None)
    if db is None:
        return {"ok": False, "skipped": True, "reason": "DATABASE_URL not configured"}

    root = Path(str(state.settings.data_dir)).resolve()
    lim = _clamp_limit(int(scan_limit))

    def _scan() -> Tuple[List[_PackIngestMeta], List[_SequenceMeta], List[_AudioMeta]]:
        pack_items = _scan_pack_manifests(root, limit=lim) if packs else []
        seq_items = _scan_sequences(root, limit=lim) if sequences else []
        audio_items = _scan_audio_analyses(root, limit=lim) if audio else []
        return pack_items, seq_items, audio_items

    pack_items, seq_items, audio_items = await asyncio.to_thread(_scan)

    upserted_packs = 0
    upserted_sequences = 0
    upserted_audio = 0

    def _write() -> Tuple[int, int, int]:
        with Session(db.engine) as session:
            up_p = (
                _upsert_pack_ingests(session, agent_id=db.agent_id, items=pack_items)
                if pack_items
                else 0
            )
            up_s = (
                _upsert_sequences(session, agent_id=db.agent_id, items=seq_items)
                if seq_items
                else 0
            )
            up_a = (
                _upsert_audio_analyses(session, agent_id=db.agent_id, items=audio_items)
                if audio_items
                else 0
            )
            session.commit()
            return up_p, up_s, up_a

    upserted_packs, upserted_sequences, upserted_audio = await asyncio.to_thread(_write)

    return {
        "ok": True,
        "scanned": {
            "packs": len(pack_items),
            "sequences": len(seq_items),
            "audio": len(audio_items),
        },
        "upserted": {
            "packs": upserted_packs,
            "sequences": upserted_sequences,
            "audio": upserted_audio,
        },
    }

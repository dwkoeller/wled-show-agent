from __future__ import annotations

import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlmodel.ext.asyncio.session import AsyncSession

from pack_io import read_json
from services.state import AppState
from services.precompute_service import schedule_precompute
from show_config import ShowConfig
from utils.blocking import run_cpu_blocking_state
from sql_store import (
    AudioAnalysisRecord,
    FppScriptRecord,
    FseqExportRecord,
    PackIngestRecord,
    SequenceMetaRecord,
    ShowConfigRecord,
)

RECONCILE_STATUS_KEY = "meta_reconcile_status"


def _now() -> float:
    return time.time()


class ReconcileCancelled(Exception):
    pass


async def _set_reconcile_status(state: AppState, status: Dict[str, Any]) -> None:
    db = getattr(state, "db", None)
    if db is None:
        return
    try:
        await db.kv_set_json(RECONCILE_STATUS_KEY, dict(status))
    except Exception:
        return


async def get_reconcile_status(state: AppState) -> Dict[str, Any] | None:
    db = getattr(state, "db", None)
    if db is None:
        return None
    try:
        row = await db.kv_get_json(RECONCILE_STATUS_KEY)
        return dict(row or {}) if row else None
    except Exception:
        return None


async def run_reconcile_with_status(
    state: AppState,
    *,
    mode: str,
    packs: bool = True,
    sequences: bool = True,
    audio: bool = False,
    show_configs: bool = True,
    fseq_exports: bool = True,
    fpp_scripts: bool = True,
    scan_limit: int = 5000,
    precompute_previews: bool = False,
    precompute_waveforms: bool = False,
) -> Dict[str, Any]:
    started_at = _now()
    db = getattr(state, "db", None)
    cancel_event = getattr(state, "reconcile_cancel_event", None)
    if cancel_event is not None and cancel_event.is_set():
        cancel_event.clear()
    run_id = None
    if db is not None:
        try:
            rec = await db.create_reconcile_run(
                source=str(mode or "manual"),
                options={
                    "packs": bool(packs),
                    "sequences": bool(sequences),
                    "audio": bool(audio),
                    "show_configs": bool(show_configs),
                    "fseq_exports": bool(fseq_exports),
                    "fpp_scripts": bool(fpp_scripts),
                    "scan_limit": int(scan_limit),
                    "precompute_previews": bool(precompute_previews),
                    "precompute_waveforms": bool(precompute_waveforms),
                },
            )
            run_id = rec.get("id")
            state.reconcile_run_id = run_id
        except Exception:
            run_id = None
    try:
        state.reconcile_task = asyncio.current_task()
    except Exception:
        state.reconcile_task = None
    status: Dict[str, Any] = {
        "ok": True,
        "running": True,
        "mode": str(mode or "manual"),
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": None,
        "duration_s": None,
        "last_error": None,
        "last_result": None,
        "cancel_requested": False,
        "phase": "starting",
        "params": {
            "packs": bool(packs),
            "sequences": bool(sequences),
            "audio": bool(audio),
            "show_configs": bool(show_configs),
            "fseq_exports": bool(fseq_exports),
            "fpp_scripts": bool(fpp_scripts),
            "scan_limit": int(scan_limit),
            "precompute_previews": bool(precompute_previews),
            "precompute_waveforms": bool(precompute_waveforms),
        },
    }
    await _set_reconcile_status(state, status)
    try:
        from services.events_service import emit_event

        await emit_event(
            state,
            event_type="meta",
            data={
                "event": "reconcile_start",
                "mode": status.get("mode"),
                "run_id": run_id,
                "started_at": started_at,
            },
        )
    except Exception:
        pass
    result: Dict[str, Any] | None = None
    error: str | None = None
    try:
        if cancel_event is not None and cancel_event.is_set():
            raise ReconcileCancelled()
        status["phase"] = "scanning"
        await _set_reconcile_status(state, status)
        result = await reconcile_data_dir(
            state,
            packs=packs,
            sequences=sequences,
            audio=audio,
            show_configs=show_configs,
            fseq_exports=fseq_exports,
            fpp_scripts=fpp_scripts,
            scan_limit=scan_limit,
            cancel_event=cancel_event,
        )
        if result and bool(result.get("ok")):
            try:
                precompute_job = None
                if precompute_previews or precompute_waveforms:
                    precompute_job = await schedule_precompute(
                        state,
                        reason="reconcile",
                        sequences=None if precompute_previews else [],
                        audio_files=None if precompute_waveforms else [],
                        scan_limit=int(scan_limit),
                    )
                if precompute_job is not None:
                    result["precompute_job"] = precompute_job
            except Exception:
                pass
        return result
    except ReconcileCancelled:
        error = "cancelled"
        result = {"ok": False, "cancelled": True}
        return result
    except Exception as e:
        error = str(e)
        raise
    finally:
        finished_at = _now()
        duration_s = max(0.0, finished_at - started_at)
        status["running"] = False
        status["phase"] = "finished"
        status["finished_at"] = finished_at
        status["duration_s"] = duration_s
        if error:
            status["ok"] = False
            status["last_error"] = error
            if error == "cancelled":
                status["cancel_requested"] = True
        if result is not None:
            status["last_result"] = result
            if bool(result.get("ok")):
                status["last_success_at"] = finished_at
        await _set_reconcile_status(state, status)
        try:
            from services.events_service import emit_event

            await emit_event(
                state,
                event_type="meta",
                data={
                    "event": "reconcile_finished",
                    "mode": status.get("mode"),
                    "run_id": run_id,
                    "ok": bool(status.get("ok")),
                    "finished_at": finished_at,
                    "duration_s": duration_s,
                    "error": status.get("last_error"),
                },
            )
        except Exception:
            pass
        if db is not None and run_id is not None:
            try:
                await db.update_reconcile_run(
                    run_id=int(run_id),
                    status="cancelled" if error == "cancelled" else ("ok" if not error else "error"),
                    error=None if error == "cancelled" else error,
                    finished_at=finished_at,
                    cancel_requested=bool(
                        status.get("cancel_requested") or error == "cancelled"
                    ),
                    result=result,
                )
            except Exception:
                pass
        state.reconcile_run_id = None
        state.reconcile_task = None


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


@dataclass(frozen=True)
class _ShowConfigMeta:
    file: str
    created_at: float
    updated_at: float
    name: str
    props_total: int
    groups_total: int
    coordinator_base_url: str | None
    fpp_base_url: str | None
    payload: Dict[str, Any]


@dataclass(frozen=True)
class _FseqExportMeta:
    file: str
    created_at: float
    updated_at: float
    bytes_written: int
    frames: int | None
    channels: int | None
    step_ms: int | None
    duration_s: float | None


@dataclass(frozen=True)
class _FppScriptMeta:
    file: str
    created_at: float
    updated_at: float
    kind: str
    bytes_written: int
    payload: Dict[str, Any]


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


def _parse_fseq_header(p: Path) -> tuple[int | None, int | None, int | None]:
    try:
        with p.open("rb") as f:
            hdr = f.read(20)
    except Exception:
        return None, None, None
    if len(hdr) < 19:
        return None, None, None
    if hdr[0:4] != b"PSEQ":
        return None, None, None
    try:
        channels = int.from_bytes(hdr[10:14], "little", signed=False)
        frames = int.from_bytes(hdr[14:18], "little", signed=False)
        step_ms = int(hdr[18])
        return frames, channels, step_ms
    except Exception:
        return None, None, None


def _scan_show_configs(root: Path, *, limit: int) -> List[_ShowConfigMeta]:
    out: List[_ShowConfigMeta] = []
    show_dir = (root / "show").resolve()
    if not show_dir.is_dir():
        return out

    seen = 0
    for dirpath, _dirnames, filenames in os.walk(show_dir, followlinks=False):
        for name in filenames:
            if seen >= limit:
                break
            if not name.endswith(".json"):
                continue
            p = Path(dirpath) / name
            if not p.is_file():
                continue

            try:
                raw = read_json(str(p))
                cfg = ShowConfig.model_validate(raw)
            except Exception:
                continue

            try:
                st = p.stat()
                updated_at = float(getattr(st, "st_mtime", 0.0) or 0.0)
                created_at = float(getattr(st, "st_ctime", updated_at) or updated_at)
            except Exception:
                created_at = _now()
                updated_at = created_at

            try:
                rel = str(p.resolve().relative_to(show_dir.resolve()))
            except Exception:
                rel = str(p.name)

            props_by_kind: Dict[str, int] = {}
            for prop in cfg.props:
                k = str(getattr(prop, "kind", "") or "").strip().lower() or "unknown"
                props_by_kind[k] = props_by_kind.get(k, 0) + 1

            payload = {
                "subnet": cfg.subnet,
                "channels_per_universe": cfg.channels_per_universe,
                "props_by_kind": props_by_kind,
            }

            out.append(
                _ShowConfigMeta(
                    file=rel,
                    created_at=created_at,
                    updated_at=updated_at,
                    name=str(cfg.name or ""),
                    props_total=len(cfg.props),
                    groups_total=len(cfg.groups or {}),
                    coordinator_base_url=str(cfg.coordinator.base_url or "").strip()
                    or None,
                    fpp_base_url=str(cfg.fpp.base_url or "").strip() or None,
                    payload=payload,
                )
            )
            seen += 1
        if seen >= limit:
            break

    return out


def _scan_fseq_exports(root: Path, *, limit: int) -> List[_FseqExportMeta]:
    out: List[_FseqExportMeta] = []
    fseq_dir = (root / "fseq").resolve()
    if not fseq_dir.is_dir():
        return out

    seen = 0
    for dirpath, _dirnames, filenames in os.walk(fseq_dir, followlinks=False):
        for name in filenames:
            if seen >= limit:
                break
            if not name.endswith(".fseq"):
                continue
            p = Path(dirpath) / name
            if not p.is_file():
                continue

            try:
                st = p.stat()
                updated_at = float(getattr(st, "st_mtime", 0.0) or 0.0)
                created_at = float(getattr(st, "st_ctime", updated_at) or updated_at)
                bytes_written = int(getattr(st, "st_size", 0) or 0)
            except Exception:
                created_at = _now()
                updated_at = created_at
                bytes_written = 0

            frames, channels, step_ms = _parse_fseq_header(p)
            duration_s = (
                (float(frames) * float(step_ms) / 1000.0)
                if frames is not None and step_ms is not None
                else None
            )

            try:
                rel = str(p.resolve().relative_to(fseq_dir.resolve()))
            except Exception:
                rel = str(p.name)

            out.append(
                _FseqExportMeta(
                    file=rel,
                    created_at=created_at,
                    updated_at=updated_at,
                    bytes_written=bytes_written,
                    frames=frames,
                    channels=channels,
                    step_ms=step_ms,
                    duration_s=duration_s,
                )
            )
            seen += 1
        if seen >= limit:
            break

    return out


def _scan_fpp_scripts(root: Path, *, limit: int) -> List[_FppScriptMeta]:
    out: List[_FppScriptMeta] = []
    scripts_dir = (root / "fpp" / "scripts").resolve()
    if not scripts_dir.is_dir():
        return out

    seen = 0
    for dirpath, _dirnames, filenames in os.walk(scripts_dir, followlinks=False):
        for name in filenames:
            if seen >= limit:
                break
            if not name.endswith(".sh"):
                continue
            p = Path(dirpath) / name
            if not p.is_file():
                continue

            try:
                st = p.stat()
                updated_at = float(getattr(st, "st_mtime", 0.0) or 0.0)
                created_at = float(getattr(st, "st_ctime", updated_at) or updated_at)
                bytes_written = int(getattr(st, "st_size", 0) or 0)
            except Exception:
                created_at = _now()
                updated_at = created_at
                bytes_written = 0

            kind = "custom"
            payload: Dict[str, Any] = {}
            try:
                raw = p.read_text(encoding="utf-8", errors="ignore")
                if "/v1/fleet/sequences/start" in raw:
                    kind = "fleet_sequence_start"
                elif "/v1/fleet/stop_all" in raw:
                    kind = "fleet_stop_all"
            except Exception:
                pass

            try:
                rel = str(p.resolve().relative_to(scripts_dir.resolve()))
            except Exception:
                rel = str(p.name)

            out.append(
                _FppScriptMeta(
                    file=rel,
                    created_at=created_at,
                    updated_at=updated_at,
                    kind=kind,
                    bytes_written=bytes_written,
                    payload=payload,
                )
            )
            seen += 1
        if seen >= limit:
            break

    return out


def scan_data_dir(
    *,
    root_path: str,
    packs: bool,
    sequences: bool,
    audio: bool,
    show_configs: bool,
    fseq_exports: bool,
    fpp_scripts: bool,
    limit: int,
) -> Tuple[
    List[_PackIngestMeta],
    List[_SequenceMeta],
    List[_AudioMeta],
    List[_ShowConfigMeta],
    List[_FseqExportMeta],
    List[_FppScriptMeta],
]:
    root = Path(str(root_path)).resolve()
    pack_items = _scan_pack_manifests(root, limit=limit) if packs else []
    seq_items = _scan_sequences(root, limit=limit) if sequences else []
    audio_items = _scan_audio_analyses(root, limit=limit) if audio else []
    show_items = _scan_show_configs(root, limit=limit) if show_configs else []
    fseq_items = _scan_fseq_exports(root, limit=limit) if fseq_exports else []
    script_items = _scan_fpp_scripts(root, limit=limit) if fpp_scripts else []
    return pack_items, seq_items, audio_items, show_items, fseq_items, script_items


async def _upsert_pack_ingests(
    session: AsyncSession, *, agent_id: str, items: List[_PackIngestMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.dest_dir))
        rec = await session.get(PackIngestRecord, key)
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


async def _upsert_sequences(
    session: AsyncSession, *, agent_id: str, items: List[_SequenceMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.file))
        rec = await session.get(SequenceMetaRecord, key)
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


async def _upsert_audio_analyses(
    session: AsyncSession, *, agent_id: str, items: List[_AudioMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.analysis_id))
        rec = await session.get(AudioAnalysisRecord, key)
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


async def _upsert_show_configs(
    session: AsyncSession, *, agent_id: str, items: List[_ShowConfigMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.file))
        rec = await session.get(ShowConfigRecord, key)
        if rec is None:
            rec = ShowConfigRecord(
                agent_id=agent_id,
                file=str(it.file),
                created_at=float(it.created_at),
                updated_at=float(it.updated_at),
                name=str(it.name or ""),
                props_total=int(it.props_total),
                groups_total=int(it.groups_total),
                coordinator_base_url=(
                    str(it.coordinator_base_url) if it.coordinator_base_url else None
                ),
                fpp_base_url=str(it.fpp_base_url) if it.fpp_base_url else None,
                payload=dict(it.payload or {}),
            )
            session.add(rec)
        else:
            rec.updated_at = float(it.updated_at) or rec.updated_at
            rec.name = str(it.name or "")
            rec.props_total = int(it.props_total)
            rec.groups_total = int(it.groups_total)
            rec.coordinator_base_url = (
                str(it.coordinator_base_url) if it.coordinator_base_url else None
            )
            rec.fpp_base_url = str(it.fpp_base_url) if it.fpp_base_url else None
            rec.payload = dict(it.payload or {})
        upserted += 1
    return upserted


async def _upsert_fseq_exports(
    session: AsyncSession, *, agent_id: str, items: List[_FseqExportMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.file))
        rec = await session.get(FseqExportRecord, key)
        if rec is None:
            rec = FseqExportRecord(
                agent_id=agent_id,
                file=str(it.file),
                created_at=float(it.created_at),
                updated_at=float(it.updated_at),
                source_sequence=None,
                bytes_written=int(it.bytes_written),
                frames=int(it.frames) if it.frames is not None else None,
                channels=int(it.channels) if it.channels is not None else None,
                step_ms=int(it.step_ms) if it.step_ms is not None else None,
                duration_s=float(it.duration_s) if it.duration_s is not None else None,
                payload={},
            )
            session.add(rec)
        else:
            rec.updated_at = float(it.updated_at) or rec.updated_at
            rec.bytes_written = int(it.bytes_written)
            if it.frames is not None:
                rec.frames = int(it.frames)
            if it.channels is not None:
                rec.channels = int(it.channels)
            if it.step_ms is not None:
                rec.step_ms = int(it.step_ms)
            if it.duration_s is not None:
                rec.duration_s = float(it.duration_s)
        upserted += 1
    return upserted


async def _upsert_fpp_scripts(
    session: AsyncSession, *, agent_id: str, items: List[_FppScriptMeta]
) -> int:
    upserted = 0
    for it in items:
        key = (agent_id, str(it.file))
        rec = await session.get(FppScriptRecord, key)
        if rec is None:
            rec = FppScriptRecord(
                agent_id=agent_id,
                file=str(it.file),
                created_at=float(it.created_at),
                updated_at=float(it.updated_at),
                kind=str(it.kind or ""),
                bytes_written=int(it.bytes_written),
                payload=dict(it.payload or {}),
            )
            session.add(rec)
        else:
            rec.updated_at = float(it.updated_at) or rec.updated_at
            rec.kind = str(it.kind or rec.kind or "")
            rec.bytes_written = int(it.bytes_written)
            rec.payload = dict(it.payload or {})
        upserted += 1
    return upserted


async def reconcile_data_dir(
    state: AppState,
    *,
    packs: bool = True,
    sequences: bool = True,
    audio: bool = False,
    show_configs: bool = True,
    fseq_exports: bool = True,
    fpp_scripts: bool = True,
    scan_limit: int = 5000,
    cancel_event: asyncio.Event | None = None,
) -> Dict[str, Any]:
    """
    Best-effort reconciliation: scan DATA_DIR and upsert SQL metadata for UI queries.
    """
    db = getattr(state, "db", None)
    if db is None:
        return {"ok": False, "skipped": True, "reason": "DATABASE_URL not configured"}

    root = Path(str(state.settings.data_dir)).resolve()
    lim = _clamp_limit(int(scan_limit))

    if cancel_event is not None and cancel_event.is_set():
        raise ReconcileCancelled()

    (
        pack_items,
        seq_items,
        audio_items,
        show_items,
        fseq_items,
        script_items,
    ) = await run_cpu_blocking_state(
        state,
        scan_data_dir,
        root_path=str(root),
        packs=bool(packs),
        sequences=bool(sequences),
        audio=bool(audio),
        show_configs=bool(show_configs),
        fseq_exports=bool(fseq_exports),
        fpp_scripts=bool(fpp_scripts),
        limit=int(lim),
    )

    if cancel_event is not None and cancel_event.is_set():
        raise ReconcileCancelled()

    async with AsyncSession(db.engine) as session:
        if cancel_event is not None and cancel_event.is_set():
            raise ReconcileCancelled()
        upserted_packs = (
            await _upsert_pack_ingests(session, agent_id=db.agent_id, items=pack_items)
            if pack_items
            else 0
        )
        if cancel_event is not None and cancel_event.is_set():
            raise ReconcileCancelled()
        upserted_sequences = (
            await _upsert_sequences(session, agent_id=db.agent_id, items=seq_items)
            if seq_items
            else 0
        )
        if cancel_event is not None and cancel_event.is_set():
            raise ReconcileCancelled()
        upserted_audio = (
            await _upsert_audio_analyses(
                session, agent_id=db.agent_id, items=audio_items
            )
            if audio_items
            else 0
        )
        if cancel_event is not None and cancel_event.is_set():
            raise ReconcileCancelled()
        upserted_show = (
            await _upsert_show_configs(
                session, agent_id=db.agent_id, items=show_items
            )
            if show_items
            else 0
        )
        if cancel_event is not None and cancel_event.is_set():
            raise ReconcileCancelled()
        upserted_fseq = (
            await _upsert_fseq_exports(
                session, agent_id=db.agent_id, items=fseq_items
            )
            if fseq_items
            else 0
        )
        if cancel_event is not None and cancel_event.is_set():
            raise ReconcileCancelled()
        upserted_scripts = (
            await _upsert_fpp_scripts(
                session, agent_id=db.agent_id, items=script_items
            )
            if script_items
            else 0
        )
        await session.commit()

    return {
        "ok": True,
        "scanned": {
            "packs": len(pack_items),
            "sequences": len(seq_items),
            "audio": len(audio_items),
            "show_configs": len(show_items),
            "fseq_exports": len(fseq_items),
            "fpp_scripts": len(script_items),
        },
        "upserted": {
            "packs": upserted_packs,
            "sequences": upserted_sequences,
            "audio": upserted_audio,
            "show_configs": upserted_show,
            "fseq_exports": upserted_fseq,
            "fpp_scripts": upserted_scripts,
        },
    }

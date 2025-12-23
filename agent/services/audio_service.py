from __future__ import annotations

import asyncio
import hashlib
import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request

from audio_analyzer import AudioAnalyzeError, analyze_beats, extract_waveform
from models.requests import AudioAnalyzeRequest
from pack_io import read_json_async, write_json_async
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth, require_admin
from services.state import AppState, get_state
from utils.blocking import run_blocking_state, run_cpu_blocking_state
from utils.cache_utils import cache_stats, cleanup_cache


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def _waveform_cache_dir(state: AppState) -> Path:
    return Path(state.settings.data_dir) / "cache" / "waveforms"


async def _waveform_cache_cleanup(
    state: AppState,
    *,
    max_mb: int | None,
    max_days: float | None,
    purge: bool = False,
) -> Dict[str, int]:
    cache_dir = _waveform_cache_dir(state)
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    max_bytes = int(max_mb * 1024 * 1024) if max_mb is not None else None
    return await run_blocking_state(
        state,
        cleanup_cache,
        cache_dir,
        max_bytes=max_bytes,
        max_days=max_days,
        purge=purge,
    )


async def _waveform_cache_stats(state: AppState) -> Dict[str, int]:
    cache_dir = _waveform_cache_dir(state)
    try:
        return await run_blocking_state(state, cache_stats, cache_dir)
    except Exception:
        return {"files": 0, "bytes": 0}


async def audio_analyze(
    req: AudioAnalyzeRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Analyze an audio file and write a beats/BPM timeline JSON under DATA_DIR.

    This is intentionally lightweight (WAV directly; other formats require ffmpeg).
    """
    try:
        audio_path = _resolve_data_path(state, req.audio_file)
        out_path = _resolve_data_path(state, req.out_file)

        analysis = await run_cpu_blocking_state(
            state,
            analyze_beats,
            audio_path=str(audio_path),
            min_bpm=int(req.min_bpm),
            max_bpm=int(req.max_bpm),
            hop_ms=int(req.hop_ms),
            window_ms=int(req.window_ms),
            peak_threshold=float(req.peak_threshold),
            min_interval_s=float(req.min_interval_s),
            prefer_ffmpeg=bool(req.prefer_ffmpeg),
        )

        out = analysis.as_dict()
        if analysis.bpm > 0:
            out["bpm_timeline"] = [
                {
                    "start_s": 0.0,
                    "end_s": float(analysis.duration_s),
                    "bpm": float(analysis.bpm),
                }
            ]
        else:
            out["bpm_timeline"] = []

        await write_json_async(str(out_path), out)

        base = Path(state.settings.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )

        if state.db is not None:
            try:
                rel_audio = (
                    str(audio_path.resolve().relative_to(base))
                    if base in audio_path.resolve().parents
                    else str(audio_path)
                )
                await state.db.add_audio_analysis(
                    analysis_id=uuid.uuid4().hex,
                    source_path=rel_audio,
                    beats_path=rel_out,
                    prefer_ffmpeg=bool(req.prefer_ffmpeg),
                    bpm=float(analysis.bpm),
                    beat_count=len(list(analysis.beats_s or [])),
                    error=None,
                )
            except Exception:
                pass

        return {"ok": True, "analysis": out, "out_file": rel_out}
    except HTTPException:
        raise
    except AudioAnalyzeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def audio_waveform(
    file: str,
    request: Request,
    points: int | None = None,
    refresh: bool = False,
    prefer_ffmpeg: bool = True,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Downsample an audio file into min/max buckets for waveform previews.
    """
    try:
        audio_path = _resolve_data_path(state, file)
        if not audio_path.is_file():
            raise HTTPException(status_code=404, detail="Audio file not found")

        base = Path(state.settings.data_dir).resolve()
        rel = (
            str(audio_path.resolve().relative_to(base))
            if base in audio_path.resolve().parents
            else str(audio_path)
        )
        stat = audio_path.stat()
        default_points = int(getattr(state.settings, "waveform_points_default", 512))
        points_i = max(32, min(5000, int(points or default_points)))
        key_raw = "|".join(
            [
                rel,
                str(stat.st_mtime),
                str(stat.st_size),
                str(points_i),
                str(int(bool(prefer_ffmpeg))),
            ]
        )
        key = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()[:16]
        cache_dir = _waveform_cache_dir(state)
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        cache_path = cache_dir / f"{key}.json"

        if not refresh and cache_path.is_file():
            cached = await read_json_async(str(cache_path))
            if isinstance(cached, dict):
                cached["cached"] = True
                cached["file"] = rel
                return cached

        waveform = await run_cpu_blocking_state(
            state,
            extract_waveform,
            audio_path=str(audio_path),
            points=points_i,
            sample_rate_hz=44100,
            prefer_ffmpeg=bool(prefer_ffmpeg),
        )
        if not isinstance(waveform, dict):
            raise HTTPException(status_code=500, detail="Waveform extraction failed")
        waveform["file"] = rel
        waveform["cached"] = False
        await write_json_async(str(cache_path), waveform)

        max_mb = int(getattr(state.settings, "waveform_cache_max_mb", 0) or 0)
        max_days = float(getattr(state.settings, "waveform_cache_max_days", 0) or 0)
        if max_mb > 0 or max_days > 0:
            async def _prune() -> None:
                try:
                    await _waveform_cache_cleanup(
                        state, max_mb=max_mb or None, max_days=max_days or None
                    )
                except Exception:
                    pass

            asyncio.create_task(_prune())

        return waveform
    except HTTPException:
        raise
    except AudioAnalyzeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def audio_waveform_cache(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    stats = await _waveform_cache_stats(state)
    payload = {
        "ok": True,
        "files": stats.get("files", 0),
        "bytes": stats.get("bytes", 0),
        "max_mb": int(getattr(state.settings, "waveform_cache_max_mb", 0) or 0),
        "max_days": float(getattr(state.settings, "waveform_cache_max_days", 0) or 0),
    }
    await log_event(
        state,
        action="audio.waveform.cache",
        ok=True,
        payload=payload,
        request=request,
    )
    return payload


async def audio_waveform_purge(
    request: Request,
    all: bool = False,
    max_mb: int | None = None,
    max_days: float | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        cfg_max_mb = int(getattr(state.settings, "waveform_cache_max_mb", 0) or 0)
        cfg_max_days = float(
            getattr(state.settings, "waveform_cache_max_days", 0) or 0
        )
        use_max_mb = max_mb if max_mb is not None else (cfg_max_mb or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await _waveform_cache_cleanup(
            state, max_mb=use_max_mb, max_days=use_max_days, purge=bool(all)
        )
        payload = {
            "ok": True,
            "purge": bool(all),
            "deleted_files": result.get("deleted_files", 0),
            "deleted_bytes": result.get("deleted_bytes", 0),
            "before_bytes": result.get("before_bytes", 0),
            "after_bytes": result.get("after_bytes", 0),
        }
        await log_event(
            state,
            action="audio.waveform.purge",
            ok=True,
            payload=payload,
            request=request,
        )
        return payload
    except Exception as e:
        await log_event(
            state,
            action="audio.waveform.purge",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))

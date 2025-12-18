from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import Depends, HTTPException

from audio_analyzer import AudioAnalyzeError, analyze_beats
from models.requests import AudioAnalyzeRequest
from pack_io import read_json, write_json
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


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

        analysis = await asyncio.to_thread(
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

        await asyncio.to_thread(write_json, str(out_path), out)

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

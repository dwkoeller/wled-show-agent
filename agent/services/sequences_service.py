from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from models.requests import GenerateSequenceRequest, PlaySequenceRequest
from pack_io import read_json, read_jsonl
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _require_sequences(state: AppState):
    svc = getattr(state, "sequences", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="Sequence service not initialized")
    return svc


def _require_looks(state: AppState):
    svc = getattr(state, "looks", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="Look service not initialized")
    return svc


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


async def _available_ddp_patterns(state: AppState) -> List[str]:
    from patterns import PatternFactory

    ddp = getattr(state, "ddp", None)
    if ddp is None:
        return []

    info = await state.wled.device_info()
    layout = None
    try:
        from segment_layout import fetch_segment_layout

        layout = await asyncio.to_thread(
            fetch_segment_layout,
            state.wled_sync,
            segment_ids=list(state.segment_ids or []),
            refresh=False,
        )
    except Exception:
        layout = None

    factory = PatternFactory(
        led_count=int(info.led_count),
        geometry=ddp.geometry,
        segment_layout=layout,
    )
    return list(factory.available())


async def sequences_list(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_sequences(state)
    files = await asyncio.to_thread(svc.list_sequences)
    return {"ok": True, "files": files}


async def sequences_generate(
    req: GenerateSequenceRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        looks_svc = _require_looks(state)
        seq_svc = _require_sequences(state)

        pack = req.pack_file or await asyncio.to_thread(looks_svc.latest_pack)
        if not pack:
            raise RuntimeError("No looks pack found; generate looks first.")
        pack_path = os.path.join(state.settings.data_dir, "looks", pack)
        looks = await asyncio.to_thread(read_jsonl, pack_path)
        if len(looks) > 2000:
            looks = looks[:2000]

        ddp_pats = await _available_ddp_patterns(state)

        beats_s: Optional[List[float]] = None
        if req.beats_file:
            beats_path = _resolve_data_path(state, req.beats_file)
            beats_obj = await asyncio.to_thread(read_json, str(beats_path))
            if not isinstance(beats_obj, dict):
                raise RuntimeError(
                    "beats_file must contain a JSON object with a beats list (beats_s or beats_ms)"
                )
            raw_beats = beats_obj.get("beats_s")
            if raw_beats is None:
                raw_beats = beats_obj.get("beats_ms")
                if raw_beats is not None:
                    try:
                        beats_s = [float(x) / 1000.0 for x in list(raw_beats)]
                    except Exception:
                        beats_s = None
            else:
                try:
                    beats_s = [float(x) for x in list(raw_beats)]
                except Exception:
                    beats_s = None
            if not beats_s or len(beats_s) < 2:
                raise RuntimeError(
                    "beats_file did not contain a usable beats list (need >= 2 marks)"
                )

        fname = await asyncio.to_thread(
            seq_svc.generate,
            name=req.name,
            looks=looks,
            duration_s=req.duration_s,
            step_s=req.step_s,
            include_ddp=req.include_ddp,
            renderable_only=bool(req.renderable_only),
            beats_s=beats_s,
            beats_per_step=int(req.beats_per_step),
            beat_offset_s=float(req.beat_offset_s),
            ddp_patterns=ddp_pats,
            seed=req.seed,
        )

        if state.db is not None:
            try:
                seq_path = (
                    Path(state.settings.data_dir).resolve() / "sequences" / str(fname)
                )
                meta = await asyncio.to_thread(read_json, str(seq_path))
                steps = (
                    list((meta or {}).get("steps", []))
                    if isinstance(meta, dict)
                    else []
                )
                steps_total = len([s for s in steps if isinstance(s, dict)])
                duration_s = 0.0
                for s in steps:
                    if not isinstance(s, dict):
                        continue
                    try:
                        duration_s += float(s.get("duration_s") or 0.0)
                    except Exception:
                        continue
                await state.db.upsert_sequence_meta(
                    file=str(fname),
                    duration_s=float(duration_s),
                    steps_total=int(steps_total),
                )
            except Exception:
                pass

        return {"ok": True, "file": fname}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def sequences_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_sequences(state)
    st = await asyncio.to_thread(svc.status)
    return {"ok": True, "status": st.__dict__}


async def sequences_play(
    req: PlaySequenceRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_sequences(state)
        st = await asyncio.to_thread(svc.play, file=req.file, loop=req.loop)
        try:
            from services.runtime_state_service import persist_runtime_state

            await persist_runtime_state(
                state, "sequences_play", {"file": req.file, "loop": bool(req.loop)}
            )
        except Exception:
            pass
        if state.db is not None:
            try:
                await state.db.set_last_applied(
                    kind="sequence",
                    name=str(req.file),
                    file=str(req.file),
                    payload={"file": str(req.file), "loop": bool(req.loop)},
                )
            except Exception:
                pass
        return {"ok": True, "status": st.__dict__}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def sequences_stop(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_sequences(state)
    st = await asyncio.to_thread(svc.stop)
    try:
        from services.runtime_state_service import persist_runtime_state

        await persist_runtime_state(state, "sequences_stop")
    except Exception:
        pass
    return {"ok": True, "status": st.__dict__}

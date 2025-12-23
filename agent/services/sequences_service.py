from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.responses import FileResponse

from geometry import TreeGeometry
from models.requests import GenerateSequenceRequest, PlaySequenceRequest
from pack_io import read_json_async, read_jsonl_async
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth, require_admin
from services.state import AppState, get_state
from utils.blocking import run_blocking_state, run_cpu_blocking_state
from utils.cache_utils import cache_stats, cleanup_cache
from utils.sequence_generate import generate_sequence_file
from utils.sequence_preview import render_sequence_preview


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


def _resolve_sequence_path(state: AppState, rel_path: str) -> Path:
    if not str(rel_path or "").strip():
        raise HTTPException(status_code=400, detail="file is required")
    seq_root = _resolve_data_path(state, "sequences").resolve()
    seq_path = (seq_root / rel_path).resolve()
    if seq_root not in seq_path.parents:
        raise HTTPException(
            status_code=400, detail="file must be within DATA_DIR/sequences"
        )
    if not seq_path.is_file():
        raise HTTPException(status_code=404, detail="Sequence not found")
    return seq_path


def _preview_cache_dir(state: AppState) -> Path:
    return Path(state.settings.data_dir) / "cache" / "previews"


async def _preview_cache_stats(state: AppState) -> Dict[str, int]:
    cache_dir = _preview_cache_dir(state)
    try:
        return await run_blocking_state(state, cache_stats, cache_dir)
    except Exception:
        return {"files": 0, "bytes": 0}


async def _preview_cache_cleanup(
    state: AppState,
    *,
    max_mb: int | None,
    max_days: float | None,
    purge: bool = False,
) -> Dict[str, int]:
    cache_dir = _preview_cache_dir(state)
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


async def _available_ddp_patterns(state: AppState) -> List[str]:
    from patterns import PatternFactory

    ddp = getattr(state, "ddp", None)
    if ddp is None:
        return []

    info = await state.wled.device_info()
    layout = None
    try:
        from segment_layout import fetch_segment_layout_async

        layout = await fetch_segment_layout_async(
            state.wled,
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
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_sequences(state)
        files = await svc.list_sequences()
        await log_event(
            state,
            action="sequences.list",
            ok=True,
            payload={"count": len(files)},
            request=request,
        )
        return {"ok": True, "files": files}
    except HTTPException as e:
        await log_event(
            state,
            action="sequences.list",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="sequences.list", ok=False, error=str(e), request=request
        )
        raise


async def sequences_generate(
    req: GenerateSequenceRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        looks_svc = _require_looks(state)
        _require_sequences(state)

        pack = req.pack_file or await looks_svc.latest_pack()
        if not pack:
            raise RuntimeError("No looks pack found; generate looks first.")
        pack_path = os.path.join(state.settings.data_dir, "looks", pack)
        looks = await read_jsonl_async(pack_path)
        if len(looks) > 2000:
            looks = looks[:2000]

        ddp_pats = await _available_ddp_patterns(state)

        beats_s: Optional[List[float]] = None
        if req.beats_file:
            beats_path = _resolve_data_path(state, req.beats_file)
            beats_obj = await read_json_async(str(beats_path))
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

        fname = await run_cpu_blocking_state(
            state,
            generate_sequence_file,
            data_dir=state.settings.data_dir,
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
                meta = await read_json_async(str(seq_path))
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

        await log_event(
            state,
            action="sequences.generate",
            ok=True,
            resource=str(fname),
            payload={"pack_file": req.pack_file, "beats_file": req.beats_file},
            request=request,
        )
        return {"ok": True, "file": fname}
    except HTTPException as e:
        await log_event(
            state,
            action="sequences.generate",
            ok=False,
            resource=str(req.pack_file or ""),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="sequences.generate",
            ok=False,
            resource=str(req.pack_file or ""),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def sequences_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_sequences(state)
        st = await svc.status()
        await log_event(state, action="sequences.status", ok=True, request=request)
        return {"ok": True, "status": st.__dict__}
    except HTTPException as e:
        await log_event(
            state,
            action="sequences.status",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="sequences.status", ok=False, error=str(e), request=request
        )
        raise


async def sequences_preview(
    file: str,
    request: Request,
    format: str = "gif",
    strict: bool = False,
    refresh: bool = False,
    width: int | None = None,
    height: int | None = None,
    fps: float | None = None,
    max_s: float | None = None,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> FileResponse:
    try:
        seq_path = _resolve_sequence_path(state, file)
        settings = state.settings

        fmt = str(format or "gif").strip().lower()
        if fmt not in ("gif", "mp4"):
            raise HTTPException(status_code=400, detail="format must be gif or mp4")

        width_i = max(
            16, int(width if width is not None else settings.sequence_preview_width)
        )
        height_i = max(
            1, int(height if height is not None else settings.sequence_preview_height)
        )
        fps_f = max(
            1.0, float(fps if fps is not None else settings.sequence_preview_fps)
        )
        max_s_f = max(
            1.0, float(max_s if max_s is not None else settings.sequence_preview_max_s)
        )

        ddp = getattr(state, "ddp", None)
        geometry = getattr(ddp, "geometry", None)
        if geometry is None:
            geometry = TreeGeometry(
                runs=settings.tree_runs,
                pixels_per_run=settings.tree_pixels_per_run,
                segment_len=settings.tree_segment_len,
                segments_per_run=settings.tree_segments_per_run,
            )

        led_count = 0
        try:
            info = await state.wled.device_info()
            led_count = int(getattr(info, "led_count", 0))
        except Exception:
            led_count = 0
        if led_count <= 0:
            try:
                led_count = int(getattr(geometry, "total_pixels", 0))
            except Exception:
                led_count = 0
        if led_count <= 0:
            led_count = int(settings.tree_runs) * int(settings.tree_pixels_per_run)
        if led_count <= 0:
            raise HTTPException(status_code=400, detail="Unable to determine LED count")

        layout = None
        try:
            from segment_layout import fetch_segment_layout_async

            layout = await fetch_segment_layout_async(
                state.wled,
                segment_ids=list(state.segment_ids or []),
                refresh=False,
            )
        except Exception:
            layout = None

        seq_root = _resolve_data_path(state, "sequences").resolve()
        st = seq_path.stat()
        rel = str(seq_path.relative_to(seq_root))
        geom_sig = (
            f"{geometry.runs}:{geometry.pixels_per_run}:"
            f"{geometry.segment_len}:{geometry.segments_per_run}"
        )
        key_raw = "|".join(
            [
                rel,
                str(st.st_mtime),
                str(st.st_size),
                str(width_i),
                str(height_i),
                f"{fps_f:.3f}",
                f"{max_s_f:.3f}",
                str(int(bool(strict))),
                fmt,
                str(led_count),
                geom_sig,
            ]
        )
        key = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()[:16]
        cache_dir = Path(settings.data_dir) / "cache" / "previews"
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        out_path = cache_dir / f"{key}.{fmt}"

        if not refresh and out_path.is_file():
            try:
                if out_path.stat().st_mtime >= st.st_mtime:
                    await log_event(
                        state,
                        action="sequences.preview",
                        ok=True,
                        resource=str(file),
                        payload={"cached": True, "format": fmt},
                        request=request,
                    )
                    return FileResponse(
                        path=str(out_path),
                        filename=f"{Path(file).stem}_preview.{fmt}",
                        media_type="image/gif" if fmt == "gif" else "video/mp4",
                        headers={"Cache-Control": "no-cache"},
                    )
            except Exception:
                pass

        tmp_path = out_path.with_name(f".{out_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            await run_cpu_blocking_state(
                state,
                render_sequence_preview,
                seq_path=str(seq_path),
                out_path=str(tmp_path),
                led_count=int(led_count),
                geometry=geometry,
                segment_layout=layout,
                width=int(width_i),
                height=int(height_i),
                fps=float(fps_f),
                max_duration_s=float(max_s_f),
                default_bri=min(128, int(settings.wled_max_bri)),
                max_bri=int(settings.wled_max_bri),
                strict=bool(strict),
                format=fmt,
            )
            try:
                tmp_path.replace(out_path)
            except Exception:
                try:
                    os.replace(str(tmp_path), str(out_path))
                except Exception:
                    pass
        finally:
            try:
                if tmp_path.is_file():
                    tmp_path.unlink()
            except Exception:
                pass

        max_mb = int(getattr(settings, "sequence_preview_cache_max_mb", 0) or 0)
        max_days = float(getattr(settings, "sequence_preview_cache_max_days", 0) or 0)
        if max_mb > 0 or max_days > 0:
            async def _prune() -> None:
                try:
                    await _preview_cache_cleanup(
                        state, max_mb=max_mb or None, max_days=max_days or None
                    )
                except Exception:
                    pass

            asyncio.create_task(_prune())

        await log_event(
            state,
            action="sequences.preview",
            ok=True,
            resource=str(file),
            payload={"cached": False, "format": fmt},
            request=request,
        )
        return FileResponse(
            path=str(out_path),
            filename=f"{Path(file).stem}_preview.{fmt}",
            media_type="image/gif" if fmt == "gif" else "video/mp4",
            headers={"Cache-Control": "no-cache"},
        )
    except HTTPException as e:
        await log_event(
            state,
            action="sequences.preview",
            ok=False,
            resource=str(file or ""),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="sequences.preview",
            ok=False,
            resource=str(file or ""),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def sequences_preview_cache(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    stats = await _preview_cache_stats(state)
    payload = {
        "ok": True,
        "files": stats.get("files", 0),
        "bytes": stats.get("bytes", 0),
        "max_mb": int(getattr(state.settings, "sequence_preview_cache_max_mb", 0) or 0),
        "max_days": float(
            getattr(state.settings, "sequence_preview_cache_max_days", 0) or 0
        ),
    }
    await log_event(
        state,
        action="sequences.preview.cache",
        ok=True,
        payload=payload,
        request=request,
    )
    return payload


async def sequences_preview_purge(
    request: Request,
    all: bool = False,
    max_mb: int | None = None,
    max_days: float | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        cfg_max_mb = int(
            getattr(state.settings, "sequence_preview_cache_max_mb", 0) or 0
        )
        cfg_max_days = float(
            getattr(state.settings, "sequence_preview_cache_max_days", 0) or 0
        )
        use_max_mb = max_mb if max_mb is not None else (cfg_max_mb or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await _preview_cache_cleanup(
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
            action="sequences.preview.purge",
            ok=True,
            payload=payload,
            request=request,
        )
        return payload
    except Exception as e:
        await log_event(
            state,
            action="sequences.preview.purge",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def sequences_play(
    req: PlaySequenceRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_sequences(state)
        st = await svc.play(file=req.file, loop=req.loop)
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
                try:
                    from services.events_service import emit_event

                    await emit_event(
                        state,
                        event_type="meta",
                        data={
                            "event": "last_applied",
                            "kind": "sequence",
                            "name": str(req.file),
                            "file": str(req.file),
                            "loop": bool(req.loop),
                        },
                    )
                except Exception:
                    pass
            except Exception:
                pass
        await log_event(
            state,
            action="sequences.play",
            ok=True,
            resource=str(req.file),
            payload={"loop": bool(req.loop)},
            request=request,
        )
        return {"ok": True, "status": st.__dict__}
    except HTTPException as e:
        await log_event(
            state,
            action="sequences.play",
            ok=False,
            resource=str(req.file),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="sequences.play",
            ok=False,
            resource=str(req.file),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def sequences_stop(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_sequences(state)
        st = await svc.stop()
        try:
            from services.runtime_state_service import persist_runtime_state

            await persist_runtime_state(state, "sequences_stop")
        except Exception:
            pass
        await log_event(state, action="sequences.stop", ok=True, request=request)
        return {"ok": True, "status": st.__dict__}
    except HTTPException as e:
        await log_event(
            state,
            action="sequences.stop",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="sequences.stop", ok=False, error=str(e), request=request
        )
        raise

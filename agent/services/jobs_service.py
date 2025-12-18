from __future__ import annotations

import asyncio
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from audio_analyzer import analyze_beats
from fseq import write_fseq_v1_file
from jobs import (
    AsyncJobContext,
    AsyncJobManager,
    jobs_snapshot_payload,
    sse_format_event,
)
from models.requests import (
    AudioAnalyzeRequest,
    FSEQExportRequest,
    GenerateLooksRequest,
    GenerateSequenceRequest,
    XlightsImportNetworksRequest,
    XlightsImportProjectRequest,
    XlightsImportSequenceRequest,
)
from pack_io import read_json, read_jsonl, write_json
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from show_config import write_show_config
from xlights_import import (
    import_xlights_models_file,
    import_xlights_networks_file,
    show_config_from_xlights_networks,
    show_config_from_xlights_project,
)
from xlights_sequence_import import import_xlights_xsq_timing_file


def _require_jobs(state: AppState) -> AsyncJobManager:
    jobs = getattr(state, "jobs", None)
    if jobs is None:
        raise HTTPException(status_code=503, detail="Jobs not initialized")
    if not isinstance(jobs, AsyncJobManager):
        raise HTTPException(status_code=503, detail="Jobs not initialized (async)")
    return jobs


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


async def jobs_list(
    limit: int = 50,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    rows = await jobs.list_jobs(limit=limit)
    return {"ok": True, "jobs": [j.as_dict() for j in rows]}


async def jobs_get(
    job_id: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    j = await jobs.get(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": j.as_dict()}


async def jobs_cancel(
    job_id: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    j = await jobs.cancel(job_id)
    if not j:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"ok": True, "job": j.as_dict()}


async def jobs_stream(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> StreamingResponse:
    jobs = _require_jobs(state)
    q = jobs.subscribe()

    async def gen():
        try:
            snap = jobs_snapshot_payload(await jobs.list_jobs(limit=100))
            yield sse_format_event(event="snapshot", data=snap)

            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=1.0)
                    yield sse_format_event(event="message", data=msg)
                except asyncio.TimeoutError:
                    yield sse_format_event(event="ping", data="{}")
        finally:
            jobs.unsubscribe(q)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)


async def jobs_looks_generate(
    req: GenerateLooksRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    looks = getattr(state, "looks", None)
    if looks is None:
        raise HTTPException(status_code=503, detail="Look service not initialized")
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        def _run_sync() -> Dict[str, Any]:
            ctx.set_progress(
                current=0,
                total=float(params.get("total_looks") or 0),
                message="Generating looks…",
            )
            summary = looks.generate_pack(
                total_looks=int(params["total_looks"]),
                themes=list(params["themes"]),
                brightness=min(state.settings.wled_max_bri, int(params["brightness"])),
                seed=int(params["seed"]),
                write_files=bool(params.get("write_files", True)),
                include_multi_segment=bool(params.get("include_multi_segment", True)),
                progress_cb=lambda cur, total, msg: (
                    ctx.check_cancelled(),
                    ctx.set_progress(current=cur, total=total, message=msg),
                ),
                cancel_cb=lambda: jobs.is_cancel_requested(ctx.job_id),
            )
            ctx.set_progress(
                current=float(summary.total),
                total=float(summary.total),
                message="Done.",
            )
            return {"summary": summary.__dict__}

        return await asyncio.to_thread(_run_sync)

    job = await jobs.create(kind="looks_generate", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_audio_analyze(
    req: AudioAnalyzeRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        def _run_sync() -> Dict[str, Any]:
            audio_path = _resolve_data_path(state, str(params["audio_file"]))
            out_path = _resolve_data_path(state, str(params["out_file"]))

            ctx.set_progress(message="Analyzing audio…")
            analysis = analyze_beats(
                audio_path=str(audio_path),
                min_bpm=int(params["min_bpm"]),
                max_bpm=int(params["max_bpm"]),
                hop_ms=int(params["hop_ms"]),
                window_ms=int(params["window_ms"]),
                peak_threshold=float(params["peak_threshold"]),
                min_interval_s=float(params["min_interval_s"]),
                prefer_ffmpeg=bool(params["prefer_ffmpeg"]),
                progress_cb=lambda cur, total, msg: (
                    ctx.check_cancelled(),
                    ctx.set_progress(current=cur, total=total, message=msg),
                ),
                cancel_cb=lambda: jobs.is_cancel_requested(ctx.job_id),
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

            write_json(str(out_path), out)

            base = Path(state.settings.data_dir).resolve()
            rel_out = (
                str(out_path.resolve().relative_to(base))
                if base in out_path.resolve().parents
                else str(out_path)
            )
            rel_audio = (
                str(audio_path.resolve().relative_to(base))
                if base in audio_path.resolve().parents
                else str(audio_path)
            )

            ctx.set_progress(message="Done.")
            return {"analysis": out, "out_file": rel_out, "_rel_audio": rel_audio}

        res = await asyncio.to_thread(_run_sync)
        # Best-effort DB metadata.
        if state.db is not None:
            try:
                await state.db.add_audio_analysis(
                    analysis_id=str(ctx.job_id),
                    source_path=str(res.get("_rel_audio") or "") or None,
                    beats_path=str(res.get("out_file") or "") or None,
                    prefer_ffmpeg=bool(params.get("prefer_ffmpeg")),
                    bpm=(
                        float((res.get("analysis") or {}).get("bpm") or 0.0)
                        if isinstance(res.get("analysis"), dict)
                        else None
                    ),
                    beat_count=(
                        len(list((res.get("analysis") or {}).get("beats_s") or []))
                        if isinstance(res.get("analysis"), dict)
                        else None
                    ),
                    error=None,
                )
            except Exception:
                pass

        res.pop("_rel_audio", None)
        return res

    job = await jobs.create(kind="audio_analyze", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_xlights_import_project(
    req: XlightsImportProjectRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        def _run_sync() -> Dict[str, Any]:
            ctx.set_progress(current=0, total=3, message="Reading project…")
            proj = _resolve_data_path(state, str(params["project_dir"]))
            if not proj.is_dir():
                raise HTTPException(
                    status_code=400,
                    detail="project_dir must be a directory under DATA_DIR",
                )

            proj_root = proj.resolve()

            def _pick(name: Optional[str], defaults: List[str]) -> Path:
                candidates = [name] if name else []
                candidates.extend([d for d in defaults if d not in candidates])
                for n in candidates:
                    if not n:
                        continue
                    p = (proj_root / n).resolve()
                    if proj_root not in p.parents:
                        continue
                    if p.is_file():
                        return p
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required xLights file (tried: {', '.join(candidates)})",
                )

            networks_path = _pick(
                params.get("networks_file"),
                ["xlights_networks.xml"],
            )
            models_path = _pick(
                params.get("models_file"),
                [
                    "xlights_rgbeffects.xml",
                    "xlights_models.xml",
                    "xlights_layout.xml",
                ],
            )

            ctx.set_progress(current=1, total=3, message="Parsing networks + models…")
            controllers = import_xlights_networks_file(str(networks_path))
            models = import_xlights_models_file(str(models_path))

            ctx.set_progress(current=2, total=3, message="Writing show config…")
            cfg = show_config_from_xlights_project(
                networks=controllers,
                models=models,
                show_name=str(params.get("show_name") or "xlights-project"),
                subnet=params.get("subnet"),
                coordinator_base_url=params.get("coordinator_base_url"),
                fpp_base_url=params.get("fpp_base_url"),
                include_controllers=bool(params.get("include_controllers", True)),
                include_models=bool(params.get("include_models", True)),
            )
            out_path = write_show_config(
                data_dir=state.settings.data_dir,
                rel_path=str(params["out_file"]),
                config=cfg,
            )
            ctx.set_progress(current=3, total=3, message="Done.")
            return {"show_config_file": out_path, "show_config": cfg.as_dict()}

        return await asyncio.to_thread(_run_sync)

    job = await jobs.create(kind="xlights_import_project", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_xlights_import_networks(
    req: XlightsImportNetworksRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        def _run_sync() -> Dict[str, Any]:
            ctx.set_progress(current=0, total=2, message="Parsing networks…")
            networks_path = _resolve_data_path(state, str(params["networks_file"]))
            controllers = import_xlights_networks_file(str(networks_path))
            cfg = show_config_from_xlights_networks(
                networks=controllers,
                show_name=str(params.get("show_name") or "xlights-import"),
                subnet=params.get("subnet"),
                coordinator_base_url=params.get("coordinator_base_url"),
                fpp_base_url=params.get("fpp_base_url"),
            )
            ctx.set_progress(current=1, total=2, message="Writing show config…")
            out_path = write_show_config(
                data_dir=state.settings.data_dir,
                rel_path=str(params["out_file"]),
                config=cfg,
            )
            ctx.set_progress(current=2, total=2, message="Done.")
            return {
                "controllers": [
                    {
                        "name": c.name,
                        "host": c.host,
                        "protocol": c.protocol,
                        "universe_start": c.universe_start,
                        "pixel_count": c.pixel_count,
                    }
                    for c in controllers
                ],
                "show_config_file": out_path,
                "show_config": cfg.as_dict(),
            }

        return await asyncio.to_thread(_run_sync)

    job = await jobs.create(kind="xlights_import_networks", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_xlights_import_sequence(
    req: XlightsImportSequenceRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        def _run_sync() -> Dict[str, Any]:
            ctx.set_progress(current=0, total=2, message="Parsing .xsq timing grid…")
            xsq_path = _resolve_data_path(state, str(params["xsq_file"]))
            out_path = _resolve_data_path(state, str(params["out_file"]))
            analysis = import_xlights_xsq_timing_file(
                xsq_path=str(xsq_path), timing_track=params.get("timing_track")
            )
            ctx.set_progress(current=1, total=2, message="Writing beats JSON…")
            write_json(str(out_path), analysis)

            base = Path(state.settings.data_dir).resolve()
            rel_out = (
                str(out_path.resolve().relative_to(base))
                if base in out_path.resolve().parents
                else str(out_path)
            )
            ctx.set_progress(current=2, total=2, message="Done.")
            return {"analysis": analysis, "out_file": rel_out}

        return await asyncio.to_thread(_run_sync)

    job = await jobs.create(kind="xlights_import_sequence", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_sequences_generate(
    req: GenerateSequenceRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    looks = getattr(state, "looks", None)
    seq = getattr(state, "sequences", None)
    if looks is None or seq is None:
        raise HTTPException(
            status_code=503, detail="Sequence/looks services not initialized"
        )

    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        ctx.set_progress(current=0, total=3, message="Loading looks…")
        pack = params.get("pack_file") or await asyncio.to_thread(looks.latest_pack)
        if not pack:
            raise RuntimeError("No looks pack found; generate looks first.")

        pack_path = os.path.join(state.settings.data_dir, "looks", str(pack))
        looks_rows = await asyncio.to_thread(read_jsonl, pack_path)
        if len(looks_rows) > 2000:
            looks_rows = looks_rows[:2000]

        ctx.set_progress(current=1, total=3, message="Preparing patterns…")
        ddp_pats = await _available_ddp_patterns(state)

        beats_s: Optional[List[float]] = None
        if params.get("beats_file"):
            ctx.set_progress(current=1.5, total=3, message="Loading beat grid…")
            beats_path = _resolve_data_path(state, str(params["beats_file"]))
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

        ctx.set_progress(current=2, total=3, message="Generating sequence…")
        fname = await asyncio.to_thread(
            seq.generate,
            name=str(params["name"]),
            looks=looks_rows,
            duration_s=int(params["duration_s"]),
            step_s=int(params["step_s"]),
            include_ddp=bool(params["include_ddp"]),
            renderable_only=bool(params.get("renderable_only", False)),
            beats_s=beats_s,
            beats_per_step=int(params["beats_per_step"]),
            beat_offset_s=float(params["beat_offset_s"]),
            ddp_patterns=ddp_pats,
            seed=int(params["seed"]),
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

        ctx.set_progress(current=3, total=3, message="Done.")
        return {"file": fname}

    job = await jobs.create(kind="sequences_generate", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_fseq_export(
    req: FSEQExportRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    ddp = getattr(state, "ddp", None)
    if ddp is None:
        raise HTTPException(status_code=503, detail="DDP streamer not initialized")

    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        def _run_sync() -> Dict[str, Any]:
            seq_root = _resolve_data_path(state, "sequences").resolve()
            seq_path = (seq_root / (params.get("sequence_file") or "")).resolve()
            if seq_root not in seq_path.parents:
                raise HTTPException(
                    status_code=400,
                    detail="sequence_file must be within DATA_DIR/sequences",
                )
            seq_obj = read_json(str(seq_path))
            steps: List[Dict[str, Any]] = list((seq_obj or {}).get("steps", []))
            if not steps:
                raise HTTPException(status_code=400, detail="Sequence has no steps")

            led_count = (
                int(params["led_count"])
                if params.get("led_count") is not None
                else int(state.wled_sync.device_info().led_count)
            )
            if led_count <= 0:
                raise HTTPException(status_code=400, detail="led_count must be > 0")
            payload_len = led_count * 3

            channel_start = int(params["channel_start"])
            if channel_start <= 0:
                raise HTTPException(
                    status_code=400, detail="channel_start must be >= 1"
                )

            channels_total = (
                int(params["channels_total"])
                if params.get("channels_total") is not None
                else (channel_start - 1 + payload_len)
            )
            if channels_total < (channel_start - 1 + payload_len):
                raise HTTPException(
                    status_code=400,
                    detail="channels_total is too small for channel_start + led_count*3",
                )

            step_ms = int(params["step_ms"])
            default_bri = min(
                state.settings.wled_max_bri, max(1, int(params["default_brightness"]))
            )

            per_step_frames: List[int] = []
            total_frames = 0
            for step in steps:
                dur_s = float(step.get("duration_s", 0.0))
                if dur_s <= 0:
                    dur_s = 0.1
                n = max(1, int(math.ceil((dur_s * 1000.0) / max(1, step_ms))))
                per_step_frames.append(n)
                total_frames += n

            ctx.set_progress(
                current=0, total=float(total_frames), message="Rendering frames…"
            )

            layout = None
            try:
                from segment_layout import fetch_segment_layout

                layout = fetch_segment_layout(
                    state.wled_sync,
                    segment_ids=list(state.segment_ids or []),
                    refresh=False,
                )
            except Exception:
                layout = None

            from patterns import PatternFactory

            factory = PatternFactory(
                led_count=led_count, geometry=ddp.geometry, segment_layout=layout
            )

            out_path = _resolve_data_path(state, str(params["out_file"]))

            frame_idx = 0
            last_report = 0

            def _frames():
                nonlocal frame_idx, last_report
                off = channel_start - 1
                for step, nframes in zip(steps, per_step_frames):
                    ctx.check_cancelled()
                    typ = str(step.get("type") or "").strip().lower()
                    if typ != "ddp":
                        raise RuntimeError(
                            f"Non-renderable step type '{typ}' (only 'ddp' is supported for fseq export)."
                        )
                    pat_name = str(step.get("pattern") or "").strip()
                    if not pat_name:
                        raise RuntimeError("DDP step missing 'pattern'")
                    params2 = step.get("params") or {}
                    if not isinstance(params2, dict):
                        params2 = {}
                    bri = step.get("brightness")
                    bri_i = (
                        default_bri
                        if bri is None
                        else min(state.settings.wled_max_bri, max(1, int(bri)))
                    )

                    pat = factory.create(pat_name, params=params2)
                    for i in range(int(nframes)):
                        ctx.check_cancelled()
                        t = (i * step_ms) / 1000.0
                        rgb = pat.frame(t=t, frame_idx=frame_idx, brightness=bri_i)
                        if len(rgb) != payload_len:
                            rgb = (rgb[:payload_len]).ljust(payload_len, b"\x00")
                        frame = bytearray(channels_total)
                        end = min(channels_total, off + payload_len)
                        frame[off:end] = rgb[: (end - off)]
                        frame_idx += 1
                        if frame_idx - last_report >= 250:
                            last_report = frame_idx
                            ctx.set_progress(
                                current=float(frame_idx),
                                total=float(total_frames),
                                message="Rendering frames…",
                            )
                        yield bytes(frame)

            res = write_fseq_v1_file(
                out_path=str(out_path),
                channel_count=channels_total,
                num_frames=total_frames,
                step_ms=step_ms,
                frame_generator=_frames(),
            )
            ctx.set_progress(
                current=float(total_frames), total=float(total_frames), message="Done."
            )
            return {
                "source_sequence": seq_path.name,
                "render": {
                    "led_count": led_count,
                    "channel_start": channel_start,
                    "channels_total": channels_total,
                    "step_ms": step_ms,
                },
                "fseq": res.__dict__,
                "out_file": str(
                    Path(out_path)
                    .resolve()
                    .relative_to(Path(state.settings.data_dir).resolve())
                ),
            }

        return await asyncio.to_thread(_run_sync)

    job = await jobs.create(kind="fseq_export", runner=_runner)
    return {"ok": True, "job": job.as_dict()}

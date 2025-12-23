from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from aiofiles import os as aio_os
from fastapi import Depends, HTTPException

from audio_analyzer import analyze_beats
from jobs import (
    AsyncJobContext,
    AsyncJobManager,
    Job,
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
from pack_io import read_json_async, read_jsonl_async, write_json_async
from services.auth_service import require_a2a_auth, require_admin
from services.state import AppState, get_state
from utils.blocking import run_cpu_blocking_state
from utils.fseq_render import render_fseq
from utils.sequence_generate import generate_sequence_file
from show_config import ShowConfig, write_show_config_async
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


async def _ensure_job_capacity(state: AppState, jobs: AsyncJobManager) -> None:
    try:
        if getattr(jobs, "queue_full")() is True:
            raise HTTPException(
                status_code=503, detail="Job queue is full; try again later."
            )
    except HTTPException:
        raise
    except Exception:
        pass

    return


async def _create_job(
    state: AppState,
    jobs: AsyncJobManager,
    *,
    kind: str,
    runner,
) -> Job:
    await _ensure_job_capacity(state, jobs)
    try:
        return await jobs.create(kind=kind, runner=runner)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def _rel_show_config_path(state: AppState, path_s: str) -> str:
    base = Path(state.settings.data_dir).resolve()
    show_root = (base / "show").resolve()
    p = Path(path_s).resolve()
    try:
        return str(p.relative_to(show_root))
    except Exception:
        try:
            return str(p.relative_to(base))
        except Exception:
            return str(p)


def _show_config_payload(cfg: ShowConfig) -> Dict[str, Any]:
    props_by_kind: Dict[str, int] = {}
    for prop in cfg.props:
        k = str(getattr(prop, "kind", "") or "").strip().lower() or "unknown"
        props_by_kind[k] = props_by_kind.get(k, 0) + 1
    return {
        "subnet": cfg.subnet,
        "channels_per_universe": cfg.channels_per_universe,
        "props_by_kind": props_by_kind,
    }


async def _maybe_upsert_show_config(
    state: AppState, *, cfg: ShowConfig, path_s: str
) -> None:
    db = getattr(state, "db", None)
    if db is None:
        return
    try:
        await db.upsert_show_config(
            file=_rel_show_config_path(state, path_s),
            name=str(cfg.name or ""),
            props_total=len(cfg.props),
            groups_total=len(cfg.groups or {}),
            coordinator_base_url=str(cfg.coordinator.base_url or "").strip() or None,
            fpp_base_url=str(cfg.fpp.base_url or "").strip() or None,
            payload=_show_config_payload(cfg),
        )
    except Exception:
        pass


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


async def jobs_list(
    limit: int = 50,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    rows = await jobs.list_jobs(limit=limit)
    queue = None
    counts = None
    try:
        queue = jobs.queue_stats()
        counts = jobs.status_counts()
    except Exception:
        queue = None
        counts = None
    return {
        "ok": True,
        "jobs": [j.as_dict() for j in rows],
        "queue": queue,
        "status_counts": counts,
    }


async def jobs_retention_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        stats = await db.job_stats()
        now = time.time()
        max_rows = int(getattr(state.settings, "job_history_max_rows", 0) or 0)
        max_days = int(getattr(state.settings, "job_history_max_days", 0) or 0)
        oldest = stats.get("oldest")
        oldest_age_s = max(0.0, now - float(oldest)) if oldest else None
        excess_rows = max(0, int(stats.get("count", 0)) - max_rows) if max_rows else 0
        excess_age_s = (
            max(0.0, float(oldest_age_s) - (max_days * 86400.0))
            if max_days and oldest_age_s is not None
            else 0.0
        )
        drift = bool(excess_rows > 0 or excess_age_s > 0)
        return {
            "ok": True,
            "stats": stats,
            "settings": {
                "max_rows": max_rows,
                "max_days": max_days,
                "maintenance_interval_s": int(
                    getattr(state.settings, "job_history_maintenance_interval_s", 0)
                    or 0
                ),
            },
            "drift": {
                "excess_rows": int(excess_rows),
                "excess_age_s": float(excess_age_s),
                "oldest_age_s": float(oldest_age_s) if oldest_age_s is not None else None,
                "drift": drift,
            },
            "last_retention": getattr(state, "job_retention_last", None),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def jobs_retention_cleanup(
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        cfg_max_rows = int(getattr(state.settings, "job_history_max_rows", 0) or 0)
        cfg_max_days = int(getattr(state.settings, "job_history_max_days", 0) or 0)
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await db.enforce_job_retention(
            max_rows=use_max_rows,
            max_days=use_max_days,
        )
        state.job_retention_last = {
            "at": time.time(),
            "result": result,
        }
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        ctx.set_progress(
            current=0,
            total=float(params.get("total_looks") or 0),
            message="Generating looks...",
        )
        summary = await looks.generate_pack(
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

    job = await _create_job(state, jobs, kind="looks_generate", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_audio_analyze(
    req: AudioAnalyzeRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        audio_path = _resolve_data_path(state, str(params["audio_file"]))
        out_path = _resolve_data_path(state, str(params["out_file"]))

        ctx.set_progress(message="Analyzing audio...")
        analysis = await run_cpu_blocking_state(
            state,
            analyze_beats,
            audio_path=str(audio_path),
            min_bpm=int(params["min_bpm"]),
            max_bpm=int(params["max_bpm"]),
            hop_ms=int(params["hop_ms"]),
            window_ms=int(params["window_ms"]),
            peak_threshold=float(params["peak_threshold"]),
            min_interval_s=float(params["min_interval_s"]),
            prefer_ffmpeg=bool(params["prefer_ffmpeg"]),
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
        rel_audio = (
            str(audio_path.resolve().relative_to(base))
            if base in audio_path.resolve().parents
            else str(audio_path)
        )

        ctx.set_progress(message="Done.")
        res = {"analysis": out, "out_file": rel_out, "_rel_audio": rel_audio}
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

    job = await _create_job(state, jobs, kind="audio_analyze", runner=_runner)
    return {"ok": True, "job": job.as_dict()}


async def jobs_xlights_import_project(
    req: XlightsImportProjectRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        ctx.set_progress(current=0, total=3, message="Reading project...")
        proj = _resolve_data_path(state, str(params["project_dir"]))
        if not await aio_os.path.isdir(str(proj)):
            raise HTTPException(
                status_code=400,
                detail="project_dir must be a directory under DATA_DIR",
            )

        proj_root = proj.resolve()

        async def _pick(name: Optional[str], defaults: List[str]) -> Path:
            candidates = [name] if name else []
            candidates.extend([d for d in defaults if d not in candidates])
            for n in candidates:
                if not n:
                    continue
                p = (proj_root / n).resolve()
                if proj_root not in p.parents:
                    continue
                if await aio_os.path.isfile(str(p)):
                    return p
            raise HTTPException(
                status_code=400,
                detail=f"Missing required xLights file (tried: {', '.join(candidates)})",
            )

        networks_path = await _pick(
            params.get("networks_file"),
            ["xlights_networks.xml"],
        )
        models_path = await _pick(
            params.get("models_file"),
            [
                "xlights_rgbeffects.xml",
                "xlights_models.xml",
                "xlights_layout.xml",
            ],
        )

        ctx.set_progress(current=1, total=3, message="Parsing networks + models...")
        controllers = await run_cpu_blocking_state(
            state, import_xlights_networks_file, str(networks_path)
        )
        models = await run_cpu_blocking_state(
            state, import_xlights_models_file, str(models_path)
        )

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

        ctx.set_progress(current=2, total=3, message="Writing show config...")
        out_path = await write_show_config_async(
            data_dir=state.settings.data_dir,
            rel_path=str(params["out_file"]),
            config=cfg,
        )
        ctx.set_progress(current=3, total=3, message="Done.")
        res = {"show_config_file": out_path, "show_config": cfg.as_dict()}
        if state.db is not None:
            try:
                raw_cfg = res.get("show_config") if isinstance(res, dict) else None
                if isinstance(raw_cfg, dict):
                    cfg = ShowConfig.model_validate(raw_cfg)
                    await _maybe_upsert_show_config(
                        state,
                        cfg=cfg,
                        path_s=str(res.get("show_config_file") or ""),
                    )
            except Exception:
                pass
        return res

    job = await _create_job(
        state, jobs, kind="xlights_import_project", runner=_runner
    )
    return {"ok": True, "job": job.as_dict()}


async def jobs_xlights_import_networks(
    req: XlightsImportNetworksRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        ctx.set_progress(current=0, total=2, message="Parsing networks...")
        networks_path = _resolve_data_path(state, str(params["networks_file"]))
        controllers = await run_cpu_blocking_state(
            state, import_xlights_networks_file, str(networks_path)
        )
        cfg = show_config_from_xlights_networks(
            networks=controllers,
            show_name=str(params.get("show_name") or "xlights-import"),
            subnet=params.get("subnet"),
            coordinator_base_url=params.get("coordinator_base_url"),
            fpp_base_url=params.get("fpp_base_url"),
        )
        ctx.set_progress(current=1, total=2, message="Writing show config...")
        out_path = await write_show_config_async(
            data_dir=state.settings.data_dir,
            rel_path=str(params["out_file"]),
            config=cfg,
        )
        ctx.set_progress(current=2, total=2, message="Done.")
        res = {
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
        if state.db is not None:
            try:
                raw_cfg = res.get("show_config") if isinstance(res, dict) else None
                if isinstance(raw_cfg, dict):
                    cfg = ShowConfig.model_validate(raw_cfg)
                    await _maybe_upsert_show_config(
                        state,
                        cfg=cfg,
                        path_s=str(res.get("show_config_file") or ""),
                    )
            except Exception:
                pass
        return res

    job = await _create_job(
        state, jobs, kind="xlights_import_networks", runner=_runner
    )
    return {"ok": True, "job": job.as_dict()}


async def jobs_xlights_import_sequence(
    req: XlightsImportSequenceRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    jobs = _require_jobs(state)
    params = req.model_dump()

    async def _runner(ctx: AsyncJobContext) -> Any:
        ctx.set_progress(current=0, total=2, message="Parsing .xsq timing grid...")
        xsq_path = _resolve_data_path(state, str(params["xsq_file"]))
        out_path = _resolve_data_path(state, str(params["out_file"]))
        analysis = await run_cpu_blocking_state(
            state,
            import_xlights_xsq_timing_file,
            xsq_path=str(xsq_path),
            timing_track=params.get("timing_track"),
        )
        ctx.set_progress(current=1, total=2, message="Writing beats JSON...")
        await write_json_async(str(out_path), analysis)

        base = Path(state.settings.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )
        ctx.set_progress(current=2, total=2, message="Done.")
        return {"analysis": analysis, "out_file": rel_out}

    job = await _create_job(
        state, jobs, kind="xlights_import_sequence", runner=_runner
    )
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
        ctx.set_progress(current=0, total=3, message="Loading looks...")
        pack = params.get("pack_file") or await looks.latest_pack()
        if not pack:
            raise RuntimeError("No looks pack found; generate looks first.")

        pack_path = os.path.join(state.settings.data_dir, "looks", str(pack))
        looks_rows = await read_jsonl_async(pack_path)
        if len(looks_rows) > 2000:
            looks_rows = looks_rows[:2000]

        ctx.set_progress(current=1, total=3, message="Preparing patterns...")
        ddp_pats = await _available_ddp_patterns(state)

        beats_s: Optional[List[float]] = None
        if params.get("beats_file"):
            ctx.set_progress(current=1.5, total=3, message="Loading beat grid...")
            beats_path = _resolve_data_path(state, str(params["beats_file"]))
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

        ctx.set_progress(current=2, total=3, message="Generating sequence...")
        fname = await run_cpu_blocking_state(
            state,
            generate_sequence_file,
            data_dir=state.settings.data_dir,
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

        ctx.set_progress(current=3, total=3, message="Done.")
        return {"file": fname}

    job = await _create_job(state, jobs, kind="sequences_generate", runner=_runner)
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
        led_count_auto: int | None = None
        if params.get("led_count") is None:
            try:
                led_count_auto = int((await state.wled.device_info()).led_count)
            except Exception:
                led_count_auto = None

        layout_auto = None
        try:
            from segment_layout import fetch_segment_layout_async

            layout_auto = await fetch_segment_layout_async(
                state.wled,
                segment_ids=list(state.segment_ids or []),
                refresh=False,
            )
        except Exception:
            layout_auto = None

        seq_root = _resolve_data_path(state, "sequences").resolve()
        seq_path = (seq_root / (params.get("sequence_file") or "")).resolve()
        if seq_root not in seq_path.parents:
            raise HTTPException(
                status_code=400,
                detail="sequence_file must be within DATA_DIR/sequences",
            )
        seq_obj = await read_json_async(str(seq_path))
        seq_map = seq_obj if isinstance(seq_obj, dict) else {}
        steps: List[Dict[str, Any]] = list((seq_map or {}).get("steps", []))

        led_count = (
            int(params["led_count"])
            if params.get("led_count") is not None
            else int(led_count_auto or 0)
        )
        channel_start = int(params["channel_start"])
        channels_total = (
            int(params["channels_total"])
            if params.get("channels_total") is not None
            else (channel_start - 1 + (led_count * 3))
        )
        step_ms = int(params["step_ms"])
        default_bri = min(
            state.settings.wled_max_bri, max(1, int(params["default_brightness"]))
        )

        out_path = _resolve_data_path(state, str(params["out_file"]))
        ctx.set_progress(message="Rendering fseq...")
        try:
            render = await run_cpu_blocking_state(
                state,
                render_fseq,
                steps=steps,
                out_path=str(out_path),
                led_count=led_count,
                channel_start=channel_start,
                channels_total=channels_total,
                step_ms=step_ms,
                default_bri=int(default_bri),
                geometry=ddp.geometry,
                segment_layout=layout_auto,
                max_bri=int(state.settings.wled_max_bri),
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        res = {
            "source_sequence": seq_path.name,
            "render": render.get("render"),
            "fseq": render.get("fseq"),
            "out_file": str(
                Path(out_path)
                .resolve()
                .relative_to(Path(state.settings.data_dir).resolve())
            ),
        }
        ctx.set_progress(message="Done.")
        if state.db is not None:
            try:
                fseq = res.get("fseq") if isinstance(res, dict) else None
                rel_path = (
                    str(res.get("out_file") or "").lstrip("/")
                    if isinstance(res, dict)
                    else ""
                )
                try:
                    rel_file = str(Path(rel_path).relative_to("fseq"))
                except Exception:
                    rel_file = rel_path
                frames = (
                    int(fseq.get("frames"))
                    if isinstance(fseq, dict) and fseq.get("frames") is not None
                    else None
                )
                step_ms = (
                    int(fseq.get("step_ms"))
                    if isinstance(fseq, dict) and fseq.get("step_ms") is not None
                    else None
                )
                duration_s = (
                    (float(frames) * float(step_ms) / 1000.0)
                    if frames is not None and step_ms is not None
                    else None
                )
                source_sequence = (
                    str(res.get("source_sequence") or "").strip() or None
                    if isinstance(res, dict)
                    else None
                )
                await state.db.upsert_fseq_export(
                    file=rel_file,
                    source_sequence=source_sequence,
                    bytes_written=(
                        int(fseq.get("bytes_written") or 0)
                        if isinstance(fseq, dict)
                        else 0
                    ),
                    frames=frames,
                    channels=(
                        int(fseq.get("channels"))
                        if isinstance(fseq, dict) and fseq.get("channels") is not None
                        else None
                    ),
                    step_ms=step_ms,
                    duration_s=duration_s,
                    payload={"render": res.get("render")}
                    if isinstance(res, dict)
                    else None,
                )
            except Exception:
                pass
        return res

    job = await _create_job(state, jobs, kind="fseq_export", runner=_runner)
    return {"ok": True, "job": job.as_dict()}

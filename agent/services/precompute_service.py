from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any, Dict, List, Sequence

from audio_analyzer import AudioAnalyzeError, extract_waveform
from geometry import TreeGeometry
from jobs import AsyncJobContext, AsyncJobManager, Job
from pack_io import write_json_async
from services.state import AppState
from utils.blocking import run_cpu_blocking_state
from utils.sequence_preview import render_sequence_preview


_AUDIO_EXTS = {".wav", ".mp3", ".aac", ".m4a", ".flac", ".ogg"}


def _require_jobs(state: AppState) -> AsyncJobManager:
    jobs = getattr(state, "jobs", None)
    if jobs is None or not isinstance(jobs, AsyncJobManager):
        raise RuntimeError("Jobs not initialized")
    return jobs


async def _ensure_job_capacity(jobs: AsyncJobManager) -> None:
    try:
        if getattr(jobs, "queue_full")() is True:
            raise RuntimeError("Job queue is full; try again later.")
    except RuntimeError:
        raise
    except Exception:
        return


async def _create_job(jobs: AsyncJobManager, *, kind: str, runner) -> Job:
    await _ensure_job_capacity(jobs)
    return await jobs.create(kind=kind, runner=runner)


def _list_sequence_files(state: AppState, *, limit: int) -> List[str]:
    seq_dir = (Path(state.settings.data_dir) / "sequences").resolve()
    if not seq_dir.is_dir():
        return []
    out: List[str] = []
    for dirpath, _dirnames, filenames in os.walk(seq_dir, followlinks=False):
        for name in filenames:
            if len(out) >= limit:
                break
            if not (name.startswith("sequence_") and name.endswith(".json")):
                continue
            p = Path(dirpath) / name
            try:
                rel = str(p.resolve().relative_to(seq_dir))
            except Exception:
                rel = str(p.name)
            out.append(rel)
        if len(out) >= limit:
            break
    return out


def _list_audio_files(state: AppState, *, limit: int) -> List[str]:
    root = Path(state.settings.data_dir).resolve()
    music_dir = (root / "music").resolve()
    if not music_dir.is_dir():
        return []
    out: List[str] = []
    for dirpath, _dirnames, filenames in os.walk(music_dir, followlinks=False):
        for name in filenames:
            if len(out) >= limit:
                break
            ext = Path(name).suffix.lower()
            if ext not in _AUDIO_EXTS:
                continue
            p = Path(dirpath) / name
            try:
                rel = str(p.resolve().relative_to(root))
            except Exception:
                rel = str(p.name)
            out.append(rel)
        if len(out) >= limit:
            break
    return out


def _preview_cache_path(
    *,
    state: AppState,
    seq_rel: str,
    st: os.stat_result,
    width: int,
    height: int,
    fps: float,
    max_s: float,
    strict: bool,
    fmt: str,
    led_count: int,
    geometry: TreeGeometry,
) -> Path:
    geom_sig = (
        f"{geometry.runs}:{geometry.pixels_per_run}:"
        f"{geometry.segment_len}:{geometry.segments_per_run}"
    )
    key_raw = "|".join(
        [
            seq_rel,
            str(st.st_mtime),
            str(st.st_size),
            str(width),
            str(height),
            f"{fps:.3f}",
            f"{max_s:.3f}",
            str(int(bool(strict))),
            fmt,
            str(led_count),
            geom_sig,
        ]
    )
    key = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()[:16]
    cache_dir = Path(state.settings.data_dir) / "cache" / "previews"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{key}.{fmt}"


def _waveform_cache_path(
    *,
    state: AppState,
    rel_path: str,
    st: os.stat_result,
    points: int,
    prefer_ffmpeg: bool,
) -> Path:
    key_raw = "|".join(
        [
            rel_path,
            str(st.st_mtime),
            str(st.st_size),
            str(points),
            str(int(bool(prefer_ffmpeg))),
        ]
    )
    key = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()[:16]
    cache_dir = Path(state.settings.data_dir) / "cache" / "waveforms"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{key}.json"


async def schedule_precompute(
    state: AppState,
    *,
    reason: str,
    sequences: Sequence[str] | None = None,
    audio_files: Sequence[str] | None = None,
    scan_limit: int | None = None,
    formats: Sequence[str] | None = None,
) -> dict[str, Any] | None:
    seq_list = list(sequences) if sequences is not None else []
    audio_list = list(audio_files) if audio_files is not None else []
    lim = max(1, int(scan_limit or state.settings.db_reconcile_scan_limit or 5000))
    if sequences is None and not seq_list:
        seq_list = _list_sequence_files(state, limit=lim)
    if audio_files is None and not audio_list:
        audio_list = _list_audio_files(state, limit=lim)
    if not seq_list and not audio_list:
        return None

    jobs = _require_jobs(state)
    fmt_list = [str(f).strip().lower() for f in (formats or ["gif"]) if str(f).strip()]
    fmt_list = [f for f in fmt_list if f in ("gif", "mp4")] or ["gif"]

    async def _runner(ctx: AsyncJobContext) -> Any:
        total = len(seq_list) * len(fmt_list) + len(audio_list)
        current = 0
        summary = {
            "reason": str(reason),
            "sequences": {"total": len(seq_list), "rendered": 0, "cached": 0},
            "audio": {"total": len(audio_list), "rendered": 0, "cached": 0},
            "errors": 0,
        }

        if seq_list:
            settings = state.settings
            width = max(16, int(settings.sequence_preview_width))
            height = max(1, int(settings.sequence_preview_height))
            fps = max(1.0, float(settings.sequence_preview_fps))
            max_s = max(1.0, float(settings.sequence_preview_max_s))
            strict = False
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
                summary["errors"] += len(seq_list) * len(fmt_list)
                seq_list = []

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

            seq_root = Path(settings.data_dir) / "sequences"
            for seq_rel in seq_list:
                for fmt in fmt_list:
                    ctx.check_cancelled()
                    seq_path = (seq_root / str(seq_rel)).resolve()
                    ctx.set_progress(
                        current=current,
                        total=total,
                        message=f"preview {seq_rel} ({fmt})",
                    )
                    current += 1
                    if not seq_path.is_file():
                        continue
                    try:
                        st = seq_path.stat()
                        out_path = _preview_cache_path(
                            state=state,
                            seq_rel=str(seq_rel),
                            st=st,
                            width=width,
                            height=height,
                            fps=fps,
                            max_s=max_s,
                            strict=strict,
                            fmt=fmt,
                            led_count=led_count,
                            geometry=geometry,
                        )
                        if out_path.is_file():
                            try:
                                if out_path.stat().st_mtime >= st.st_mtime:
                                    summary["sequences"]["cached"] += 1
                                    continue
                            except Exception:
                                pass
                        tmp_path = out_path.with_name(
                            f".{out_path.name}.{os.urandom(4).hex()}.tmp"
                        )
                        try:
                            await run_cpu_blocking_state(
                                state,
                                render_sequence_preview,
                                seq_path=str(seq_path),
                                out_path=str(tmp_path),
                                led_count=int(led_count),
                                geometry=geometry,
                                segment_layout=layout,
                                width=int(width),
                                height=int(height),
                                fps=float(fps),
                                max_duration_s=float(max_s),
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
                            summary["sequences"]["rendered"] += 1
                        finally:
                            try:
                                if tmp_path.is_file():
                                    tmp_path.unlink()
                            except Exception:
                                pass
                    except Exception:
                        summary["errors"] += 1

        if audio_list:
            points = int(getattr(state.settings, "waveform_points_default", 512))
            points = max(32, min(5000, points))
            prefer_ffmpeg = True
            base = Path(state.settings.data_dir).resolve()
            for rel_path in audio_list:
                ctx.check_cancelled()
                ctx.set_progress(
                    current=current,
                    total=total,
                    message=f"waveform {rel_path}",
                )
                current += 1
                abs_path = (base / str(rel_path)).resolve()
                if base not in abs_path.parents:
                    continue
                if not abs_path.is_file():
                    continue
                try:
                    st = abs_path.stat()
                    out_path = _waveform_cache_path(
                        state=state,
                        rel_path=str(rel_path),
                        st=st,
                        points=points,
                        prefer_ffmpeg=prefer_ffmpeg,
                    )
                    if out_path.is_file():
                        summary["audio"]["cached"] += 1
                        continue
                    waveform = await run_cpu_blocking_state(
                        state,
                        extract_waveform,
                        audio_path=str(abs_path),
                        points=int(points),
                        sample_rate_hz=44100,
                        prefer_ffmpeg=bool(prefer_ffmpeg),
                    )
                    if not isinstance(waveform, dict):
                        summary["errors"] += 1
                        continue
                    waveform["file"] = str(rel_path)
                    waveform["cached"] = False
                    await write_json_async(str(out_path), waveform)
                    summary["audio"]["rendered"] += 1
                except AudioAnalyzeError:
                    summary["errors"] += 1
                except Exception:
                    summary["errors"] += 1

        ctx.set_progress(current=total, total=total, message="Done.")
        return {"summary": summary}

    job = await _create_job(jobs, kind="precompute_assets", runner=_runner)
    return job.as_dict()

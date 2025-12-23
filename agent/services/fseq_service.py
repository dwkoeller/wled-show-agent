from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from fastapi import Depends, HTTPException

from models.requests import FSEQExportRequest
from pack_io import read_json_async
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from utils.blocking import run_cpu_blocking_state
from utils.fseq_render import render_fseq


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


async def fseq_export(
    req: FSEQExportRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Export a renderable (procedural-pattern) sequence JSON file to an uncompressed .fseq (v1).

    Note: steps of type "look" (WLED effect states) are not offline-renderable and are rejected.
    """

    led_count_auto: int | None = None
    if req.led_count is None:
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
    seq_path = (seq_root / (req.sequence_file or "")).resolve()
    if seq_root not in seq_path.parents:
        raise HTTPException(
            status_code=400,
            detail="sequence_file must be within DATA_DIR/sequences",
        )
    seq = await read_json_async(str(seq_path))
    seq_obj = seq if isinstance(seq, dict) else {}
    steps: List[Dict[str, Any]] = list((seq_obj or {}).get("steps", []))

    led_count = (
        int(req.led_count) if req.led_count is not None else int(led_count_auto or 0)
    )
    channel_start = int(req.channel_start)
    channels_total = (
        int(req.channels_total)
        if req.channels_total is not None
        else (channel_start - 1 + (led_count * 3))
    )
    step_ms = int(req.step_ms)
    default_bri = min(
        state.settings.wled_max_bri, max(1, int(req.default_brightness))
    )

    ddp = getattr(state, "ddp", None)
    if ddp is None:
        raise HTTPException(status_code=503, detail="DDP streamer not initialized")

    out_path = _resolve_data_path(state, req.out_file)
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

    res: Dict[str, Any] = {
        "ok": True,
        "source_sequence": seq_path.name,
        "render": render.get("render"),
        "fseq": render.get("fseq"),
        "out_file": str(
            Path(out_path)
            .resolve()
            .relative_to(Path(state.settings.data_dir).resolve())
        ),
    }
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

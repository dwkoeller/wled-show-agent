from __future__ import annotations

import asyncio
import math
from pathlib import Path
from typing import Any, Dict, List

from fastapi import Depends, HTTPException

from fseq import write_fseq_v1_file
from models.requests import FSEQExportRequest
from pack_io import read_json
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


async def fseq_export(
    req: FSEQExportRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Export a renderable (procedural-pattern) sequence JSON file to an uncompressed .fseq (v1).

    Note: steps of type "look" (WLED effect states) are not offline-renderable and are rejected.
    """

    def _run() -> Dict[str, Any]:
        seq_root = _resolve_data_path(state, "sequences").resolve()
        seq_path = (seq_root / (req.sequence_file or "")).resolve()
        if seq_root not in seq_path.parents:
            raise HTTPException(
                status_code=400,
                detail="sequence_file must be within DATA_DIR/sequences",
            )
        seq = read_json(str(seq_path))
        steps: List[Dict[str, Any]] = list((seq or {}).get("steps", []))
        if not steps:
            raise HTTPException(status_code=400, detail="Sequence has no steps")

        led_count = (
            int(req.led_count)
            if req.led_count is not None
            else int(state.wled_sync.device_info().led_count)
        )
        if led_count <= 0:
            raise HTTPException(status_code=400, detail="led_count must be > 0")
        payload_len = led_count * 3

        channel_start = int(req.channel_start)
        if channel_start <= 0:
            raise HTTPException(status_code=400, detail="channel_start must be >= 1")

        channels_total = (
            int(req.channels_total)
            if req.channels_total is not None
            else (channel_start - 1 + payload_len)
        )
        if channels_total < (channel_start - 1 + payload_len):
            raise HTTPException(
                status_code=400,
                detail="channels_total is too small for channel_start + led_count*3",
            )

        step_ms = int(req.step_ms)
        default_bri = min(
            state.settings.wled_max_bri, max(1, int(req.default_brightness))
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

        ddp = getattr(state, "ddp", None)
        if ddp is None:
            raise HTTPException(status_code=503, detail="DDP streamer not initialized")

        factory = PatternFactory(
            led_count=led_count, geometry=ddp.geometry, segment_layout=layout
        )

        out_path = _resolve_data_path(state, req.out_file)

        frame_idx = 0

        def _frames():
            nonlocal frame_idx
            off = channel_start - 1
            for step, nframes in zip(steps, per_step_frames):
                typ = str(step.get("type") or "").strip().lower()
                if typ != "ddp":
                    raise RuntimeError(
                        f"Non-renderable step type '{typ}' (only 'ddp' is supported for fseq export)."
                    )
                pat_name = str(step.get("pattern") or "").strip()
                if not pat_name:
                    raise RuntimeError("DDP step missing 'pattern'")
                params = step.get("params") or {}
                if not isinstance(params, dict):
                    params = {}
                bri = step.get("brightness")
                bri_i = (
                    default_bri
                    if bri is None
                    else min(state.settings.wled_max_bri, max(1, int(bri)))
                )

                pat = factory.create(pat_name, params=params)
                for i in range(int(nframes)):
                    t = (i * step_ms) / 1000.0
                    rgb = pat.frame(t=t, frame_idx=frame_idx, brightness=bri_i)
                    if len(rgb) != payload_len:
                        rgb = (rgb[:payload_len]).ljust(payload_len, b"\x00")
                    frame = bytearray(channels_total)
                    end = min(channels_total, off + payload_len)
                    frame[off:end] = rgb[: (end - off)]
                    frame_idx += 1
                    yield bytes(frame)

        res = write_fseq_v1_file(
            out_path=str(out_path),
            channel_count=channels_total,
            num_frames=total_frames,
            step_ms=step_ms,
            frame_generator=_frames(),
        )
        return {
            "ok": True,
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

    return await asyncio.to_thread(_run)

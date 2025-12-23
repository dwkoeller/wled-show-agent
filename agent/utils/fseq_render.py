from __future__ import annotations

import math
from typing import Any, Dict, List

from fseq import write_fseq_v1_file
from geometry import TreeGeometry
from patterns import PatternFactory
from segment_layout import SegmentLayout


def render_fseq(
    *,
    steps: List[Dict[str, Any]],
    out_path: str,
    led_count: int,
    channel_start: int,
    channels_total: int,
    step_ms: int,
    default_bri: int,
    geometry: TreeGeometry,
    segment_layout: SegmentLayout | None,
    max_bri: int,
) -> Dict[str, Any]:
    if not steps:
        raise ValueError("Sequence has no steps")
    if led_count <= 0:
        raise ValueError("led_count must be > 0")
    if channel_start <= 0:
        raise ValueError("channel_start must be >= 1")

    payload_len = int(led_count) * 3
    if channels_total < (channel_start - 1 + payload_len):
        raise ValueError(
            "channels_total is too small for channel_start + led_count*3"
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

    factory = PatternFactory(
        led_count=int(led_count),
        geometry=geometry,
        segment_layout=segment_layout,
    )

    frame_idx = 0

    def _frames():
        nonlocal frame_idx
        off = int(channel_start) - 1
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
                int(default_bri)
                if bri is None
                else min(int(max_bri), max(1, int(bri)))
            )

            pat = factory.create(pat_name, params=params)
            for i in range(int(nframes)):
                t = (i * int(step_ms)) / 1000.0
                rgb = pat.frame(t=t, frame_idx=frame_idx, brightness=bri_i)
                if len(rgb) != payload_len:
                    rgb = (rgb[:payload_len]).ljust(payload_len, b"\x00")
                frame = bytearray(int(channels_total))
                end = min(int(channels_total), off + payload_len)
                frame[off:end] = rgb[: (end - off)]
                frame_idx += 1
                yield bytes(frame)

    res = write_fseq_v1_file(
        out_path=str(out_path),
        channel_count=int(channels_total),
        num_frames=int(total_frames),
        step_ms=int(step_ms),
        frame_generator=_frames(),
    )
    return {
        "render": {
            "led_count": int(led_count),
            "channel_start": int(channel_start),
            "channels_total": int(channels_total),
            "step_ms": int(step_ms),
        },
        "fseq": res.__dict__,
    }

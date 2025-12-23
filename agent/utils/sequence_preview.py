from __future__ import annotations

import json
import math
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List

from geometry import TreeGeometry
from patterns import PatternFactory
from segment_layout import SegmentLayout


def _downsample_rgb(
    rgb: bytes, *, led_count: int, width: int, height: int
) -> bytes:
    if led_count <= 0 or width <= 0 or height <= 0:
        return b""
    expected = int(led_count) * 3
    if len(rgb) < expected:
        rgb = rgb.ljust(expected, b"\x00")
    elif len(rgb) > expected:
        rgb = rgb[:expected]

    row = bytearray(width * 3)
    for x in range(width):
        start = int(x * led_count / width)
        end = int((x + 1) * led_count / width)
        if end <= start:
            end = min(led_count, start + 1)
        start_b = start * 3
        end_b = min(expected, end * 3)
        if start_b >= end_b:
            continue
        count = max(1, (end_b - start_b) // 3)
        r_sum = 0
        g_sum = 0
        b_sum = 0
        for idx in range(start_b, end_b, 3):
            r_sum += rgb[idx]
            g_sum += rgb[idx + 1]
            b_sum += rgb[idx + 2]
        row[x * 3] = int(r_sum / count)
        row[x * 3 + 1] = int(g_sum / count)
        row[x * 3 + 2] = int(b_sum / count)

    if height == 1:
        return bytes(row)
    return bytes(row) * int(height)


def _render_frame_stream(
    *,
    steps: Iterable[Dict[str, Any]],
    led_count: int,
    geometry: TreeGeometry,
    segment_layout: SegmentLayout | None,
    width: int,
    height: int,
    fps: float,
    max_duration_s: float,
    default_bri: int,
    max_bri: int,
    strict: bool,
) -> Iterable[bytes]:
    factory = PatternFactory(
        led_count=int(led_count),
        geometry=geometry,
        segment_layout=segment_layout,
    )
    last_frame = bytes(width * height * 3)
    elapsed = 0.0
    frame_idx = 0
    for step in steps:
        if max_duration_s and elapsed >= max_duration_s:
            break
        if not isinstance(step, dict):
            continue
        dur_s = float(step.get("duration_s") or 0.0)
        if dur_s <= 0:
            continue
        if max_duration_s and elapsed + dur_s > max_duration_s:
            dur_s = max(0.0, max_duration_s - elapsed)
        if dur_s <= 0:
            break
        nframes = max(1, int(math.ceil(dur_s * fps)))
        typ = str(step.get("type") or "").strip().lower()
        bri = step.get("brightness")
        bri_i = (
            int(default_bri)
            if bri is None
            else min(int(max_bri), max(1, int(bri)))
        )

        if typ == "ddp":
            pat_name = str(step.get("pattern") or "").strip()
            if not pat_name:
                if strict:
                    raise RuntimeError("DDP step missing pattern")
                pat = None
            else:
                params = step.get("params") or {}
                if not isinstance(params, dict):
                    params = {}
                pat = factory.create(pat_name, params=params)
            for i in range(nframes):
                t = (i / float(fps)) if fps > 0 else 0.0
                if pat is None:
                    frame = last_frame
                else:
                    rgb = pat.frame(t=t, frame_idx=frame_idx, brightness=bri_i)
                    frame = _downsample_rgb(
                        rgb,
                        led_count=int(led_count),
                        width=int(width),
                        height=int(height),
                    )
                    last_frame = frame
                frame_idx += 1
                yield frame
        else:
            if strict:
                raise RuntimeError(f"Non-renderable step type '{typ}'")
            for _ in range(nframes):
                frame_idx += 1
                yield last_frame

        elapsed += dur_s


def render_sequence_preview(
    *,
    seq_path: str,
    out_path: str,
    led_count: int,
    geometry: TreeGeometry,
    segment_layout: SegmentLayout | None,
    width: int,
    height: int,
    fps: float,
    max_duration_s: float,
    default_bri: int,
    max_bri: int,
    strict: bool,
    format: str,
) -> Dict[str, Any]:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("ffmpeg not available for preview rendering")

    fmt = str(format or "gif").strip().lower()
    if fmt not in ("gif", "mp4"):
        raise RuntimeError("format must be gif or mp4")

    with open(seq_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    steps = list(payload.get("steps", [])) if isinstance(payload, dict) else []

    width_i = max(16, int(width))
    height_i = max(1, int(height))
    fps_f = max(1.0, float(fps))

    cmd = [
        ffmpeg,
        "-y",
        "-loglevel",
        "error",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "-s",
        f"{width_i}x{height_i}",
        "-r",
        f"{fps_f:.3f}",
        "-i",
        "pipe:0",
    ]
    if fmt == "gif":
        cmd += [
            "-vf",
            f"fps={fps_f:.3f},scale={width_i}:{height_i}:flags=neighbor",
            "-loop",
            "0",
            out_path,
        ]
    else:
        cmd += [
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-preset",
            "veryfast",
            "-movflags",
            "+faststart",
            out_path,
        ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    assert proc.stdin is not None
    frames = 0
    for frame in _render_frame_stream(
        steps=steps,
        led_count=int(led_count),
        geometry=geometry,
        segment_layout=segment_layout,
        width=width_i,
        height=height_i,
        fps=fps_f,
        max_duration_s=float(max_duration_s),
        default_bri=int(default_bri),
        max_bri=int(max_bri),
        strict=bool(strict),
    ):
        if not frame:
            continue
        proc.stdin.write(frame)
        frames += 1
    proc.stdin.close()
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"ffmpeg failed with code {rc}")

    return {
        "frames": int(frames),
        "width": int(width_i),
        "height": int(height_i),
        "fps": float(fps_f),
        "format": fmt,
        "out_path": str(out_path),
    }

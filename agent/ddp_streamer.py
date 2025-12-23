from __future__ import annotations

import asyncio
import pickle
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ddp_sender import DDPAsyncSender, DDPConfig
from geometry import TreeGeometry
from patterns import PatternFactory
from segment_layout import fetch_segment_layout_async
from services.blocking_service import BlockingQueueFull
from utils.blocking import run_cpu_blocking
from wled_client import AsyncWLEDClient


@dataclass
class StreamStatus:
    running: bool
    pattern: str | None
    fps: float | None
    started_at: float | None
    frames_sent: int


@dataclass
class StreamMetrics:
    frames_sent_total: int = 0
    frames_dropped_total: int = 0
    frame_overruns_total: int = 0
    frame_compute_seconds_sum: float = 0.0
    frame_compute_seconds_count: int = 0
    frame_lag_seconds_sum: float = 0.0
    frame_lag_seconds_count: int = 0
    last_frame_compute_s: float | None = None
    last_frame_lag_s: float | None = None
    max_frame_lag_s: float = 0.0


class DDPStreamer:
    def __init__(
        self,
        *,
        wled: AsyncWLEDClient,
        geometry: TreeGeometry,
        ddp_cfg: DDPConfig,
        fps_default: float = 20.0,
        fps_max: float = 45.0,
        drop_late_frames: bool = True,
        max_lag_s: float = 0.25,
        segment_ids: Optional[list[int]] = None,
        blocking: Any | None = None,
        cpu_pool: Any | None = None,
    ) -> None:
        self.wled = wled
        self.geometry = geometry
        self.ddp_cfg = ddp_cfg
        self.fps_default = fps_default
        self.fps_max = fps_max
        self.drop_late_frames = bool(drop_late_frames)
        self.max_lag_s = max(0.0, float(max_lag_s))

        self.segment_ids = list(segment_ids) if segment_ids else None
        self._blocking = blocking
        self._cpu_pool = cpu_pool

        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._status = StreamStatus(
            running=False, pattern=None, fps=None, started_at=None, frames_sent=0
        )
        self._metrics = StreamMetrics()

    async def status(self) -> StreamStatus:
        async with self._lock:
            return StreamStatus(**self._status.__dict__)

    async def metrics(self) -> StreamMetrics:
        async with self._lock:
            return StreamMetrics(**self._metrics.__dict__)

    async def _cleanup_after_run(self) -> None:
        async with self._lock:
            if not self._status.running:
                return
            self._status.running = False
            self._status.pattern = None
            self._status.fps = None
            self._task = None
        try:
            await self.wled.exit_live_mode()
        except Exception:
            pass

    async def stop(self) -> StreamStatus:
        async with self._lock:
            if not self._status.running:
                return StreamStatus(**self._status.__dict__)
            self._stop.set()
            task = self._task

        if task is not None:
            try:
                task.cancel()
            except Exception:
                pass
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        await self._cleanup_after_run()
        async with self._lock:
            return StreamStatus(**self._status.__dict__)

    async def start(
        self,
        *,
        pattern: str,
        params: Optional[Dict[str, Any]] = None,
        duration_s: float = 30.0,
        brightness: int = 128,
        fps: Optional[float] = None,
    ) -> StreamStatus:
        fps_val = float(fps if fps is not None else self.fps_default)
        fps_val = max(1.0, min(self.fps_max, fps_val))
        duration_s = max(0.1, float(duration_s))
        brightness = max(0, min(255, int(brightness)))

        # Stop any existing stream
        await self.stop()

        # Build factory + pattern instance.
        layout = None
        try:
            layout = await fetch_segment_layout_async(
                self.wled, segment_ids=self.segment_ids, refresh=True
            )
        except Exception:
            layout = None

        led_count = int(getattr(layout, "led_count", 0) or 0) if layout else 0
        if led_count <= 0:
            raise RuntimeError("WLED returned led_count=0; cannot stream DDP")

        factory = PatternFactory(
            led_count=led_count, geometry=self.geometry, segment_layout=layout
        )
        pat = factory.create(pattern, params=params or {})

        # Best-effort enter live mode.
        try:
            await self.wled.enter_live_mode()
            await self.wled.set_brightness(brightness)
        except Exception:
            # Not fatal; DDP might still work without forcing live mode
            pass

        self._stop.clear()

        async with self._lock:
            self._status.running = True
            self._status.pattern = pattern
            self._status.fps = fps_val
            self._status.started_at = time.time()
            self._status.frames_sent = 0
            self._metrics.last_frame_compute_s = None
            self._metrics.last_frame_lag_s = None
            self._metrics.max_frame_lag_s = 0.0
            self._task = asyncio.create_task(
                self._run_stream(
                    pat=pat,
                    duration_s=duration_s,
                    brightness=brightness,
                    fps_val=fps_val,
                ),
                name="ddp_streamer",
            )
            return StreamStatus(**self._status.__dict__)

    async def _run_stream(
        self,
        *,
        pat: Any,
        duration_s: float,
        brightness: int,
        fps_val: float,
    ) -> None:
        sender: DDPAsyncSender | None = None
        frame_idx = 0
        compute_pool = self._cpu_pool or self._blocking
        if self._cpu_pool is not None:
            # Process pools require picklable pattern instances; fall back if needed.
            try:
                pickle.dumps(pat.frame)
            except Exception:
                compute_pool = self._blocking
        try:
            sender = DDPAsyncSender(self.ddp_cfg)
            start_ts = time.monotonic()
            next_frame = start_ts
            frame_period = max(0.001, 1.0 / fps_val)
            while not self._stop.is_set():
                now = time.monotonic()
                if now >= (start_ts + duration_s):
                    break
                if now < next_frame:
                    await asyncio.sleep(min(0.01, next_frame - now))
                    continue
                lag_s = max(0.0, now - next_frame)
                dropped = 0
                if self.drop_late_frames and lag_s > self.max_lag_s:
                    drops = max(1, int(lag_s / frame_period))
                    if drops > 0:
                        dropped = drops
                        next_frame += float(drops) * frame_period
                        lag_s = max(0.0, now - next_frame)
                t = now - start_ts
                frame_start = time.perf_counter()
                try:
                    rgb = await run_cpu_blocking(
                        compute_pool,
                        pat.frame,
                        t=t,
                        frame_idx=frame_idx,
                        brightness=brightness,
                    )
                except BlockingQueueFull:
                    async with self._lock:
                        self._metrics.frames_dropped_total += 1
                    next_frame += frame_period
                    continue
                except Exception:
                    async with self._lock:
                        self._metrics.frames_dropped_total += 1
                    next_frame += frame_period
                    continue

                await sender.send_frame(rgb)
                frame_compute_s = max(0.0, time.perf_counter() - frame_start)
                overrun = frame_compute_s > frame_period
                frame_idx += 1

                async with self._lock:
                    self._status.frames_sent = int(frame_idx)
                    self._metrics.frames_sent_total += 1
                    if dropped:
                        self._metrics.frames_dropped_total += int(dropped)
                    if overrun:
                        self._metrics.frame_overruns_total += 1
                    self._metrics.frame_compute_seconds_sum += float(frame_compute_s)
                    self._metrics.frame_compute_seconds_count += 1
                    self._metrics.frame_lag_seconds_sum += float(lag_s)
                    self._metrics.frame_lag_seconds_count += 1
                    self._metrics.last_frame_compute_s = float(frame_compute_s)
                    self._metrics.last_frame_lag_s = float(lag_s)
                    if lag_s > self._metrics.max_frame_lag_s:
                        self._metrics.max_frame_lag_s = float(lag_s)
                next_frame += frame_period
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            if sender is not None:
                sender.close()
            await self._cleanup_after_run()

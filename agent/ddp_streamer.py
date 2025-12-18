from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ddp_sender import DDPConfig, DDPSender
from geometry import TreeGeometry
from segment_layout import fetch_segment_layout
from patterns import PatternFactory
from wled_client import WLEDClient


@dataclass
class StreamStatus:
    running: bool
    pattern: str | None
    fps: float | None
    started_at: float | None
    frames_sent: int


class DDPStreamer:
    def __init__(
        self,
        *,
        wled: WLEDClient,
        geometry: TreeGeometry,
        ddp_cfg: DDPConfig,
        fps_default: float = 20.0,
        fps_max: float = 45.0,
        segment_ids: Optional[list[int]] = None,
    ) -> None:
        self.wled = wled
        self.geometry = geometry
        self.ddp_cfg = ddp_cfg
        self.fps_default = fps_default
        self.fps_max = fps_max

        self.segment_ids = list(segment_ids) if segment_ids else None

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._status = StreamStatus(
            running=False, pattern=None, fps=None, started_at=None, frames_sent=0
        )

    def status(self) -> StreamStatus:
        with self._lock:
            return StreamStatus(**self._status.__dict__)

    def stop(self) -> StreamStatus:
        with self._lock:
            if not self._status.running:
                return StreamStatus(**self._status.__dict__)
            self._stop.set()
            th = self._thread
        if th:
            th.join(timeout=2.5)
        # best effort exit live mode
        try:
            self.wled.exit_live_mode()
        except Exception:
            pass
        with self._lock:
            self._status.running = False
            self._status.pattern = None
            self._status.fps = None
            self._thread = None
        return self.status()

    def start(
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
        self.stop()

        # Query LED count
        info = self.wled.device_info()
        led_count = info.led_count
        if led_count <= 0:
            raise RuntimeError("WLED returned led_count=0; cannot stream DDP")

        # Build factory + pattern instance
        layout = None
        try:
            layout = fetch_segment_layout(
                self.wled, segment_ids=self.segment_ids, refresh=True
            )
        except Exception:
            layout = None

        factory = PatternFactory(
            led_count=led_count, geometry=self.geometry, segment_layout=layout
        )
        pat = factory.create(pattern, params=params or {})

        # Best-effort enter live mode
        try:
            self.wled.enter_live_mode()
            self.wled.set_brightness(brightness)
        except Exception:
            # Not fatal; DDP might still work without forcing live mode
            pass

        self._stop.clear()
        sender = DDPSender(self.ddp_cfg)

        def _run() -> None:
            start_ts = time.monotonic()
            next_frame = start_ts
            frame_idx = 0
            try:
                while not self._stop.is_set():
                    now = time.monotonic()
                    if now >= (start_ts + duration_s):
                        break
                    if now < next_frame:
                        time.sleep(min(0.01, next_frame - now))
                        continue
                    t = now - start_ts
                    rgb = pat.frame(t=t, frame_idx=frame_idx, brightness=brightness)
                    sender.send_frame(rgb)
                    frame_idx += 1
                    with self._lock:
                        self._status.frames_sent = frame_idx
                    next_frame += 1.0 / fps_val
            finally:
                sender.close()
                # best-effort exit live mode
                try:
                    self.wled.exit_live_mode()
                except Exception:
                    pass
                with self._lock:
                    self._status.running = False
                    self._status.pattern = None
                    self._status.fps = None
                    self._thread = None

        th = threading.Thread(target=_run, name="ddp_streamer", daemon=True)
        with self._lock:
            self._status.running = True
            self._status.pattern = pattern
            self._status.fps = fps_val
            self._status.started_at = time.time()
            self._status.frames_sent = 0
            self._thread = th

        th.start()
        return self.status()

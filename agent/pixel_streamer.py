from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from artnet_sender import ArtNetConfig, ArtNetSender
from e131_sender import E131Config, E131Sender
from geometry import TreeGeometry
from patterns import PatternFactory


@dataclass(frozen=True)
class PixelStreamConfig:
    protocol: str  # "e131" or "artnet"
    host: str
    port: int
    universe_start: int
    channels_per_universe: int
    priority: int = 100
    source_name: str = "wled-show-agent"


@dataclass
class StreamStatus:
    running: bool
    pattern: str | None
    fps: float | None
    started_at: float | None
    frames_sent: int


class PixelStreamer:
    def __init__(
        self,
        *,
        led_count: int,
        geometry: TreeGeometry,
        cfg: PixelStreamConfig,
        fps_default: float = 20.0,
        fps_max: float = 45.0,
    ) -> None:
        self.led_count = int(led_count)
        if self.led_count <= 0:
            raise ValueError("led_count must be > 0")
        self.geometry = geometry
        self.cfg = cfg
        self.fps_default = fps_default
        self.fps_max = fps_max

        proto = str(cfg.protocol).strip().lower()
        if proto == "artnet":
            self._sender = ArtNetSender(
                ArtNetConfig(
                    host=cfg.host,
                    port=int(cfg.port),
                    universe_start=int(cfg.universe_start),
                    channels_per_universe=int(cfg.channels_per_universe),
                )
            )
        elif proto == "e131":
            self._sender = E131Sender(
                E131Config(
                    host=cfg.host,
                    port=int(cfg.port),
                    universe_start=int(cfg.universe_start),
                    channels_per_universe=int(cfg.channels_per_universe),
                    priority=int(cfg.priority),
                    source_name=str(cfg.source_name or "wled-show-agent"),
                )
            )
        else:
            raise ValueError("protocol must be 'e131' or 'artnet'")

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

        factory = PatternFactory(
            led_count=self.led_count, geometry=self.geometry, segment_layout=None
        )
        pat = factory.create(pattern, params=params or {})

        self._stop.clear()

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
                    self._sender.send_frame(rgb)
                    frame_idx += 1
                    with self._lock:
                        self._status.frames_sent = frame_idx
                    next_frame += 1.0 / fps_val
            finally:
                with self._lock:
                    self._status.running = False
                    self._status.pattern = None
                    self._status.fps = None
                    self._thread = None

        th = threading.Thread(target=_run, name="pixel_streamer", daemon=True)
        with self._lock:
            self._status.running = True
            self._status.pattern = pattern
            self._status.fps = fps_val
            self._status.started_at = time.time()
            self._status.frames_sent = 0
            self._thread = th

        th.start()
        return self.status()

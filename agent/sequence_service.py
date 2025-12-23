from __future__ import annotations

import time
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from ddp_streamer import DDPStreamer
from look_service import LookService
from pack_io import read_json_async
from utils.blocking import run_cpu_blocking
from utils.sequence_generate import generate_sequence_file
from wled_client import AsyncWLEDClient


@dataclass
class SequenceStatus:
    running: bool
    file: str | None
    started_at: float | None
    step_index: int
    steps_total: int
    loop: bool


class SequenceService:
    def __init__(
        self,
        *,
        wled: AsyncWLEDClient,
        looks: LookService,
        ddp: DDPStreamer,
        data_dir: str,
        blocking: Any | None = None,
        cpu_pool: Any | None = None,
    ) -> None:
        self.wled = wled
        self.looks = looks
        self.ddp = ddp
        self.data_dir = data_dir
        self._blocking = blocking
        self._cpu_pool = cpu_pool

        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._status = SequenceStatus(
            running=False,
            file=None,
            started_at=None,
            step_index=0,
            steps_total=0,
            loop=False,
        )

    def _seq_dir(self) -> Path:
        d = Path(self.data_dir) / "sequences"
        d.mkdir(parents=True, exist_ok=True)
        return d

    async def list_sequences(self) -> List[str]:
        def _run() -> List[str]:
            return sorted([p.name for p in self._seq_dir().glob("sequence_*.json")])

        return await run_cpu_blocking(self._cpu_pool, _run)

    def generate(
        self,
        *,
        name: str,
        looks: List[Dict[str, Any]],
        duration_s: int,
        step_s: int,
        include_ddp: bool,
        renderable_only: bool = False,
        beats_s: Optional[Sequence[float]] = None,
        beats_per_step: int = 4,
        beat_offset_s: float = 0.0,
        ddp_patterns: List[str],
        seed: int,
    ) -> str:
        return generate_sequence_file(
            data_dir=self.data_dir,
            name=name,
            looks=looks,
            duration_s=duration_s,
            step_s=step_s,
            include_ddp=include_ddp,
            renderable_only=renderable_only,
            beats_s=beats_s,
            beats_per_step=beats_per_step,
            beat_offset_s=beat_offset_s,
            ddp_patterns=ddp_patterns,
            seed=seed,
        )

    async def status(self) -> SequenceStatus:
        async with self._lock:
            return SequenceStatus(**self._status.__dict__)

    async def stop(self) -> SequenceStatus:
        async with self._lock:
            if not self._status.running:
                return SequenceStatus(**self._status.__dict__)
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

        try:
            await self.ddp.stop()
        except Exception:
            pass

        async with self._lock:
            self._task = None
            self._status.running = False
            self._status.file = None
            self._status.started_at = None
            self._status.step_index = 0
            self._status.steps_total = 0
            self._status.loop = False
            return SequenceStatus(**self._status.__dict__)

    async def play(self, *, file: str, loop: bool = False) -> SequenceStatus:
        # Stop current.
        await self.stop()
        seq_path = self._seq_dir() / file
        seq = await read_json_async(str(seq_path))
        steps: List[Dict[str, Any]] = list((seq or {}).get("steps", []))
        if not steps:
            raise RuntimeError("Sequence has no steps")

        self._stop.clear()

        async def _sleep_interruptible(seconds: float) -> None:
            dur_s = max(0.05, float(seconds))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=dur_s)
            except asyncio.TimeoutError:
                return

        async def _run() -> None:
            try:
                while not self._stop.is_set():
                    for i, step in enumerate(steps):
                        async with self._lock:
                            self._status.step_index = int(i)
                            self._status.steps_total = int(len(steps))
                        if self._stop.is_set():
                            break

                        dur = float(step.get("duration_s", 5))
                        typ = str(step.get("type") or "").strip().lower()

                        if typ == "look":
                            look = step.get("look") or {}
                            bri = step.get("brightness")
                            bri_i = int(bri) if bri is not None else None
                            await self.looks.apply_look(look, brightness_override=bri_i)
                            await _sleep_interruptible(dur)
                        elif typ == "preset":
                            await self.wled.set_preset(
                                int(step.get("preset_id", 1)), verbose=False
                            )
                            await _sleep_interruptible(dur)
                        elif typ == "ddp":
                            pat = str(step.get("pattern"))
                            params = step.get("params") or {}
                            await self.ddp.start(
                                pattern=pat,
                                params=params,
                                duration_s=dur,
                                brightness=int(step.get("brightness", 128)),
                                fps=float(step.get("fps", self.ddp.fps_default)),
                            )
                            await _sleep_interruptible(dur)
                            await self.ddp.stop()
                        else:
                            await _sleep_interruptible(dur)

                    if not loop:
                        break
            finally:
                async with self._lock:
                    self._status.running = False
                    self._status.file = None
                    self._status.loop = False
                    self._task = None

        task = asyncio.create_task(_run(), name="sequence_runner")
        async with self._lock:
            self._status.running = True
            self._status.file = file
            self._status.started_at = time.time()
            self._status.step_index = 0
            self._status.steps_total = len(steps)
            self._status.loop = bool(loop)
            self._task = task
            return SequenceStatus(**self._status.__dict__)

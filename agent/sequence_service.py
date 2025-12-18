from __future__ import annotations

import random
import threading
import time
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from ddp_streamer import DDPStreamer
from look_service import LookService
from pack_io import nowstamp, read_json, write_json
from wled_client import WLEDClient


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
        self, *, wled: WLEDClient, looks: LookService, ddp: DDPStreamer, data_dir: str
    ) -> None:
        self.wled = wled
        self.looks = looks
        self.ddp = ddp
        self.data_dir = data_dir

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
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

    def list_sequences(self) -> List[str]:
        return sorted([p.name for p in self._seq_dir().glob("sequence_*.json")])

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
        """
        Generate a sequence JSON file under DATA_DIR/sequences.

        - Default mode uses a fixed `step_s` duration per step (last step is shortened to fit `duration_s`).
        - If `beats_s` is provided, step durations are derived from the beat grid (each step spans
          `beats_per_step` beats), with an optional `beat_offset_s` applied.
        - If `renderable_only=True`, steps are always procedural `ddp` patterns (no WLED "look" steps).
        """
        rng = random.Random(seed)
        total_duration_s = max(0.1, float(duration_s))
        fixed_step_s = max(1, int(step_s))
        beats_per_step = max(1, int(beats_per_step))
        beat_offset_s = float(beat_offset_s)

        # Step durations are either fixed (step_s) or derived from a beat grid.
        step_durations_s: List[float] = []
        if beats_s is not None and len(beats_s) >= 2:
            raw_beats = [float(x) + beat_offset_s for x in beats_s]
            beat_marks = sorted({x for x in raw_beats if x >= 0.0})
            if beat_marks and beat_marks[0] > 1e-6:
                beat_marks.insert(0, 0.0)
            if len(beat_marks) >= 2:
                boundaries: List[float] = [beat_marks[0]]
                idx = 0
                while (
                    idx + beats_per_step < len(beat_marks)
                    and boundaries[-1] < total_duration_s
                ):
                    nxt = float(beat_marks[idx + beats_per_step])
                    if nxt > boundaries[-1] + 1e-6:
                        boundaries.append(nxt)
                    idx += beats_per_step
                    if boundaries[-1] >= total_duration_s:
                        break
                if boundaries[-1] < total_duration_s:
                    boundaries.append(total_duration_s)
                for i in range(len(boundaries) - 1):
                    dur = float(boundaries[i + 1] - boundaries[i])
                    if dur > 1e-3:
                        step_durations_s.append(max(0.05, dur))

        if not step_durations_s:
            steps_n = max(1, int(math.ceil(total_duration_s / float(fixed_step_s))))
            elapsed = 0.0
            for i in range(steps_n):
                remaining = total_duration_s - elapsed
                dur = (
                    float(fixed_step_s)
                    if i < steps_n - 1
                    else float(max(0.1, remaining))
                )
                step_durations_s.append(dur)
                elapsed += dur

        median_step_s = (
            float(statistics.median(step_durations_s))
            if step_durations_s
            else float(fixed_step_s)
        )
        ddp_every_s = float(max(30.0, 10.0 * median_step_s))
        next_ddp_at_s = ddp_every_s

        steps: List[Dict[str, Any]] = []
        elapsed_s = 0.0
        for i, dur_s in enumerate(step_durations_s):
            # Renderable-only sequences must be procedural patterns (no WLED "look" steps).
            if renderable_only:
                if not ddp_patterns:
                    raise RuntimeError(
                        "No DDP patterns available for renderable_only sequence"
                    )
                pat = rng.choice(ddp_patterns)
                params = {}
                # randomize a few known params
                if pat in ("candy_spiral",):
                    params = {
                        "speed": rng.choice([0.3, 0.5, 0.8]),
                        "stripes": rng.choice([6, 8, 10]),
                        "twist": rng.choice([0.8, 1.2, 1.6]),
                    }
                elif pat in ("rainbow_cycle", "glitter_rainbow"):
                    params = {
                        "speed": rng.choice([0.04, 0.06, 0.08]),
                        "spread": rng.choice([1.0, 1.3, 2.0]),
                    }
                elif pat in ("comet",):
                    params = {
                        "speed": rng.choice([150.0, 220.0, 320.0]),
                        "tail": rng.choice([40, 70, 120]),
                    }
                elif pat in ("snowfall",):
                    params = {
                        "density": rng.choice([0.004, 0.008, 0.015]),
                        "speed": rng.choice([0.1, 0.15, 0.25]),
                    }
                elif pat in ("matrix_rain",):
                    params = {
                        "streaks_per_run": rng.choice([1, 2, 3]),
                        "seed": rng.randrange(1, 999999),
                    }
                steps.append(
                    {
                        "type": "ddp",
                        "pattern": pat,
                        "params": params,
                        "duration_s": float(dur_s),
                    }
                )
                elapsed_s += float(dur_s)
                continue

            # Insert a DDP step occasionally (roughly every ~max(30s, 10 steps)).
            if include_ddp and ddp_patterns and (elapsed_s >= next_ddp_at_s) and i != 0:
                pat = rng.choice(ddp_patterns)
                params = {}
                # randomize a few known params
                if pat in ("candy_spiral",):
                    params = {
                        "speed": rng.choice([0.3, 0.5, 0.8]),
                        "stripes": rng.choice([6, 8, 10]),
                        "twist": rng.choice([0.8, 1.2, 1.6]),
                    }
                elif pat in ("rainbow_cycle", "glitter_rainbow"):
                    params = {
                        "speed": rng.choice([0.04, 0.06, 0.08]),
                        "spread": rng.choice([1.0, 1.3, 2.0]),
                    }
                elif pat in ("comet",):
                    params = {
                        "speed": rng.choice([150.0, 220.0, 320.0]),
                        "tail": rng.choice([40, 70, 120]),
                    }
                elif pat in ("snowfall",):
                    params = {
                        "density": rng.choice([0.004, 0.008, 0.015]),
                        "speed": rng.choice([0.1, 0.15, 0.25]),
                    }
                elif pat in ("matrix_rain",):
                    params = {
                        "streaks_per_run": rng.choice([1, 2, 3]),
                        "seed": rng.randrange(1, 999999),
                    }
                steps.append(
                    {
                        "type": "ddp",
                        "pattern": pat,
                        "params": params,
                        "duration_s": float(dur_s),
                    }
                )
                next_ddp_at_s += ddp_every_s
            else:
                look = rng.choice(looks)
                steps.append({"type": "look", "look": look, "duration_s": float(dur_s)})
            elapsed_s += float(dur_s)

        seq = {
            "name": name,
            "created": nowstamp(),
            "steps": steps,
        }
        fname = f"sequence_{name}_{nowstamp()}.json"
        write_json(str(self._seq_dir() / fname), seq)
        return fname

    def status(self) -> SequenceStatus:
        with self._lock:
            return SequenceStatus(**self._status.__dict__)

    def stop(self) -> SequenceStatus:
        with self._lock:
            if not self._status.running:
                return SequenceStatus(**self._status.__dict__)
            self._stop.set()
            th = self._thread
        if th:
            th.join(timeout=2.5)
        try:
            self.ddp.stop()
        except Exception:
            pass
        with self._lock:
            self._status.running = False
            self._status.file = None
            self._status.started_at = None
            self._status.step_index = 0
            self._status.steps_total = 0
            self._status.loop = False
            self._thread = None
        return self.status()

    def play(self, *, file: str, loop: bool = False) -> SequenceStatus:
        # Stop current
        self.stop()
        seq_path = self._seq_dir() / file
        seq = read_json(str(seq_path))
        steps: List[Dict[str, Any]] = list(seq.get("steps", []))
        if not steps:
            raise RuntimeError("Sequence has no steps")

        self._stop.clear()

        def _run() -> None:
            try:
                while not self._stop.is_set():
                    for i, step in enumerate(steps):
                        with self._lock:
                            self._status.step_index = i
                            self._status.steps_total = len(steps)
                        if self._stop.is_set():
                            break
                        dur = float(step.get("duration_s", 5))
                        typ = step.get("type")
                        if typ == "look":
                            look = step.get("look") or {}
                            bri = step.get("brightness")
                            self.looks.apply_look(
                                look,
                                brightness_override=(
                                    int(bri) if bri is not None else None
                                ),
                            )
                            time.sleep(dur)
                        elif typ == "preset":
                            self.wled.set_preset(
                                int(step.get("preset_id", 1)), verbose=False
                            )
                            time.sleep(dur)
                        elif typ == "ddp":
                            pat = str(step.get("pattern"))
                            params = step.get("params") or {}
                            self.ddp.start(
                                pattern=pat,
                                params=params,
                                duration_s=dur,
                                brightness=int(step.get("brightness", 128)),
                                fps=float(step.get("fps", self.ddp.fps_default)),
                            )
                            time.sleep(dur)
                            self.ddp.stop()
                        else:
                            time.sleep(dur)
                    if not loop:
                        break
            finally:
                with self._lock:
                    self._status.running = False
                    self._status.file = None
                    self._status.loop = False
                    self._thread = None

        th = threading.Thread(target=_run, name="sequence_runner", daemon=True)
        with self._lock:
            self._status.running = True
            self._status.file = file
            self._status.started_at = time.time()
            self._status.step_index = 0
            self._status.steps_total = len(steps)
            self._status.loop = bool(loop)
            self._thread = th
        th.start()
        return self.status()

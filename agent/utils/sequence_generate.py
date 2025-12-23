from __future__ import annotations

import math
import random
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from pack_io import nowstamp, write_json


def generate_sequence_file(
    *,
    data_dir: str,
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
        if renderable_only:
            if not ddp_patterns:
                raise RuntimeError(
                    "No DDP patterns available for renderable_only sequence"
                )
            pat = rng.choice(ddp_patterns)
            params = {}
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

        if include_ddp and ddp_patterns and (elapsed_s >= next_ddp_at_s) and i != 0:
            pat = rng.choice(ddp_patterns)
            params = {}
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
    seq_dir = Path(data_dir) / "sequences"
    seq_dir.mkdir(parents=True, exist_ok=True)
    fname = f"sequence_{name}_{nowstamp()}.json"
    write_json(str(seq_dir / fname), seq)
    return fname

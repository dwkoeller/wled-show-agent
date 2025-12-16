from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class TreeGeometry:
    runs: int
    pixels_per_run: int
    segment_len: int
    segments_per_run: int

    @property
    def total_pixels(self) -> int:
        return self.runs * self.pixels_per_run

    def enabled_for(self, led_count: int) -> bool:
        return self.total_pixels > 0 and self.total_pixels == led_count

    def idx_to_run_pos(self, idx: int) -> Tuple[int, int]:
        """Map linear index -> (run, pos_in_run)."""
        run = idx // self.pixels_per_run
        pos = idx % self.pixels_per_run
        return run, pos

    def coords(self, idx: int) -> Tuple[float, float, float]:
        """
        Return a simple cylindrical coordinate triple (angle, y, z).
        - angle: 0..2π around the tree
        - y: 0..1 from bottom to top
        - z: alias of y (for convenience)
        """
        run, pos = self.idx_to_run_pos(idx)
        angle = (run / max(1, self.runs)) * (2.0 * math.pi)
        y = pos / max(1, (self.pixels_per_run - 1))
        return angle, y, y

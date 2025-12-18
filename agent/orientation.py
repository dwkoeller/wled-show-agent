from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


def _norm_dir(val: str | None) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ("cw", "clockwise"):
        return "cw"
    if s in (
        "ccw",
        "counterclockwise",
        "counter-clockwise",
        "anticlockwise",
        "anti-clockwise",
    ):
        return "ccw"
    return None


def _norm_pos(val: str | None) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ("front", "street", "street-facing", "south"):
        return "front"
    if s in ("back", "rear", "north"):
        return "back"
    if s in ("left", "west"):
        return "left"
    if s in ("right", "east"):
        return "right"
    return None


@dataclass(frozen=True)
class OrientationInfo:
    """Street-facing orientation mapping for quadrant-style segment layouts."""

    kind: str  # "quarters" or "unknown"
    ordered_segment_ids: List[int]
    order_direction_from_street: str  # "cw" or "ccw" (meaning increasing segment order)
    right_segment_id: int
    positions: Dict[str, int]  # keys: front/back/left/right -> segment_id
    notes: List[str]

    def pos_to_id(self, pos: str) -> Optional[int]:
        return self.positions.get(pos)

    def pos_to_order_index(self, pos: str) -> Optional[int]:
        sid = self.pos_to_id(pos)
        if sid is None:
            return None
        try:
            return self.ordered_segment_ids.index(int(sid))
        except Exception:
            return None

    def phase_offset_for_pos(self, pos: str) -> Optional[float]:
        idx = self.pos_to_order_index(pos)
        return float(idx) if idx is not None else None

    def signed_speed_for_direction(self, desired_direction: str, speed: float) -> float:
        """Return speed with sign adjusted so motion matches desired_direction from the street."""
        d = _norm_dir(desired_direction) or desired_direction
        if d not in ("cw", "ccw"):
            return float(speed)
        base = abs(float(speed))
        # If increasing order matches desired direction, keep positive; else negative.
        return base if d == self.order_direction_from_street else -base


def infer_orientation(
    *,
    ordered_segment_ids: List[int],
    right_segment_id: int,
    order_direction_from_street: str = "ccw",
) -> OrientationInfo:
    """Infer a street-facing mapping (front/back/left/right) for a 4-segment tree.

    Assumptions (matches most mega-tree installs):
    - The *street* is in front of the tree.
    - "Right" and "Left" are from the street viewpoint (facing the house).
    - ordered_segment_ids is in physical order around the tree (usually sorted by WLED segment start index).
    - order_direction_from_street indicates whether that order goes clockwise or counterclockwise around the tree
      as seen from the street.
    """
    notes: List[str] = []

    if not ordered_segment_ids:
        return OrientationInfo(
            kind="unknown",
            ordered_segment_ids=[],
            order_direction_from_street=_norm_dir(order_direction_from_street) or "ccw",
            right_segment_id=int(right_segment_id),
            positions={},
            notes=["No segment IDs available."],
        )

    dir_norm = _norm_dir(order_direction_from_street) or "ccw"
    step = 1 if dir_norm == "ccw" else -1

    # Normalize right_segment_id to an element in ordered_segment_ids.
    right_sid = int(right_segment_id)
    if right_sid not in [int(x) for x in ordered_segment_ids]:
        right_sid = int(ordered_segment_ids[0])
        notes.append(
            "RIGHT_SEGMENT_ID not found; defaulting right to first ordered segment."
        )

    # Only a clean mapping for 4 segments.
    if len(ordered_segment_ids) != 4:
        notes.append(
            "Orientation mapping is only exact for 4 segments; returning unknown mapping."
        )
        return OrientationInfo(
            kind="unknown",
            ordered_segment_ids=[int(x) for x in ordered_segment_ids],
            order_direction_from_street=dir_norm,
            right_segment_id=right_sid,
            positions={"right": right_sid},
            notes=notes,
        )

    ordered = [int(x) for x in ordered_segment_ids]
    r_idx = ordered.index(right_sid)

    # With 4 equal quarters:
    # - next quarter in +step direction is "back"
    # - 2 steps is "left"
    # - 3 steps is "front"
    back = ordered[(r_idx + step) % 4]
    left = ordered[(r_idx + 2 * step) % 4]
    front = ordered[(r_idx + 3 * step) % 4]

    positions = {"right": right_sid, "back": back, "left": left, "front": front}

    notes.append(
        "Assuming 4 equal quadrants: right->back->left->front follows the configured order direction from street."
    )

    return OrientationInfo(
        kind="quarters",
        ordered_segment_ids=ordered,
        order_direction_from_street=dir_norm,
        right_segment_id=right_sid,
        positions=positions,
        notes=notes,
    )


def normalize_direction(val: str | None) -> Optional[str]:
    return _norm_dir(val)


def normalize_position(val: str | None) -> Optional[str]:
    return _norm_pos(val)

from __future__ import annotations

from typing import Any, Dict, Optional

from orientation import OrientationInfo, normalize_direction, normalize_position


# Patterns where "direction" and "start_pos" have a well-defined meaning as "around the tree".
QUADRANT_AWARE_PATTERNS = {
    "quad_chase",
    "quad_spiral",
    "quad_twinkle",
    "quad_comets",
    "opposite_pulse",
}

# Patterns that support a phase offset (segment-order offset) at t=0.
PHASE_OFFSET_PATTERNS = {"quad_chase", "quad_spiral"}

# Default speeds (only used if user specifies direction but omits speed).
DEFAULT_SPEEDS = {
    "quad_chase": 0.6,
    "quad_spiral": 0.18,
    "quad_comets": 0.22,
}


def prepare_ddp_params(
    *,
    pattern: str,
    params: Optional[Dict[str, Any]],
    orientation: Optional[OrientationInfo],
    default_start_pos: str = "front",
) -> Dict[str, Any]:
    """Normalize and enrich params for DDP patterns.

    Adds support for user-friendly fields:
    - direction: "cw" or "ccw" (from the street)
    - start_pos: "front"|"right"|"back"|"left" (from the street)
    - start_segment: explicit WLED segment ID to start from

    For quadrant-aware patterns, we translate these into:
    - speed sign (so "cw" / "ccw" does what you expect)
    - phase_offset (so the pattern can start at front/right/back/left)
    """
    pat = str(pattern).strip().lower()
    p: Dict[str, Any] = dict(params or {})

    # Pull out user-friendly controls (allow either top-level or inside params).
    direction = normalize_direction(p.pop("direction", None))
    start_pos = normalize_position(p.pop("start_pos", None))
    start_segment = p.pop("start_segment", None)

    if orientation is None:
        # No known mapping; keep params as-is.
        return p

    if pat in QUADRANT_AWARE_PATTERNS:
        # If caller did not specify a start position, apply a sensible default.
        if start_pos is None and default_start_pos:
            start_pos = normalize_position(default_start_pos)

        # Direction -> speed sign. Only apply if the pattern uses speed.
        if direction is not None:
            if "speed" in p:
                try:
                    p["speed"] = orientation.signed_speed_for_direction(direction, float(p.get("speed")))
                except Exception:
                    pass
            else:
                # Provide a default speed with the correct sign.
                base = DEFAULT_SPEEDS.get(pat)
                if base is not None:
                    p["speed"] = orientation.signed_speed_for_direction(direction, float(base))

        # Start position -> phase offset (segment-order offset). Only for patterns that support it.
        if pat in PHASE_OFFSET_PATTERNS and "phase_offset" not in p:
            # Prefer explicit start_segment if provided.
            if start_segment is not None:
                try:
                    sid = int(start_segment)
                    idx = orientation.ordered_segment_ids.index(sid)
                    p["phase_offset"] = float(idx)
                except Exception:
                    pass
            elif start_pos is not None:
                off = orientation.phase_offset_for_pos(start_pos)
                if off is not None:
                    p["phase_offset"] = float(off)

    return p

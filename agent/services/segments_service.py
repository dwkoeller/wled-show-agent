from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from fastapi import Depends, HTTPException

from orientation import OrientationInfo, infer_orientation
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


async def segments_layout(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        from segment_layout import fetch_segment_layout

        layout = await asyncio.to_thread(
            fetch_segment_layout,
            state.wled_sync,
            segment_ids=list(state.segment_ids or []),
            refresh=True,
        )
        return {
            "ok": True,
            "layout": {
                "kind": layout.kind,
                "led_count": layout.led_count,
                "segments": [
                    {"id": s.id, "start": s.start, "stop": s.stop, "len": s.length}
                    for s in layout.segments
                ],
                "ordered_ids": layout.ordered_ids(),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _get_orientation(
    state: AppState, *, refresh: bool = False
) -> Optional[OrientationInfo]:
    ordered = list(state.segment_ids or [])
    try:
        from segment_layout import fetch_segment_layout

        layout = await asyncio.to_thread(
            fetch_segment_layout,
            state.wled_sync,
            segment_ids=list(state.segment_ids or []),
            refresh=bool(refresh),
        )
        if layout and layout.segments:
            ordered = layout.ordered_ids()
    except Exception:
        ordered = list(state.segment_ids or [])

    if not ordered:
        return None

    s = state.settings
    try:
        return infer_orientation(
            ordered_segment_ids=[int(x) for x in ordered],
            right_segment_id=int(s.quad_right_segment_id),
            order_direction_from_street=str(s.quad_order_from_street),
        )
    except Exception:
        return None


async def segments_orientation(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """Return a best-effort street-facing quadrant mapping (front/right/back/left)."""
    ori = await _get_orientation(state, refresh=True)
    s = state.settings
    return {
        "ok": True,
        "configured": {
            "quad_right_segment_id": s.quad_right_segment_id,
            "quad_order_from_street": s.quad_order_from_street,
            "quad_default_start_pos": s.quad_default_start_pos,
        },
        "orientation": (
            None
            if ori is None
            else {
                "kind": ori.kind,
                "ordered_segment_ids": ori.ordered_segment_ids,
                "order_direction_from_street": ori.order_direction_from_street,
                "right_segment_id": ori.right_segment_id,
                "positions": ori.positions,
                "notes": ori.notes,
            }
        ),
    }

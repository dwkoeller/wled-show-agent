from __future__ import annotations

from typing import Any, Dict, List

from fastapi import Depends, HTTPException

from models.requests import ApplyStateRequest
from services.state import AppState, get_state
from wled_client import AsyncWLEDClient, WLEDError
from utils.outbound_http import retry_policy_from_settings


def _segment_ids(state: AppState) -> List[int]:
    ids = list(getattr(state, "segment_ids", []) or [])
    if ids:
        return ids
    ids = list(getattr(state.settings, "wled_segment_ids", []) or [])
    return ids if ids else [0]


def _client(state: AppState) -> AsyncWLEDClient:
    http = state.peer_http
    if http is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")
    return AsyncWLEDClient(
        state.settings.wled_tree_url,
        client=http,
        timeout_s=float(state.settings.wled_http_timeout_s),
        retry=retry_policy_from_settings(state.settings),
    )


async def wled_info(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    try:
        info = await _client(state).get_info()
        return {
            "ok": True,
            "info": info,
            "segment_ids": _segment_ids(state),
            "replicate_to_all_segments": state.settings.wled_replicate_to_all_segments,
        }
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def wled_state(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    try:
        st = await _client(state).get_state()
        return {"ok": True, "state": st}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def wled_segments(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    """Return the current segment list from WLED (useful when you have 2+ segments)."""
    try:
        segs = await _client(state).get_segments(refresh=True)
        return {"ok": True, "segment_ids": _segment_ids(state), "segments": segs}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def wled_presets(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    try:
        presets = await _client(state).get_presets_json(refresh=True)
        return {"ok": True, "presets": presets}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def wled_effects(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    try:
        effects = await _client(state).get_effects(refresh=True)
        return {"ok": True, "effects": list(effects)}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def wled_palettes(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    try:
        palettes = await _client(state).get_palettes(refresh=True)
        return {"ok": True, "palettes": list(palettes)}
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def wled_apply_state(
    req: ApplyStateRequest, state: AppState = Depends(get_state)
) -> Dict[str, Any]:
    try:
        payload = dict(req.state or {})
        # brightness safety
        if "bri" in payload:
            try:
                bri = int(payload.get("bri"))
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid brightness 'bri'")
            payload["bri"] = min(state.settings.wled_max_bri, max(1, bri))

        cd = getattr(state, "wled_cooldown", None)
        if cd is not None:
            await cd.wait()

        out = await _client(state).apply_state(payload, verbose=False)
        return {"ok": True, "result": out}
    except HTTPException:
        raise
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))

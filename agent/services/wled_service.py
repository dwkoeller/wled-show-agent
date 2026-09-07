from __future__ import annotations

import asyncio
import ipaddress
import time
from typing import Any, Dict, List

from fastapi import Depends, HTTPException

from models.requests import (
    ApplyStateRequest,
    WledCalibrationRequest,
    WledDiscoveryRequest,
    WledPreviewRequest,
)
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


def _safe_state(state: AppState, payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    if "bri" in out:
        try:
            out["bri"] = min(state.settings.wled_max_bri, max(1, int(out["bri"])))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid brightness 'bri': {exc}")
    return out


def _remember_undo(state: AppState, previous: Dict[str, Any]) -> None:
    if not previous:
        return
    state.wled_undo_states.insert(0, dict(previous))
    del state.wled_undo_states[3:]


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
        payload = _safe_state(state, req.state)
        previous = await _client(state).get_state()

        cd = getattr(state, "wled_cooldown", None)
        if cd is not None:
            await cd.wait()

        out = await _client(state).apply_state(payload, verbose=False)
        _remember_undo(state, previous)
        return {"ok": True, "result": out, "undo_available": bool(state.wled_undo_states)}
    except HTTPException:
        raise
    except WLEDError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def wled_preview_state(
    req: WledPreviewRequest, state: AppState = Depends(get_state)
) -> Dict[str, Any]:
    """Validate and clamp a state without contacting or changing the controller."""
    payload = _safe_state(state, req.state)
    return {
        "ok": True,
        "would_write": True,
        "requested": dict(req.state),
        "clamped": payload,
        "max_brightness": int(state.settings.wled_max_bri),
    }


async def wled_undo_state(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    if not state.wled_undo_states:
        raise HTTPException(status_code=409, detail="No WLED state is available to undo")
    previous = state.wled_undo_states.pop(0)
    try:
        result = await _client(state).apply_state(previous, verbose=False)
        return {"ok": True, "restored": previous, "result": result, "undo_available": bool(state.wled_undo_states)}
    except WLEDError as exc:
        state.wled_undo_states.insert(0, previous)
        raise HTTPException(status_code=502, detail=str(exc))


async def wled_discover(
    req: WledDiscoveryRequest, state: AppState = Depends(get_state)
) -> Dict[str, Any]:
    """Probe a bounded IPv4 subnet for WLED's read-only /json/info endpoint."""
    try:
        network = ipaddress.ip_network(req.subnet, strict=False)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid subnet: {exc}")
    hosts = list(network.hosts())
    if len(hosts) > req.max_hosts:
        raise HTTPException(status_code=400, detail=f"Subnet has {len(hosts)} hosts; max_hosts is {req.max_hosts}")
    client = state.peer_http
    if client is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")

    async def probe(address: ipaddress.IPv4Address) -> Dict[str, Any] | None:
        started = time.perf_counter()
        try:
            response = await client.get(f"http://{address}/json/info", timeout=req.timeout_s)
            if response.status_code != 200:
                return None
            body = response.json()
            if not isinstance(body, dict) or not (body.get("ver") or body.get("product")):
                return None
            return {"address": str(address), "info": body, "latency_ms": round((time.perf_counter() - started) * 1000, 1)}
        except Exception:
            return None

    found = await asyncio.gather(*(probe(address) for address in hosts))
    devices = [item for item in found if item is not None]
    return {"ok": True, "subnet": str(network), "scanned": len(hosts), "devices": devices}


async def wled_calibration(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    """Return the live controller facts needed to calibrate segments and geometry."""
    try:
        client = _client(state)
        info, current, segments = await asyncio.gather(
            client.get_info(), client.get_state(), client.get_segments(refresh=True)
        )
        return {
            "ok": True,
            "controller_url": state.settings.wled_tree_url,
            "info": info,
            "state": current,
            "segments": segments,
            "configured_segment_ids": _segment_ids(state),
            "max_brightness": int(state.settings.wled_max_bri),
        }
    except WLEDError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


async def wled_apply_calibration(
    req: WledCalibrationRequest, state: AppState = Depends(get_state)
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if req.segments is not None:
        payload["seg"] = req.segments
    if req.brightness is not None:
        payload["bri"] = req.brightness
    if not payload:
        raise HTTPException(status_code=400, detail="Provide segments or brightness")
    return await wled_apply_state(ApplyStateRequest(state=payload), state)

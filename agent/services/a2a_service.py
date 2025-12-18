from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Optional

from fastapi import Depends, HTTPException

from config.constants import APP_VERSION
from ddp_control import prepare_ddp_params
from models.requests import A2AInvokeRequest
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


A2AActionFn = Callable[[AppState, Dict[str, Any]], Awaitable[Dict[str, Any]]]


async def _a2a_pick_random_look_spec(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    looks = getattr(state, "looks", None)
    if looks is None:
        raise RuntimeError("Look service not initialized")
    pack, row = await asyncio.to_thread(
        looks.choose_random,
        theme=params.get("theme"),
        pack_file=params.get("pack_file"),
        seed=params.get("seed"),
    )
    return {"pack_file": pack, "look_spec": row}


async def _a2a_apply_look_spec(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    looks = getattr(state, "looks", None)
    if looks is None:
        raise RuntimeError("Look service not initialized")
    look_spec = (
        params.get("look_spec") or params.get("look") or params.get("state") or {}
    )
    if not isinstance(look_spec, dict):
        raise ValueError("look_spec must be an object")
    bri = params.get("brightness")
    if bri is None:
        bri = params.get("brightness_override")
    bri_i: Optional[int] = None
    if bri is not None:
        bri_i = min(state.settings.wled_max_bri, max(1, int(bri)))
    if state.wled_cooldown is not None:
        await state.wled_cooldown.wait()
    out = await asyncio.to_thread(
        looks.apply_look, look_spec, brightness_override=bri_i
    )
    return dict(out or {})


async def _a2a_apply_state(state: AppState, params: Dict[str, Any]) -> Dict[str, Any]:
    payload = params.get("state") or {}
    if not isinstance(payload, dict):
        raise ValueError("state must be an object")
    if "bri" in payload:
        payload["bri"] = min(state.settings.wled_max_bri, max(1, int(payload["bri"])))
    if state.wled_cooldown is not None:
        await state.wled_cooldown.wait()
    out = await state.wled.apply_state(payload, verbose=False)
    return {"result": out}


async def _a2a_start_ddp_pattern(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    ddp = getattr(state, "ddp", None)
    if ddp is None:
        raise RuntimeError("DDP streamer not initialized")

    merged = dict(params.get("params") or {})
    if params.get("direction") and "direction" not in merged:
        merged["direction"] = params.get("direction")
    if params.get("start_pos") and "start_pos" not in merged:
        merged["start_pos"] = params.get("start_pos")

    # Best-effort orientation support.
    ori = None
    try:
        from services.ddp_service import _get_orientation

        ori = await _get_orientation(state, refresh=False)
    except Exception:
        ori = None

    merged = prepare_ddp_params(
        pattern=str(params.get("pattern")),
        params=merged,
        orientation=ori,
        default_start_pos=str(state.settings.quad_default_start_pos),
    )

    st = await asyncio.to_thread(
        ddp.start,
        pattern=str(params.get("pattern")),
        params=merged,
        duration_s=float(params.get("duration_s", 30.0)),
        brightness=min(state.settings.wled_max_bri, int(params.get("brightness", 128))),
        fps=float(params.get("fps", state.settings.ddp_fps_default)),
    )
    return {"status": st.__dict__}


async def _a2a_stop_ddp(state: AppState, _: Dict[str, Any]) -> Dict[str, Any]:
    ddp = getattr(state, "ddp", None)
    if ddp is None:
        raise RuntimeError("DDP streamer not initialized")
    st = await asyncio.to_thread(ddp.stop)
    return {"status": st.__dict__}


async def _a2a_stop_all(state: AppState, _: Dict[str, Any]) -> Dict[str, Any]:
    # sequences.stop() best-effort stops DDP too.
    seq = getattr(state, "sequences", None)
    ddp = getattr(state, "ddp", None)
    fleet = getattr(state, "fleet_sequences", None)

    seq_st = None
    if seq is not None:
        try:
            seq_st = (await asyncio.to_thread(seq.stop)).__dict__
        except Exception:
            seq_st = None

    fleet_st = None
    if fleet is not None:
        try:
            fleet_st = (await asyncio.to_thread(fleet.stop)).__dict__
        except Exception:
            fleet_st = None

    ddp_st = None
    try:
        ddp_st = ddp.status().__dict__ if ddp is not None else None
    except Exception:
        ddp_st = None

    return {"sequence": seq_st, "fleet_sequence": fleet_st, "ddp": ddp_st}


async def _a2a_status(state: AppState, _: Dict[str, Any]) -> Dict[str, Any]:
    fleet_st = None
    try:
        fleet = getattr(state, "fleet_sequences", None)
        if fleet is not None:
            fleet_st = fleet.status().__dict__
    except Exception:
        fleet_st = None
    ddp_st = None
    try:
        ddp = getattr(state, "ddp", None)
        ddp_st = ddp.status().__dict__ if ddp is not None else None
    except Exception:
        ddp_st = None
    seq_st = None
    try:
        seq = getattr(state, "sequences", None)
        seq_st = seq.status().__dict__ if seq is not None else None
    except Exception:
        seq_st = None
    return {"sequence": seq_st, "fleet_sequence": fleet_st, "ddp": ddp_st}


_ACTIONS: Dict[str, A2AActionFn] = {
    "pick_random_look_spec": _a2a_pick_random_look_spec,
    "apply_look_spec": _a2a_apply_look_spec,
    "apply_state": _a2a_apply_state,
    "start_ddp_pattern": _a2a_start_ddp_pattern,
    "stop_ddp": _a2a_stop_ddp,
    "stop_all": _a2a_stop_all,
    "status": _a2a_status,
}

CAPABILITIES: List[Dict[str, Any]] = [
    {
        "action": "pick_random_look_spec",
        "description": "Choose a look spec from a local pack without applying it.",
        "params": {
            "theme": "optional string",
            "pack_file": "optional string",
            "seed": "optional int",
        },
    },
    {
        "action": "apply_look_spec",
        "description": "Apply a look spec (effect/palette by name) to this WLED device.",
        "params": {"look_spec": "object", "brightness_override": "optional int"},
    },
    {
        "action": "apply_state",
        "description": "Apply a raw WLED /json/state payload (brightness capped).",
        "params": {"state": "object"},
    },
    {
        "action": "start_ddp_pattern",
        "description": "Start a realtime DDP pattern for a duration.",
        "params": {
            "pattern": "string",
            "params": "object",
            "duration_s": "optional number",
            "brightness": "optional int",
            "fps": "optional number",
            "direction": "optional 'cw'|'ccw'",
            "start_pos": "optional 'front'|'right'|'back'|'left'",
        },
    },
    {"action": "stop_ddp", "description": "Stop any running DDP stream.", "params": {}},
    {"action": "stop_all", "description": "Stop sequences and DDP.", "params": {}},
    {"action": "status", "description": "Get sequence + DDP status.", "params": {}},
]


def actions() -> Dict[str, A2AActionFn]:
    return dict(_ACTIONS)


async def a2a_card(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    s = state.settings
    return {
        "ok": True,
        "agent": {
            "id": s.agent_id,
            "name": s.agent_name,
            "role": s.agent_role,
            "version": APP_VERSION,
            "endpoints": {"card": "/v1/a2a/card", "invoke": "/v1/a2a/invoke"},
            "wled": {
                "url": s.wled_tree_url,
                "segment_ids": list(state.segment_ids or []),
                "replicate_to_all_segments": s.wled_replicate_to_all_segments,
            },
            "capabilities": list(CAPABILITIES),
        },
    }


async def a2a_invoke(
    req: A2AInvokeRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    action = (req.action or "").strip()
    fn = _ACTIONS.get(action)
    if fn is None:
        return {
            "ok": False,
            "request_id": req.request_id,
            "error": f"Unknown action '{action}'",
        }
    try:
        res = await fn(state, dict(req.params or {}))
        return {
            "ok": True,
            "request_id": req.request_id,
            "action": action,
            "result": res,
        }
    except Exception as e:
        return {
            "ok": False,
            "request_id": req.request_id,
            "action": action,
            "error": str(e),
        }

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List, Optional

from fastapi import Depends, HTTPException, Request

from config.constants import APP_VERSION
from ddp_control import prepare_ddp_params
from models.requests import A2AInvokeRequest
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from fpp_client import AsyncFPPClient
from utils.outbound_http import retry_policy_from_settings


A2AActionFn = Callable[[AppState, Dict[str, Any]], Awaitable[Dict[str, Any]]]


async def _a2a_pick_random_look_spec(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    looks = getattr(state, "looks", None)
    if looks is None:
        raise RuntimeError("Look service not initialized")
    pack, row = await looks.choose_random(
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
    transition_ms = params.get("transition_ms")
    if transition_ms is None and params.get("transition_s") is not None:
        try:
            transition_ms = float(params.get("transition_s")) * 1000.0
        except Exception:
            transition_ms = None
    if state.wled_cooldown is not None:
        await state.wled_cooldown.wait()
    out = await looks.apply_look(
        look_spec, brightness_override=bri_i, transition_ms=transition_ms
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


def _ledfx_client(state: AppState):
    base_url = str(state.settings.ledfx_base_url or "").strip()
    if not base_url:
        raise RuntimeError("LedFx is not configured; set LEDFX_BASE_URL.")
    if state.peer_http is None:
        raise RuntimeError("HTTP client not initialized")
    from ledfx_client import AsyncLedFxClient
    from utils.outbound_http import retry_policy_from_settings

    return AsyncLedFxClient(
        base_url=base_url,
        client=state.peer_http,
        timeout_s=float(state.settings.ledfx_http_timeout_s),
        headers={k: v for (k, v) in state.settings.ledfx_headers},
        retry=retry_policy_from_settings(state.settings),
    )


async def _resolve_ledfx_virtual_id(
    state: AppState, virtual_id: str | None
) -> str:
    vid = str(virtual_id or "").strip()
    if vid:
        return vid
    client = _ledfx_client(state)
    resp = await client.virtuals()
    body = resp.body
    raw = None
    if isinstance(body, dict):
        raw = body.get("virtuals")
        if raw is None:
            data = body.get("data")
            if isinstance(data, dict):
                raw = data.get("virtuals")
    if raw is None:
        raw = body
    ids: list[str] = []
    if isinstance(raw, dict):
        ids = [str(k) for k in raw.keys()]
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                candidate = item.get("id") or item.get("name")
                if candidate:
                    ids.append(str(candidate))
            elif item:
                ids.append(str(item))
    ids = [x for x in ids if x.strip()]
    if len(ids) == 1:
        return ids[0]
    if not ids:
        raise ValueError("No LedFx virtuals found")
    raise ValueError("virtual_id is required when multiple virtuals exist")


async def _a2a_ledfx_activate_scene(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    scene_id = str(params.get("scene_id") or params.get("scene") or "").strip()
    if not scene_id:
        raise ValueError("scene_id is required")
    client = _ledfx_client(state)
    resp = await client.activate_scene(scene_id)
    try:
        from services.ledfx_service import _record_last_applied

        await _record_last_applied(
            state,
            kind="ledfx_scene",
            name=scene_id,
            file=None,
            payload={"action": "activate", "scene_id": scene_id},
        )
    except Exception:
        pass
    return {"ledfx": resp.as_dict()}


async def _a2a_ledfx_deactivate_scene(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    scene_id = str(params.get("scene_id") or params.get("scene") or "").strip()
    if not scene_id:
        raise ValueError("scene_id is required")
    client = _ledfx_client(state)
    resp = await client.deactivate_scene(scene_id)
    try:
        from services.ledfx_service import _record_last_applied

        await _record_last_applied(
            state,
            kind="ledfx_scene",
            name=scene_id,
            file=None,
            payload={"action": "deactivate", "scene_id": scene_id},
        )
    except Exception:
        pass
    return {"ledfx": resp.as_dict()}


async def _a2a_ledfx_virtual_effect(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    effect = str(params.get("effect") or "").strip()
    if not effect:
        raise ValueError("effect is required")
    virtual_id = await _resolve_ledfx_virtual_id(state, params.get("virtual_id"))
    config = params.get("config")
    cfg = dict(config) if isinstance(config, dict) else {}
    client = _ledfx_client(state)
    resp = await client.set_virtual_effect(
        virtual_id=virtual_id,
        effect=effect,
        config=cfg,
    )
    try:
        from services.ledfx_service import _record_last_applied

        await _record_last_applied(
            state,
            kind="ledfx_effect",
            name=effect,
            file=virtual_id,
            payload={"virtual_id": virtual_id, "effect": effect, "config": cfg},
        )
    except Exception:
        pass
    return {"ledfx": resp.as_dict()}


async def _a2a_ledfx_virtual_brightness(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    if params.get("brightness") is None:
        raise ValueError("brightness is required")
    try:
        raw = float(params.get("brightness"))
    except Exception:
        raise ValueError("brightness must be a number")
    virtual_id = await _resolve_ledfx_virtual_id(state, params.get("virtual_id"))
    primary = max(0.0, raw)
    fallback: float | None = None
    if primary > 1.0:
        raw_val = min(255.0, primary)
        primary = max(0.0, min(1.0, raw_val / 255.0))
        fallback = raw_val
    client = _ledfx_client(state)
    resp = await client.set_virtual_brightness(
        virtual_id=virtual_id,
        brightness=primary,
        fallback_brightness=fallback,
    )
    try:
        from services.ledfx_service import _record_last_applied

        await _record_last_applied(
            state,
            kind="ledfx_brightness",
            name=str(raw),
            file=virtual_id,
            payload={"virtual_id": virtual_id, "brightness": raw},
        )
    except Exception:
        pass
    return {"ledfx": resp.as_dict()}


async def _a2a_ledfx_status(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    enabled = bool(str(state.settings.ledfx_base_url or "").strip())
    out: Dict[str, Any] = {"ledfx_enabled": enabled, "health": False}
    if not enabled:
        return out
    try:
        resp = await _ledfx_client(state).status()
        out["health"] = True
        out["status"] = resp.as_dict()
    except Exception as e:
        out["status"] = {"ok": False, "error": str(e)}
    last_applied: Dict[str, Any] = {}
    db = getattr(state, "db", None)
    if db is not None:
        try:
            rows = await db.list_last_applied()
            for row in rows or []:
                kind = str(row.get("kind") or "").strip().lower()
                if not kind.startswith("ledfx"):
                    continue
                last_applied[kind] = {
                    "kind": kind,
                    "name": row.get("name"),
                    "file": row.get("file"),
                    "updated_at": row.get("updated_at"),
                    "payload": row.get("payload") or {},
                }
        except Exception:
            last_applied = {}
    if last_applied:
        out["last_applied"] = last_applied
        out["last_scene"] = last_applied.get("ledfx_scene")
        out["last_effect"] = last_applied.get("ledfx_effect")
        out["last_brightness"] = last_applied.get("ledfx_brightness")
    return out

async def _a2a_crossfade(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    look = params.get("look") or params.get("look_spec")
    raw_state = params.get("state")
    if look is None and raw_state is None:
        raise ValueError("look or state is required")

    transition_ms = params.get("transition_ms")
    if transition_ms is None and params.get("transition_s") is not None:
        try:
            transition_ms = float(params.get("transition_s")) * 1000.0
        except Exception:
            transition_ms = None

    brightness = params.get("brightness")
    bri_i: Optional[int] = None
    if brightness is not None:
        bri_i = min(state.settings.wled_max_bri, max(1, int(brightness)))

    if look is not None:
        looks = getattr(state, "looks", None)
        if looks is None:
            raise RuntimeError("Look service not initialized")
        if not isinstance(look, dict):
            raise ValueError("look must be an object")
        if state.wled_cooldown is not None:
            await state.wled_cooldown.wait()
        out = await looks.apply_look(
            dict(look),
            brightness_override=bri_i,
            transition_ms=transition_ms,
        )
        return dict(out or {})

    payload = raw_state or {}
    if not isinstance(payload, dict):
        raise ValueError("state must be an object")
    payload = dict(payload)
    if bri_i is not None and "bri" not in payload:
        payload["bri"] = int(bri_i)
    if "bri" in payload:
        payload["bri"] = min(state.settings.wled_max_bri, max(1, int(payload["bri"])))
    if transition_ms is not None:
        try:
            tt = max(0, int(round(float(transition_ms) / 100.0)))
            payload["tt"] = tt
            payload["transition"] = tt
        except Exception:
            pass
    if state.wled_cooldown is not None:
        await state.wled_cooldown.wait()
    out = await state.wled.apply_state(payload, verbose=False)
    return {"result": out}


async def _a2a_apply_preset(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    try:
        preset_id = int(params.get("preset_id"))
    except Exception:
        raise ValueError("preset_id must be an integer")
    payload: Dict[str, Any] = {"ps": preset_id}
    if params.get("brightness") is not None:
        payload["bri"] = min(
            state.settings.wled_max_bri, max(1, int(params.get("brightness")))
        )
    transition_ms = params.get("transition_ms")
    if transition_ms is None and params.get("transition_s") is not None:
        try:
            transition_ms = float(params.get("transition_s")) * 1000.0
        except Exception:
            transition_ms = None
    if transition_ms is not None:
        try:
            tt = max(0, int(round(float(transition_ms) / 100.0)))
            payload["tt"] = tt
            payload["transition"] = tt
        except Exception:
            pass
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

    st = await ddp.start(
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
    st = await ddp.stop()
    return {"status": st.__dict__}


async def _a2a_stop_all(state: AppState, _: Dict[str, Any]) -> Dict[str, Any]:
    # sequences.stop() best-effort stops DDP too.
    seq = getattr(state, "sequences", None)
    ddp = getattr(state, "ddp", None)
    fleet = getattr(state, "fleet_sequences", None)

    seq_st = None
    if seq is not None:
        try:
            seq_st = (await seq.stop()).__dict__
        except Exception:
            seq_st = None

    fleet_st = None
    if fleet is not None:
        try:
            fleet_st = (await fleet.stop()).__dict__
        except Exception:
            fleet_st = None

    ddp_st = None
    try:
        ddp_st = (await ddp.status()).__dict__ if ddp is not None else None
    except Exception:
        ddp_st = None

    return {"sequence": seq_st, "fleet_sequence": fleet_st, "ddp": ddp_st}


async def _a2a_start_sequence(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    seq = getattr(state, "sequences", None)
    if seq is None:
        raise RuntimeError("Sequence service not initialized")
    file = str(params.get("file") or params.get("sequence") or "").strip()
    if not file:
        raise ValueError("file is required")
    loop = bool(params.get("loop", False))
    st = await seq.play(file=file, loop=loop)
    try:
        from services.runtime_state_service import persist_runtime_state

        await persist_runtime_state(
            state, "sequences_play", {"file": file, "loop": bool(loop)}
        )
    except Exception:
        pass
    if state.db is not None:
        try:
            await state.db.set_last_applied(
                kind="sequence",
                name=str(file),
                file=str(file),
                payload={"file": str(file), "loop": bool(loop)},
            )
            try:
                from services.events_service import emit_event

                await emit_event(
                    state,
                    event_type="meta",
                    data={
                        "event": "last_applied",
                        "kind": "sequence",
                        "name": str(file),
                        "file": str(file),
                        "loop": bool(loop),
                    },
                )
            except Exception:
                pass
        except Exception:
            pass
    return {"status": st.__dict__}


async def _a2a_stop_sequence(state: AppState, _: Dict[str, Any]) -> Dict[str, Any]:
    seq = getattr(state, "sequences", None)
    if seq is None:
        raise RuntimeError("Sequence service not initialized")
    st = await seq.stop()
    try:
        from services.runtime_state_service import persist_runtime_state

        await persist_runtime_state(state, "sequences_stop")
    except Exception:
        pass
    return {"status": st.__dict__}


async def _a2a_status(state: AppState, _: Dict[str, Any]) -> Dict[str, Any]:
    fleet_st = None
    try:
        fleet = getattr(state, "fleet_sequences", None)
        if fleet is not None:
            fleet_st = (await fleet.status()).__dict__
    except Exception:
        fleet_st = None
    ddp_st = None
    try:
        ddp = getattr(state, "ddp", None)
        ddp_st = (await ddp.status()).__dict__ if ddp is not None else None
    except Exception:
        ddp_st = None
    seq_st = None
    try:
        seq = getattr(state, "sequences", None)
        seq_st = (await seq.status()).__dict__ if seq is not None else None
    except Exception:
        seq_st = None
    return {"sequence": seq_st, "fleet_sequence": fleet_st, "ddp": ddp_st}


async def _a2a_health_status(
    state: AppState, params: Dict[str, Any]
) -> Dict[str, Any]:
    s = state.settings
    out: Dict[str, Any] = {
        "agent_id": s.agent_id,
        "name": s.agent_name,
        "role": s.agent_role,
        "controller_kind": s.controller_kind,
        "version": APP_VERSION,
    }
    if s.agent_base_url:
        out["base_url"] = str(s.agent_base_url)
    if s.agent_tags:
        out["tags"] = [str(t) for t in s.agent_tags]

    # WLED status (optional).
    wled: Dict[str, Any] = {"ok": False, "enabled": bool(s.wled_tree_url)}
    if s.wled_tree_url:
        try:
            info = await state.wled.get_info()
            st = await state.wled.get_state()
            wled.update(
                {
                    "ok": True,
                    "name": info.get("name"),
                    "version": info.get("ver"),
                    "bri": st.get("bri"),
                    "on": st.get("on"),
                    "preset": st.get("ps"),
                    "live": st.get("live"),
                }
            )
        except Exception as e:
            wled["error"] = str(e)
    out["wled"] = wled

    # FPP status (optional).
    fpp: Dict[str, Any] = {"ok": False, "enabled": bool(s.fpp_base_url)}
    if s.fpp_base_url:
        try:
            client = AsyncFPPClient(
                base_url=str(s.fpp_base_url),
                client=state.peer_http,
                timeout_s=float(s.fpp_http_timeout_s),
                headers={k: v for (k, v) in (s.fpp_headers or [])},
                retry=retry_policy_from_settings(s),
            )
            resp = await client.status()
            body = resp.body if isinstance(resp.body, dict) else {}
            status = None
            playlist = None
            if isinstance(body, dict):
                status = (
                    body.get("status")
                    or body.get("status_name")
                    or body.get("state")
                    or body.get("fppd")
                )
                playlist = (
                    body.get("currentPlaylist")
                    or body.get("current_playlist")
                    or body.get("playlist")
                )
            fpp.update(
                {
                    "ok": True,
                    "status": status,
                    "playlist": playlist,
                    "status_code": resp.status_code,
                }
            )
        except Exception as e:
            fpp["error"] = str(e)
    out["fpp"] = fpp

    # LedFx summary (optional).
    try:
        out["ledfx"] = await _a2a_ledfx_status(state, params)
    except Exception as e:
        out["ledfx"] = {
            "ledfx_enabled": bool(s.ledfx_base_url),
            "health": False,
            "error": str(e),
        }

    include_last = params.get("include_last_applied", True)
    if include_last:
        db = getattr(state, "db", None)
        if db is not None:
            try:
                rows = await db.list_last_applied()
                summary: Dict[str, Any] = {}
                for row in rows or []:
                    kind = str(row.get("kind") or "").strip().lower()
                    if not kind or kind.startswith("ledfx"):
                        continue
                    summary[kind] = {
                        "kind": kind,
                        "name": row.get("name"),
                        "file": row.get("file"),
                        "updated_at": row.get("updated_at"),
                    }
                if summary:
                    out["last_applied"] = summary
            except Exception:
                pass

    return out


_ACTIONS: Dict[str, A2AActionFn] = {
    "pick_random_look_spec": _a2a_pick_random_look_spec,
    "apply_look_spec": _a2a_apply_look_spec,
    "apply_state": _a2a_apply_state,
    "crossfade": _a2a_crossfade,
    "apply_preset": _a2a_apply_preset,
    "ledfx_activate_scene": _a2a_ledfx_activate_scene,
    "ledfx_deactivate_scene": _a2a_ledfx_deactivate_scene,
    "ledfx_virtual_effect": _a2a_ledfx_virtual_effect,
    "ledfx_virtual_brightness": _a2a_ledfx_virtual_brightness,
    "ledfx_status": _a2a_ledfx_status,
    "health_status": _a2a_health_status,
    "start_ddp_pattern": _a2a_start_ddp_pattern,
    "stop_ddp": _a2a_stop_ddp,
    "start_sequence": _a2a_start_sequence,
    "stop_sequence": _a2a_stop_sequence,
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
        "params": {
            "look_spec": "object",
            "brightness_override": "optional int",
            "transition_ms": "optional int",
        },
    },
    {
        "action": "apply_state",
        "description": "Apply a raw WLED /json/state payload (brightness capped).",
        "params": {"state": "object"},
    },
    {
        "action": "crossfade",
        "description": "Apply a look or raw state with a transition (crossfade).",
        "params": {
            "look": "optional object",
            "state": "optional object",
            "brightness": "optional int",
            "transition_ms": "optional int",
        },
    },
    {
        "action": "apply_preset",
        "description": "Apply a WLED preset by ID (optional brightness/transition).",
        "params": {
            "preset_id": "int",
            "brightness": "optional int",
            "transition_ms": "optional int",
        },
    },
    {
        "action": "ledfx_activate_scene",
        "description": "Activate a LedFx scene by id or name.",
        "params": {"scene_id": "string"},
    },
    {
        "action": "ledfx_deactivate_scene",
        "description": "Deactivate a LedFx scene by id or name.",
        "params": {"scene_id": "string"},
    },
    {
        "action": "ledfx_virtual_effect",
        "description": "Set the active effect for a LedFx virtual.",
        "params": {
            "virtual_id": "optional string",
            "effect": "string",
            "config": "optional object",
        },
    },
    {
        "action": "ledfx_virtual_brightness",
        "description": "Set brightness for a LedFx virtual (0..1 or 0..255).",
        "params": {"virtual_id": "optional string", "brightness": "number"},
    },
    {
        "action": "ledfx_status",
        "description": "Get LedFx health + last applied scene/effect metadata.",
        "params": {},
    },
    {
        "action": "health_status",
        "description": "Get compact WLED/FPP/LedFx health summary for this agent.",
        "params": {"include_last_applied": "optional bool"},
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
    {
        "action": "start_sequence",
        "description": "Start a local sequence JSON file.",
        "params": {"file": "string", "loop": "optional bool"},
    },
    {"action": "stop_sequence", "description": "Stop the running sequence.", "params": {}},
    {"action": "stop_all", "description": "Stop sequences and DDP.", "params": {}},
    {"action": "status", "description": "Get sequence + DDP status.", "params": {}},
]


def actions() -> Dict[str, A2AActionFn]:
    return dict(_ACTIONS)


async def a2a_card(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    s = state.settings
    await log_event(state, action="a2a.card", ok=True, request=request)
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
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    action = (req.action or "").strip()
    fn = _ACTIONS.get(action)
    if fn is None:
        await log_event(
            state,
            action="a2a.invoke",
            ok=False,
            error=f"Unknown action '{action}'",
            payload={"action": action},
            request=request,
        )
        return {
            "ok": False,
            "request_id": req.request_id,
            "error": f"Unknown action '{action}'",
        }
    try:
        res = await fn(state, dict(req.params or {}))
        await log_event(
            state,
            action="a2a.invoke",
            ok=True,
            payload={"action": action},
            request=request,
        )
        return {
            "ok": True,
            "request_id": req.request_id,
            "action": action,
            "result": res,
        }
    except Exception as e:
        await log_event(
            state,
            action="a2a.invoke",
            ok=False,
            error=str(e),
            payload={"action": action},
            request=request,
        )
        return {
            "ok": False,
            "request_id": req.request_id,
            "action": action,
            "error": str(e),
        }

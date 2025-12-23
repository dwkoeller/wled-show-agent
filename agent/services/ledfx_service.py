from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List

from fastapi import Depends, HTTPException, Request

from ledfx_client import AsyncLedFxClient, LedFxError
from models.requests import (
    LedFxProxyRequest,
    LedFxSceneActivateRequest,
    LedFxSceneDeactivateRequest,
    LedFxVirtualBrightnessRequest,
    LedFxVirtualEffectRequest,
)
from services import a2a_service, fleet_service
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from utils.outbound_http import retry_policy_from_settings


_FLEET_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": None, "include_self": True}
_FLEET_LOCK = asyncio.Lock()


def _client(state: AppState) -> AsyncLedFxClient:
    base_url = str(state.settings.ledfx_base_url or "").strip()
    if not base_url:
        raise HTTPException(
            status_code=400,
            detail="LedFx integration not configured; set LEDFX_BASE_URL.",
        )
    http = state.peer_http
    if http is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")
    return AsyncLedFxClient(
        base_url=base_url,
        client=http,
        timeout_s=float(state.settings.ledfx_http_timeout_s),
        headers={k: v for (k, v) in state.settings.ledfx_headers},
        retry=retry_policy_from_settings(state.settings),
    )


def _coerce_items(source: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    if isinstance(source, dict):
        for key, raw in source.items():
            if isinstance(raw, dict):
                item = dict(raw)
                item.setdefault("id", str(key))
                if "name" not in item:
                    item["name"] = str(item.get("id") or key)
            else:
                item = {"id": str(key), "name": str(raw or key)}
            items.append(item)
    elif isinstance(source, list):
        for raw in source:
            if isinstance(raw, dict):
                item = dict(raw)
                if "id" not in item and item.get("name"):
                    item["id"] = str(item.get("name"))
                if "name" not in item and item.get("id"):
                    item["name"] = str(item.get("id"))
            else:
                item = {"id": str(raw), "name": str(raw)}
            items.append(item)
    elif source is not None:
        items.append({"id": str(source), "name": str(source)})
    return items


def _extract_items(body: Any, key: str) -> List[Dict[str, Any]]:
    source: Any = None
    if isinstance(body, dict):
        if key in body:
            source = body.get(key)
        elif isinstance(body.get("data"), dict) and key in body["data"]:
            source = body["data"].get(key)
        elif isinstance(body.get("result"), dict) and key in body["result"]:
            source = body["result"].get(key)
    elif body is not None:
        source = body
    items = _coerce_items(source)
    items.sort(key=lambda x: str(x.get("name") or x.get("id") or ""))
    return items


def _extract_effects(body: Any) -> List[Dict[str, Any]]:
    if isinstance(body, dict):
        if "effects" in body:
            return _coerce_items(body.get("effects"))
        data = body.get("data")
        if isinstance(data, dict) and "effects" in data:
            return _coerce_items(data.get("effects"))
        result = body.get("result")
        if isinstance(result, dict) and "effects" in result:
            return _coerce_items(result.get("effects"))
    return _coerce_items(body)


async def _resolve_virtual_id(state: AppState, virtual_id: str | None) -> str:
    if virtual_id is not None:
        vid = str(virtual_id).strip()
        if not vid:
            raise HTTPException(status_code=400, detail="virtual_id is required")
        return vid
    resp = await _client(state).virtuals()
    items = _extract_items(resp.body, "virtuals")
    ids = [
        str(item.get("id") or item.get("name") or "").strip()
        for item in items
        if isinstance(item, dict)
    ]
    ids = [x for x in ids if x]
    if len(ids) == 1:
        return ids[0]
    if not ids:
        raise HTTPException(status_code=400, detail="No virtuals found in LedFx")
    raise HTTPException(
        status_code=400,
        detail=f"virtual_id is required; available: {', '.join(ids[:8])}",
    )


def _brightness_values(value: float) -> tuple[float, float | None]:
    raw = max(0.0, float(value))
    if raw <= 1.0:
        return raw, None
    fallback = min(255.0, raw)
    return max(0.0, min(1.0, fallback / 255.0)), fallback


def _normalize_proxy_path(path: str) -> str:
    p = str(path or "").strip()
    if not p:
        raise HTTPException(status_code=400, detail="path is required")
    if not p.startswith("/"):
        p = "/" + p
    if not p.startswith("/api"):
        raise HTTPException(status_code=400, detail="path must start with /api")
    return p


async def _record_last_applied(
    state: AppState,
    *,
    kind: str,
    name: str | None,
    file: str | None,
    payload: Dict[str, Any] | None = None,
) -> None:
    db = getattr(state, "db", None)
    if db is not None:
        try:
            await db.set_last_applied(
                kind=str(kind),
                name=str(name) if name else None,
                file=str(file) if file else None,
                payload=dict(payload or {}),
            )
        except Exception:
            pass
    try:
        from services.events_service import emit_event

        await emit_event(
            state,
            event_type="meta",
            data={
                "event": "last_applied",
                "kind": str(kind),
                "name": str(name) if name else None,
                "file": str(file) if file else None,
            },
        )
    except Exception:
        return


async def ledfx_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        res = {"ok": True, "ledfx": (await _client(state).status()).as_dict()}
        await log_event(state, action="ledfx.status", ok=True, request=request)
        return res
    except LedFxError as e:
        await log_event(
            state, action="ledfx.status", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def ledfx_virtuals(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).virtuals()
        items = _extract_items(resp.body, "virtuals")
        res = {"ok": True, "virtuals": items, "ledfx": resp.as_dict()}
        await log_event(
            state,
            action="ledfx.virtuals",
            ok=True,
            payload={"count": len(items)},
            request=request,
        )
        return res
    except LedFxError as e:
        await log_event(
            state, action="ledfx.virtuals", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def ledfx_scenes(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).scenes()
        items = _extract_items(resp.body, "scenes")
        res = {"ok": True, "scenes": items, "ledfx": resp.as_dict()}
        await log_event(
            state,
            action="ledfx.scenes",
            ok=True,
            payload={"count": len(items)},
            request=request,
        )
        return res
    except LedFxError as e:
        await log_event(
            state, action="ledfx.scenes", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def ledfx_effects(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).effects()
        items = _extract_effects(resp.body)
        items.sort(key=lambda x: str(x.get("name") or x.get("id") or ""))
        res = {"ok": True, "effects": items, "ledfx": resp.as_dict()}
        await log_event(
            state,
            action="ledfx.effects",
            ok=True,
            payload={"count": len(items)},
            request=request,
        )
        return res
    except LedFxError as e:
        await log_event(
            state, action="ledfx.effects", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def ledfx_scene_activate(
    req: LedFxSceneActivateRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).activate_scene(req.scene_id)
        await _record_last_applied(
            state,
            kind="ledfx_scene",
            name=str(req.scene_id),
            file=None,
            payload={
                "action": "activate",
                "scene_id": str(req.scene_id),
                "ledfx": resp.as_dict(),
            },
        )
        await log_event(
            state,
            action="ledfx.scene.activate",
            ok=True,
            resource=str(req.scene_id),
            request=request,
        )
        return {"ok": True, "ledfx": resp.as_dict()}
    except Exception as e:
        await log_event(
            state,
            action="ledfx.scene.activate",
            ok=False,
            resource=str(req.scene_id),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def ledfx_scene_deactivate(
    req: LedFxSceneDeactivateRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).deactivate_scene(req.scene_id)
        await _record_last_applied(
            state,
            kind="ledfx_scene",
            name=str(req.scene_id),
            file=None,
            payload={
                "action": "deactivate",
                "scene_id": str(req.scene_id),
                "ledfx": resp.as_dict(),
            },
        )
        await log_event(
            state,
            action="ledfx.scene.deactivate",
            ok=True,
            resource=str(req.scene_id),
            request=request,
        )
        return {"ok": True, "ledfx": resp.as_dict()}
    except Exception as e:
        await log_event(
            state,
            action="ledfx.scene.deactivate",
            ok=False,
            resource=str(req.scene_id),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def ledfx_virtual_effect(
    req: LedFxVirtualEffectRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        vid = await _resolve_virtual_id(state, req.virtual_id)
        resp = await _client(state).set_virtual_effect(
            virtual_id=vid,
            effect=req.effect,
            config=req.config,
        )
        await _record_last_applied(
            state,
            kind="ledfx_effect",
            name=str(req.effect),
            file=str(vid),
            payload={
                "virtual_id": str(vid),
                "effect": str(req.effect),
                "config": dict(req.config or {}),
                "ledfx": resp.as_dict(),
            },
        )
        await log_event(
            state,
            action="ledfx.virtual.effect",
            ok=True,
            resource=vid,
            payload={"effect": req.effect},
            request=request,
        )
        return {"ok": True, "ledfx": resp.as_dict()}
    except Exception as e:
        await log_event(
            state,
            action="ledfx.virtual.effect",
            ok=False,
            resource=str(req.virtual_id or ""),
            error=str(e),
            payload={"effect": req.effect},
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def ledfx_virtual_brightness(
    req: LedFxVirtualBrightnessRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        vid = await _resolve_virtual_id(state, req.virtual_id)
        primary, fallback = _brightness_values(req.brightness)
        resp = await _client(state).set_virtual_brightness(
            virtual_id=vid,
            brightness=primary,
            fallback_brightness=fallback,
        )
        await _record_last_applied(
            state,
            kind="ledfx_brightness",
            name=str(req.brightness),
            file=str(vid),
            payload={
                "virtual_id": str(vid),
                "brightness": float(req.brightness),
                "ledfx": resp.as_dict(),
            },
        )
        await log_event(
            state,
            action="ledfx.virtual.brightness",
            ok=True,
            resource=vid,
            payload={"brightness": req.brightness},
            request=request,
        )
        return {"ok": True, "ledfx": resp.as_dict()}
    except Exception as e:
        await log_event(
            state,
            action="ledfx.virtual.brightness",
            ok=False,
            resource=str(req.virtual_id or ""),
            error=str(e),
            payload={"brightness": req.brightness},
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def ledfx_proxy(
    req: LedFxProxyRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    method = (req.method or "GET").strip().upper()
    if method not in ("GET", "POST", "PUT", "DELETE", "PATCH"):
        raise HTTPException(
            status_code=400,
            detail="Unsupported method; use GET/POST/PUT/DELETE/PATCH.",
        )
    path = _normalize_proxy_path(req.path)
    try:
        resp = await _client(state).request(
            method, path, params=dict(req.params or {}), json_body=req.json_body
        )
        await log_event(
            state,
            action="ledfx.proxy",
            ok=True,
            resource=str(path or ""),
            payload={"method": method},
            request=request,
        )
        return {"ok": True, "ledfx": resp.as_dict()}
    except LedFxError as e:
        await log_event(
            state,
            action="ledfx.proxy",
            ok=False,
            resource=str(path or ""),
            error=str(e),
            payload={"method": method},
            request=request,
        )
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        await log_event(
            state,
            action="ledfx.proxy",
            ok=False,
            resource=str(path or ""),
            error=str(e),
            payload={"method": method},
            request=request,
        )
        raise


def _summarize_ledfx_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    last_applied = raw.get("last_applied") if isinstance(raw, dict) else {}
    return {
        "health": bool(raw.get("health")) if isinstance(raw, dict) else False,
        "ledfx_enabled": bool(raw.get("ledfx_enabled"))
        if isinstance(raw, dict)
        else False,
        "last_scene": last_applied.get("ledfx_scene") if isinstance(last_applied, dict) else None,
        "last_effect": last_applied.get("ledfx_effect") if isinstance(last_applied, dict) else None,
        "last_brightness": last_applied.get("ledfx_brightness")
        if isinstance(last_applied, dict)
        else None,
        "status": raw.get("status") if isinstance(raw, dict) else None,
    }


async def ledfx_fleet_summary(
    request: Request,
    include_self: bool = True,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    ttl_s = float(getattr(state.settings, "ledfx_fleet_cache_ttl_s", 15.0))
    now = time.time()
    async with _FLEET_LOCK:
        cached = _FLEET_CACHE.get("payload")
        ts = float(_FLEET_CACHE.get("ts") or 0.0)
        cached_include_self = bool(_FLEET_CACHE.get("include_self", True))
        if (
            cached is not None
            and ttl_s > 0
            and now - ts < ttl_s
            and cached_include_self == bool(include_self)
        ):
            payload = dict(cached)
            payload["cached"] = True
            return payload

    timeout_s = float(state.settings.a2a_http_timeout_s)
    peer_targets = list((state.peers or {}).keys())
    if "*" not in peer_targets:
        peer_targets.append("*")
    peers = await fleet_service._select_peers(
        state, peer_targets if peer_targets else ["*"]
    )
    results: Dict[str, Any] = {}

    if include_self:
        fn = a2a_service.actions().get("ledfx_status")
        if fn is None:
            results["self"] = {"ok": False, "error": "ledfx_status action unavailable"}
        else:
            try:
                res = await fn(state, {})
                results["self"] = {"ok": True, "result": res}
            except Exception as e:
                results["self"] = {"ok": False, "error": str(e)}

    if peers:
        payload = {"action": "ledfx_status", "params": {}}
        sem = asyncio.Semaphore(min(8, len(peers)))

        async def _call(peer: Any) -> None:
            async with sem:
                out = await fleet_service._peer_post_json(
                    state=state,
                    peer=peer,
                    path="/v1/a2a/invoke",
                    payload=payload,
                    timeout_s=timeout_s,
                )
                key = str(getattr(peer, "name", "") or getattr(peer, "base_url", ""))
                results[key] = out

        await asyncio.gather(*[_call(p) for p in peers])

    summary: Dict[str, Any] = {
        "agents": {},
        "total": 0,
        "healthy": 0,
        "enabled": 0,
    }

    for key, value in results.items():
        entry: Dict[str, Any] = {"ok": False}
        if isinstance(value, dict) and value.get("ok") is True:
            raw = value.get("result") if isinstance(value.get("result"), dict) else {}
            entry = {"ok": True}
            entry.update(_summarize_ledfx_payload(raw))
        elif isinstance(value, dict):
            entry["error"] = value.get("error")
        summary["agents"][key] = entry
        summary["total"] += 1
        if entry.get("ledfx_enabled"):
            summary["enabled"] += 1
        if entry.get("health"):
            summary["healthy"] += 1

    payload = {
        "ok": True,
        "cached": False,
        "generated_at": now,
        "ttl_s": ttl_s,
        "summary": summary,
    }

    async with _FLEET_LOCK:
        _FLEET_CACHE["ts"] = now
        _FLEET_CACHE["payload"] = payload
        _FLEET_CACHE["include_self"] = bool(include_self)

    await log_event(
        state,
        action="ledfx.fleet",
        ok=True,
        payload={"agents": summary.get("total", 0)},
        request=request,
    )
    return payload

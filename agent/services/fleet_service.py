from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import httpx
from fastapi import Depends
from fastapi.concurrency import run_in_threadpool

from models.requests import (
    FleetApplyRandomLookRequest,
    FleetInvokeRequest,
    FleetStopAllRequest,
)
from services import app_state as legacy
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _peer_headers(state: AppState) -> Dict[str, str]:
    key = state.settings.a2a_api_key
    return {"X-A2A-Key": str(key)} if key else {}


async def _peer_get_json(
    *,
    state: AppState,
    peer: Any,
    path: str,
    timeout_s: float,
) -> Dict[str, Any]:
    base_url = str(getattr(peer, "base_url", "") or "").rstrip("/")
    url = base_url + path
    client = state.peer_http
    if client is None:
        return {"ok": False, "error": "peer_http is not initialized"}
    try:
        resp = await client.get(
            url,
            headers=_peer_headers(state),
            timeout=float(timeout_s),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


async def _peer_post_json(
    *,
    state: AppState,
    peer: Any,
    path: str,
    payload: Dict[str, Any],
    timeout_s: float,
) -> Dict[str, Any]:
    base_url = str(getattr(peer, "base_url", "") or "").rstrip("/")
    url = base_url + path
    client = state.peer_http
    if client is None:
        return {"ok": False, "error": "peer_http is not initialized"}
    try:
        resp = await client.post(
            url,
            json=payload,
            headers=_peer_headers(state),
            timeout=float(timeout_s),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}

    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


async def _peer_supported_actions(
    *, state: AppState, peer: Any, timeout_s: float
) -> set[str]:
    card = await _peer_get_json(
        state=state, peer=peer, path="/v1/a2a/card", timeout_s=timeout_s
    )
    if not isinstance(card, dict) or card.get("ok") is not True:
        return set()
    agent = card.get("agent") or {}
    caps = agent.get("capabilities") or []
    actions: set[str] = set()
    if isinstance(caps, list):
        for c in caps:
            if isinstance(c, dict) and "action" in c:
                actions.add(str(c.get("action")))
            elif isinstance(c, str):
                actions.add(c)
    return actions


def _select_peers(state: AppState, targets: Optional[List[str]]) -> List[Any]:
    peers = state.peers or {}
    if not targets:
        return list(peers.values())
    out: List[Any] = []
    for t in targets:
        if t in peers:
            out.append(peers[t])
    return out


async def fleet_peers(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    peers = state.peers or {}
    s = state.settings
    return {
        "ok": True,
        "self": {"id": s.agent_id, "name": s.agent_name, "role": s.agent_role},
        "peers": [
            {"name": getattr(p, "name", ""), "base_url": getattr(p, "base_url", "")}
            for p in peers.values()
        ],
    }


async def fleet_invoke(
    req: FleetInvokeRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    action = (req.action or "").strip()
    timeout_s = (
        float(req.timeout_s)
        if req.timeout_s is not None
        else float(state.settings.a2a_http_timeout_s)
    )
    peers = _select_peers(state, req.targets)

    results: Dict[str, Any] = {}

    if req.include_self:
        fn = legacy._A2A_ACTIONS.get(action)  # type: ignore[attr-defined]
        if fn is None:
            results["self"] = {"ok": False, "error": f"Unknown action '{action}'"}
        else:
            try:
                res = await run_in_threadpool(fn, dict(req.params or {}))
                results["self"] = {"ok": True, "result": res}
            except Exception as e:
                results["self"] = {"ok": False, "error": str(e)}

    if peers:
        payload = {"action": action, "params": dict(req.params or {})}
        sem = asyncio.Semaphore(min(8, len(peers)))

        async def _call(peer: Any) -> None:
            async with sem:
                out = await _peer_post_json(
                    state=state,
                    peer=peer,
                    path="/v1/a2a/invoke",
                    payload=payload,
                    timeout_s=timeout_s,
                )
                results[str(getattr(peer, "name", ""))] = out

        await asyncio.gather(*[_call(p) for p in peers])

    return {"ok": True, "action": action, "results": results}


async def fleet_apply_random_look(
    req: FleetApplyRandomLookRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    # Pick on this agent, then broadcast the same look_spec to peers so devices match.
    pack, row = await run_in_threadpool(
        legacy.LOOKS.choose_random,  # type: ignore[union-attr]
        theme=req.theme,
        pack_file=req.pack_file,
        seed=req.seed,
    )
    bri = (
        min(state.settings.wled_max_bri, req.brightness)
        if req.brightness is not None
        else None
    )

    results: Dict[str, Any] = {
        "pack_file": pack,
        "picked": {
            "id": row.get("id"),
            "name": row.get("name"),
            "theme": row.get("theme"),
        },
    }

    peers = _select_peers(state, req.targets)
    timeout_s = float(state.settings.a2a_http_timeout_s)

    if req.include_self:
        try:
            await run_in_threadpool(legacy.COOLDOWN.wait)  # type: ignore[union-attr]
            res = await run_in_threadpool(
                legacy.LOOKS.apply_look,  # type: ignore[union-attr]
                row,
                brightness_override=bri,
            )
            results["self"] = {"ok": True, "result": res}
        except Exception as e:
            results["self"] = {"ok": False, "error": str(e)}

    if state.db is not None:
        try:
            await state.db.set_last_applied(
                kind="look",
                name=str(row.get("name") or "") or None,
                file=str(pack) if pack else None,
                payload={
                    "look": dict(row or {}),
                    "pack_file": str(pack) if pack else None,
                    "brightness_override": bri,
                    "scope": "fleet",
                },
            )
        except Exception:
            pass

    if peers:
        # Cache capabilities in parallel.
        caps = await asyncio.gather(
            *[
                _peer_supported_actions(state=state, peer=p, timeout_s=timeout_s)
                for p in peers
            ]
        )
        eligible: List[Any] = []
        for peer, actions in zip(peers, caps):
            if "apply_look_spec" in actions:
                eligible.append(peer)
            else:
                results[str(getattr(peer, "name", ""))] = {
                    "ok": False,
                    "skipped": True,
                    "reason": "Peer does not support apply_look_spec",
                }

        if eligible:
            payload = {
                "action": "apply_look_spec",
                "params": {"look_spec": row, "brightness_override": bri},
            }
            sem = asyncio.Semaphore(min(8, len(eligible)))

            async def _call(peer: Any) -> None:
                async with sem:
                    out = await _peer_post_json(
                        state=state,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=timeout_s,
                    )
                    results[str(getattr(peer, "name", ""))] = out

            await asyncio.gather(*[_call(p) for p in eligible])

    return {"ok": True, "result": results}


async def fleet_stop_all(
    req: FleetStopAllRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    timeout_s = (
        float(req.timeout_s)
        if req.timeout_s is not None
        else float(state.settings.a2a_http_timeout_s)
    )
    peers = _select_peers(state, req.targets)
    results: Dict[str, Any] = {}

    if req.include_self:
        try:
            res = await run_in_threadpool(legacy._a2a_action_stop_all, {})  # type: ignore[attr-defined]
            results["self"] = {"ok": True, "result": res}
        except Exception as e:
            results["self"] = {"ok": False, "error": str(e)}

    if peers:
        payload = {"action": "stop_all", "params": {}}
        sem = asyncio.Semaphore(min(8, len(peers)))

        async def _call(peer: Any) -> None:
            async with sem:
                out = await _peer_post_json(
                    state=state,
                    peer=peer,
                    path="/v1/a2a/invoke",
                    payload=payload,
                    timeout_s=timeout_s,
                )
                results[str(getattr(peer, "name", ""))] = out

        await asyncio.gather(*[_call(p) for p in peers])

    await run_in_threadpool(legacy._persist_runtime_state, "fleet_stop_all", {"targets": req.targets})  # type: ignore[attr-defined]
    return {"ok": True, "action": "stop_all", "results": results}

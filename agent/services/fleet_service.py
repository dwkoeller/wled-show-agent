from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

from fastapi import Depends

from models.requests import (
    FleetApplyRandomLookRequest,
    FleetInvokeRequest,
    FleetStopAllRequest,
)
from services import a2a_service
from services.auth_service import require_a2a_auth
from services.runtime_state_service import persist_runtime_state
from services.state import AppState, get_state
from utils.outbound_http import request_with_retry


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
        resp = await request_with_retry(
            client=client,
            method="GET",
            url=url,
            target_kind="peer",
            target=str(getattr(peer, "name", "") or base_url),
            timeout_s=float(timeout_s),
            headers=_peer_headers(state),
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
        resp = await request_with_retry(
            client=client,
            method="POST",
            url=url,
            target_kind="peer",
            target=str(getattr(peer, "name", "") or base_url),
            timeout_s=float(timeout_s),
            headers=_peer_headers(state),
            json_body=payload,
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


async def fleet_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    stale_after_s: float = 30.0,
    limit: int = 200,
    include_payload: bool = False,
) -> Dict[str, Any]:
    """
    Fleet status derived from SQL heartbeats (no fanout).
    """
    db = getattr(state, "db", None)
    if db is None:
        return {"ok": False, "error": "Database not initialized"}

    now = time.time()
    stale = max(1.0, float(stale_after_s))

    rows = await db.list_agent_heartbeats(limit=max(1, int(limit)))
    by_id: Dict[str, Dict[str, Any]] = {str(r.get("agent_id")): dict(r) for r in rows}

    peers = state.peers or {}
    configured_ids = {str(state.settings.agent_id)} | {str(k) for k in peers.keys()}

    def _format(
        aid: str, rec: Dict[str, Any] | None, *, configured: bool
    ) -> Dict[str, Any]:
        if not rec:
            return {
                "agent_id": aid,
                "configured": configured,
                "online": False,
                "age_s": None,
                "updated_at": None,
                "started_at": None,
                "name": None,
                "role": None,
                "controller_kind": None,
                "version": None,
            }
        updated_at = float(rec.get("updated_at") or 0.0)
        age_s = max(0.0, now - updated_at) if updated_at else None
        payload = dict(rec.get("payload") or {})
        out: Dict[str, Any] = {
            "agent_id": aid,
            "configured": configured,
            "online": bool(age_s is not None and age_s <= stale),
            "age_s": age_s,
            "updated_at": updated_at or None,
            "started_at": float(rec.get("started_at") or 0.0) or None,
            "name": rec.get("name"),
            "role": rec.get("role"),
            "controller_kind": rec.get("controller_kind"),
            "version": rec.get("version"),
            "capabilities": (
                payload.get("capabilities") if isinstance(payload, dict) else None
            ),
        }
        if include_payload:
            out["payload"] = payload
        return out

    agents: List[Dict[str, Any]] = []

    # Configured agents first (self + peers).
    for aid in sorted(configured_ids):
        agents.append(_format(aid, by_id.get(aid), configured=True))

    # Then any other agents present in the DB.
    for aid, rec in sorted(by_id.items()):
        if aid in configured_ids:
            continue
        agents.append(_format(aid, rec, configured=False))

    online = sum(1 for a in agents if a.get("online"))
    configured = sum(1 for a in agents if a.get("configured"))

    return {
        "ok": True,
        "now": now,
        "stale_after_s": stale,
        "summary": {"agents": len(agents), "online": online, "configured": configured},
        "agents": agents,
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
        fn = a2a_service.actions().get(action)
        if fn is None:
            results["self"] = {"ok": False, "error": f"Unknown action '{action}'"}
        else:
            try:
                res = await fn(state, dict(req.params or {}))
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
    looks = getattr(state, "looks", None)
    if looks is None:
        raise RuntimeError("Look service not initialized")
    pack, row = await asyncio.to_thread(
        looks.choose_random,
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
            if state.wled_cooldown is not None:
                await state.wled_cooldown.wait()
            res = await asyncio.to_thread(
                looks.apply_look, row, brightness_override=bri
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
            res = await a2a_service.actions()["stop_all"](state, {})
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

    try:
        await persist_runtime_state(state, "fleet_stop_all", {"targets": req.targets})
    except Exception:
        pass
    return {"ok": True, "action": "stop_all", "results": results}

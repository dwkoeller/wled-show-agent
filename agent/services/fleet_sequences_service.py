from __future__ import annotations

import asyncio
import random
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from models.requests import FleetSequenceStartRequest, FleetSequenceStaggeredStartRequest
from services import fleet_service
from services import a2a_service
from services.auth_service import require_a2a_auth
from services.runtime_state_service import persist_runtime_state
from services.state import AppState, get_state


def _require_fleet_sequences(state: AppState):
    svc = getattr(state, "fleet_sequences", None)
    if svc is None:
        raise HTTPException(
            status_code=500, detail="Fleet sequence service not initialized."
        )
    return svc


async def fleet_sequences_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_fleet_sequences(state)
    st = await svc.status()
    return {"ok": True, "status": st.__dict__}


async def fleet_sequences_start(
    req: FleetSequenceStartRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_fleet_sequences(state)
    try:
        st = await svc.start(
            file=req.file,
            loop=req.loop,
            targets=req.targets,
            include_self=req.include_self,
            timeout_s=req.timeout_s,
        )
        try:
            await persist_runtime_state(
                state,
                "fleet_sequences_start",
                {"file": req.file, "loop": bool(req.loop), "targets": req.targets},
            )
        except Exception:
            pass
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def fleet_sequences_stop(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_fleet_sequences(state)
    st = await svc.stop()
    try:
        await persist_runtime_state(state, "fleet_sequences_stop")
    except Exception:
        pass
    return {"ok": True, "status": st.__dict__}


async def fleet_sequences_start_staggered(
    req: FleetSequenceStaggeredStartRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    timeout_s = (
        float(req.timeout_s)
        if req.timeout_s is not None
        else float(state.settings.a2a_http_timeout_s)
    )
    peers = await fleet_service._select_peers(state, req.targets)  # type: ignore[attr-defined]
    if req.shuffle:
        random.shuffle(peers)

    caps_list = await asyncio.gather(
        *[
            fleet_service._peer_supported_actions(  # type: ignore[attr-defined]
                state=state, peer=p, timeout_s=timeout_s
            )
            for p in peers
        ],
        return_exceptions=True,
    )
    caps_by_peer: Dict[str, Optional[set[str]]] = {}
    for peer, caps in zip(peers, caps_list):
        pname = getattr(peer, "name", str(peer))
        if isinstance(caps, set):
            caps_by_peer[pname] = caps
        else:
            caps_by_peer[pname] = None

    results: List[Dict[str, Any]] = []

    async def _invoke_peer(peer: Any, delay_s: float) -> Dict[str, Any]:
        pname = getattr(peer, "name", str(peer))
        caps = caps_by_peer.get(pname)
        if caps is not None and "start_sequence" not in caps:
            return {"target": pname, "ok": False, "error": "start_sequence not supported"}
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        payload = {"action": "start_sequence", "params": {"file": req.file, "loop": req.loop}}
        out = await fleet_service._peer_post_json(  # type: ignore[attr-defined]
            state=state,
            peer=peer,
            path="/v1/a2a/invoke",
            payload=payload,
            timeout_s=timeout_s,
        )
        out["target"] = pname
        return out

    async def _invoke_local(delay_s: float) -> Dict[str, Any]:
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        action = a2a_service.actions().get("start_sequence")
        if action is None:
            return {"target": "local", "ok": False, "error": "start_sequence not available"}
        res = await action(state, {"file": req.file, "loop": req.loop})
        return {"target": "local", "ok": True, "result": res}

    start_delay = max(0.0, float(req.start_delay_s or 0.0))
    stagger_s = max(0.0, float(req.stagger_s or 0.0))
    tasks: List[asyncio.Task[Dict[str, Any]]] = []
    index = 0

    if req.include_self:
        tasks.append(
            asyncio.create_task(_invoke_local(start_delay + (index * stagger_s)))
        )
        index += 1

    for peer in peers:
        tasks.append(
            asyncio.create_task(_invoke_peer(peer, start_delay + (index * stagger_s)))
        )
        index += 1

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=False)

    try:
        await persist_runtime_state(
            state,
            "fleet_sequences_start_staggered",
            {
                "file": req.file,
                "loop": bool(req.loop),
                "targets": req.targets,
                "include_self": bool(req.include_self),
                "stagger_s": float(stagger_s),
            },
        )
    except Exception:
        pass

    return {
        "ok": True,
        "results": results,
        "stagger_s": float(stagger_s),
        "start_delay_s": float(start_delay),
    }

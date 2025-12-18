from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi import Depends, HTTPException

from models.requests import FleetSequenceStartRequest
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
    st = await asyncio.to_thread(svc.status)
    return {"ok": True, "status": st.__dict__}


async def fleet_sequences_start(
    req: FleetSequenceStartRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_fleet_sequences(state)
    try:
        st = await asyncio.to_thread(
            svc.start,
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
    st = await asyncio.to_thread(svc.stop)
    try:
        await persist_runtime_state(state, "fleet_sequences_stop")
    except Exception:
        pass
    return {"ok": True, "status": st.__dict__}

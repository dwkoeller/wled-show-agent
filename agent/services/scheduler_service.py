from __future__ import annotations

from typing import Any, Dict

from fastapi import Depends, HTTPException

from services.auth_service import require_a2a_auth
from services.scheduler_core import SchedulerConfig, hhmm_to_minutes
from services.state import AppState, get_state


def _require_scheduler(state: AppState):
    sched = getattr(state, "scheduler", None)
    if sched is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    return sched


async def scheduler_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if hasattr(sched, "status"):
        return await sched.status()
    raise HTTPException(status_code=500, detail="Scheduler does not support status()")


async def scheduler_get_config(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "get_config"):
        raise HTTPException(
            status_code=500, detail="Scheduler does not support get_config()"
        )
    cfg = await sched.get_config()
    return {"ok": True, "config": cfg.model_dump()}


async def scheduler_set_config(
    cfg: SchedulerConfig,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    try:
        hhmm_to_minutes(cfg.start_hhmm)
        hhmm_to_minutes(cfg.end_hhmm)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not hasattr(sched, "set_config"):
        raise HTTPException(
            status_code=500, detail="Scheduler does not support set_config()"
        )
    await sched.set_config(cfg, persist=True)
    return {"ok": True, "config": cfg.model_dump()}


async def scheduler_start(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "start"):
        raise HTTPException(
            status_code=500, detail="Scheduler does not support start()"
        )
    await sched.start()
    return await sched.status()


async def scheduler_stop(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "stop"):
        raise HTTPException(status_code=500, detail="Scheduler does not support stop()")
    await sched.stop()
    return await sched.status()


async def scheduler_run_once(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "run_once"):
        raise HTTPException(
            status_code=500, detail="Scheduler does not support run_once()"
        )
    await sched.run_once()
    return await sched.status()


async def scheduler_events(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 200,
    agent_id: str | None = None,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        events = await db.list_scheduler_events(limit=int(limit), agent_id=agent_id)
        return {"ok": True, "events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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


def scheduler_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    return _require_scheduler(state).status()


def scheduler_get_config(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    return {"ok": True, "config": sched.get_config().model_dump()}


def scheduler_set_config(
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
    sched.set_config(cfg, persist=True)
    return {"ok": True, "config": cfg.model_dump()}


def scheduler_start(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    sched.start()
    return sched.status()


def scheduler_stop(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    sched.stop()
    return sched.status()


def scheduler_run_once(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    sched.run_once()
    return sched.status()

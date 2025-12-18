from __future__ import annotations

import asyncio
import time
import inspect
from typing import Any, Dict

from fastapi import Depends

from config.constants import APP_VERSION
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


async def metrics(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Lightweight JSON metrics for LAN monitoring.
    """
    fleet_st = None
    try:
        fleet = getattr(state, "fleet_sequences", None)
        if fleet is not None:
            fleet_st = (await asyncio.to_thread(fleet.status)).__dict__
    except Exception:
        fleet_st = None

    jobs_count = 0
    try:
        jobs = getattr(state, "jobs", None)
        if jobs is not None and hasattr(jobs, "list_jobs"):
            res = jobs.list_jobs(limit=10_000)
            rows = await res if inspect.isawaitable(res) else res
            jobs_count = len(list(rows or []))
    except Exception:
        jobs_count = 0

    scheduler_status: Dict[str, Any]
    try:
        sched = getattr(state, "scheduler", None)
        if sched is None:
            scheduler_status = {"ok": False, "error": "Scheduler not initialized"}
        else:
            res = sched.status()
            scheduler_status = await res if inspect.isawaitable(res) else res
    except Exception as e:
        scheduler_status = {"ok": False, "error": str(e)}

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

    peers = state.peers or {}

    return {
        "ok": True,
        "version": APP_VERSION,
        "uptime_s": float(state.uptime_s()),
        "peers_configured": len(peers),
        "jobs": {"count": jobs_count},
        "scheduler": scheduler_status,
        "ddp": ddp_st,
        "sequence": seq_st,
        "fleet_sequence": fleet_st,
    }

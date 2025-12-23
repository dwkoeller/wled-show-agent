from __future__ import annotations

import csv
import io
import json
import time
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse, Response

from services.audit_logger import log_event
from services.auth_service import require_a2a_auth, require_admin
from services.scheduler_async import SchedulerConfig, hhmm_to_minutes
from services.state import AppState, get_state


def _require_scheduler(state: AppState):
    sched = getattr(state, "scheduler", None)
    if sched is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    return sched


async def scheduler_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if hasattr(sched, "status"):
        try:
            status = await sched.status()
            await log_event(state, action="scheduler.status", ok=True, request=request)
            return status
        except HTTPException as e:
            await log_event(
                state,
                action="scheduler.status",
                ok=False,
                error=str(getattr(e, "detail", e)),
                request=request,
            )
            raise
        except Exception as e:
            await log_event(
                state, action="scheduler.status", ok=False, error=str(e), request=request
            )
            raise
    raise HTTPException(status_code=500, detail="Scheduler does not support status()")


async def scheduler_get_config(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "get_config"):
        raise HTTPException(
            status_code=500, detail="Scheduler does not support get_config()"
        )
    try:
        cfg = await sched.get_config()
        await log_event(
            state, action="scheduler.config.get", ok=True, request=request
        )
        return {"ok": True, "config": cfg.model_dump()}
    except HTTPException as e:
        await log_event(
            state,
            action="scheduler.config.get",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="scheduler.config.get",
            ok=False,
            error=str(e),
            request=request,
        )
        raise


async def scheduler_set_config(
    cfg: SchedulerConfig,
    request: Request,
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
    try:
        await sched.set_config(cfg, persist=True)
        await log_event(
            state,
            action="scheduler.config.set",
            ok=True,
            payload={
                "enabled": bool(cfg.enabled),
                "autostart": bool(cfg.autostart),
                "start_hhmm": cfg.start_hhmm,
                "end_hhmm": cfg.end_hhmm,
            },
            request=request,
        )
        return {"ok": True, "config": cfg.model_dump()}
    except HTTPException as e:
        await log_event(
            state,
            action="scheduler.config.set",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="scheduler.config.set",
            ok=False,
            error=str(e),
            request=request,
        )
        raise


async def scheduler_start(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "start"):
        raise HTTPException(
            status_code=500, detail="Scheduler does not support start()"
        )
    try:
        await sched.start()
        await log_event(
            state, action="scheduler.start", ok=True, request=request
        )
        return await sched.status()
    except HTTPException as e:
        await log_event(
            state,
            action="scheduler.start",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="scheduler.start", ok=False, error=str(e), request=request
        )
        raise


async def scheduler_stop(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "stop"):
        raise HTTPException(status_code=500, detail="Scheduler does not support stop()")
    try:
        await sched.stop()
        await log_event(
            state, action="scheduler.stop", ok=True, request=request
        )
        return await sched.status()
    except HTTPException as e:
        await log_event(
            state,
            action="scheduler.stop",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="scheduler.stop", ok=False, error=str(e), request=request
        )
        raise


async def scheduler_run_once(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    sched = _require_scheduler(state)
    if not hasattr(sched, "run_once"):
        raise HTTPException(
            status_code=500, detail="Scheduler does not support run_once()"
        )
    try:
        await sched.run_once()
        await log_event(
            state, action="scheduler.run_once", ok=True, request=request
        )
        return await sched.status()
    except HTTPException as e:
        await log_event(
            state,
            action="scheduler.run_once",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="scheduler.run_once", ok=False, error=str(e), request=request
        )
        raise


async def scheduler_events(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 200,
    agent_id: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, int(limit))
        off = max(0, int(offset))
        events = await db.list_scheduler_events(
            limit=lim,
            agent_id=agent_id,
            since=since,
            until=until,
            offset=off,
        )
        count = len(events)
        next_offset = off + count if count >= lim else None
        await log_event(
            state,
            action="scheduler.events",
            ok=True,
            payload={
                "limit": lim,
                "agent_id": agent_id,
                "offset": off,
                "since": since,
                "until": until,
            },
            request=request,
        )
        return {
            "ok": True,
            "events": events,
            "count": count,
            "limit": lim,
            "offset": off,
            "next_offset": next_offset,
        }
    except Exception as e:
        await log_event(
            state,
            action="scheduler.events",
            ok=False,
            error=str(e),
            payload={
                "limit": int(limit),
                "agent_id": agent_id,
                "offset": offset,
                "since": since,
                "until": until,
            },
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def scheduler_events_export(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
    limit: int = 2000,
    agent_id: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
    format: str = "csv",
) -> Dict[str, Any] | Response:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, min(20000, int(limit)))
        off = max(0, int(offset))
        events = await db.list_scheduler_events(
            limit=lim,
            agent_id=agent_id,
            since=since,
            until=until,
            offset=off,
        )
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = json.dumps({"ok": True, "events": events}, indent=2)
            return Response(content=payload, media_type="application/json")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "agent_id",
                "created_at",
                "action",
                "scope",
                "reason",
                "ok",
                "duration_s",
                "error",
                "payload",
            ]
        )
        for row in events:
            payload = row.get("payload") or {}
            writer.writerow(
                [
                    row.get("id"),
                    row.get("agent_id"),
                    row.get("created_at"),
                    row.get("action"),
                    row.get("scope"),
                    row.get("reason"),
                    row.get("ok"),
                    row.get("duration_s"),
                    row.get("error"),
                    json.dumps(payload, separators=(",", ":")),
                ]
            )
        await log_event(
            state,
            action="scheduler.events.export",
            ok=True,
            payload={
                "limit": lim,
                "agent_id": agent_id,
                "offset": off,
                "format": fmt,
                "since": since,
                "until": until,
            },
            request=request,
        )
        return PlainTextResponse(
            output.getvalue(),
            headers={"Content-Disposition": "attachment; filename=scheduler_events.csv"},
        )
    except Exception as e:
        await log_event(
            state,
            action="scheduler.events.export",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def scheduler_retention_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        stats = await db.scheduler_events_stats()
        now = time.time()
        max_rows = int(getattr(state.settings, "scheduler_events_max_rows", 0) or 0)
        max_days = int(getattr(state.settings, "scheduler_events_max_days", 0) or 0)
        oldest = stats.get("oldest")
        oldest_age_s = max(0.0, now - float(oldest)) if oldest else None
        excess_rows = max(0, int(stats.get("count", 0)) - max_rows) if max_rows else 0
        excess_age_s = (
            max(0.0, float(oldest_age_s) - (max_days * 86400.0))
            if max_days and oldest_age_s is not None
            else 0.0
        )
        drift = bool(excess_rows > 0 or excess_age_s > 0)
        return {
            "ok": True,
            "stats": stats,
            "settings": {
                "max_rows": max_rows,
                "max_days": max_days,
                "maintenance_interval_s": int(
                    getattr(state.settings, "scheduler_events_maintenance_interval_s", 0)
                    or 0
                ),
            },
            "drift": {
                "excess_rows": int(excess_rows),
                "excess_age_s": float(excess_age_s),
                "oldest_age_s": float(oldest_age_s) if oldest_age_s is not None else None,
                "drift": drift,
            },
            "last_retention": getattr(state, "scheduler_events_retention_last", None),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def scheduler_retention_cleanup(
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        cfg_max_rows = int(getattr(state.settings, "scheduler_events_max_rows", 0) or 0)
        cfg_max_days = int(getattr(state.settings, "scheduler_events_max_days", 0) or 0)
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await db.enforce_scheduler_events_retention(
            max_rows=use_max_rows,
            max_days=use_max_days,
        )
        state.scheduler_events_retention_last = {
            "at": time.time(),
            "result": result,
        }
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

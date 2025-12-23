from __future__ import annotations

import asyncio
import csv
import inspect
import io
import json
import time
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request
from fastapi.responses import Response

from config.constants import APP_VERSION
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth, require_admin
from services.state import AppState, get_state
from utils.outbound_metrics import REGISTRY as OUTBOUND_REGISTRY
from utils.rate_limit_metrics import REGISTRY as RATE_LIMIT_REGISTRY


async def collect_metrics_snapshot(state: AppState) -> Dict[str, Any]:
    fleet_st = None
    try:
        fleet = getattr(state, "fleet_sequences", None)
        if fleet is not None:
            fleet_st = (await fleet.status()).__dict__
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
        ddp_st = (await ddp.status()).__dict__ if ddp is not None else None
    except Exception:
        ddp_st = None
    ddp_metrics = None
    try:
        ddp = getattr(state, "ddp", None)
        if ddp is not None and hasattr(ddp, "metrics"):
            ddp_metrics = (await ddp.metrics()).__dict__
    except Exception:
        ddp_metrics = None

    seq_st = None
    try:
        seq = getattr(state, "sequences", None)
        seq_st = (await seq.status()).__dict__ if seq is not None else None
    except Exception:
        seq_st = None

    events_bus = None
    spool_stats = None
    try:
        events = getattr(state, "events", None)
        if events is not None and hasattr(events, "stats"):
            events_bus = await events.stats()
    except Exception:
        events_bus = None
    try:
        from services.events_service import get_spool_stats

        spool_stats = await get_spool_stats(state)
    except Exception:
        spool_stats = None

    peers = state.peers or {}

    outbound = None
    try:
        outbound = OUTBOUND_REGISTRY.snapshot()
    except Exception:
        outbound = None
    rate_limit = None
    try:
        rate_limit = RATE_LIMIT_REGISTRY.snapshot()
    except Exception:
        rate_limit = None

    return {
        "ok": True,
        "version": APP_VERSION,
        "uptime_s": float(state.uptime_s()),
        "peers_configured": len(peers),
        "jobs": {"count": jobs_count},
        "scheduler": scheduler_status,
        "ddp": ddp_st,
        "ddp_metrics": ddp_metrics,
        "sequence": seq_st,
        "fleet_sequence": fleet_st,
        "events": {"bus": events_bus, "spool": spool_stats},
        "outbound": outbound,
        "rate_limit": rate_limit,
    }


async def metrics(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Lightweight JSON metrics for LAN monitoring.
    """
    payload = await collect_metrics_snapshot(state)
    await log_event(
        state,
        action="metrics.json",
        ok=True,
        payload={
            "peers_configured": int(payload.get("peers_configured", 0) or 0),
            "jobs": int(payload.get("jobs", {}).get("count", 0) or 0),
        },
        request=request,
    )
    return payload


async def metrics_history(
    request: Request,
    limit: int = 200,
    offset: int = 0,
    since: float | None = None,
    until: float | None = None,
    order: str = "desc",
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, min(5000, int(limit)))
        off = max(0, int(offset))
        rows = await db.list_metrics_samples(
            limit=lim,
            since=since,
            until=until,
            offset=off,
            order=order,
        )
        count = len(rows)
        next_offset = off + count if count >= lim else None
        await log_event(
            state,
            action="metrics.history",
            ok=True,
            payload={"limit": lim, "offset": off, "count": count},
            request=request,
        )
        return {
            "ok": True,
            "samples": rows,
            "count": count,
            "limit": lim,
            "offset": off,
            "next_offset": next_offset,
        }
    except Exception as e:
        await log_event(
            state,
            action="metrics.history",
            ok=False,
            error=str(e),
            payload={"limit": int(limit), "offset": int(offset)},
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def metrics_history_export(
    request: Request,
    format: str = "csv",
    limit: int = 2000,
    since: float | None = None,
    until: float | None = None,
    order: str = "desc",
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = max(1, min(20000, int(limit)))
        rows = await db.list_metrics_samples(
            limit=lim,
            since=since,
            until=until,
            order=order,
        )
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = {"ok": True, "samples": rows, "count": len(rows)}
            await log_event(
                state,
                action="metrics.history.export",
                ok=True,
                payload={"format": "json", "count": len(rows)},
                request=request,
            )
            return Response(
                content=json.dumps(payload, separators=(",", ":")),
                media_type="application/json",
                headers={
                    "Content-Disposition": "attachment; filename=metrics_history.json"
                },
            )

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "created_at",
                "jobs_count",
                "scheduler_ok",
                "scheduler_running",
                "scheduler_in_window",
                "outbound_failures",
                "outbound_retries",
                "spool_dropped",
                "spool_queued_events",
                "spool_queued_bytes",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.get("created_at"),
                    row.get("jobs_count"),
                    row.get("scheduler_ok"),
                    row.get("scheduler_running"),
                    row.get("scheduler_in_window"),
                    row.get("outbound_failures"),
                    row.get("outbound_retries"),
                    row.get("spool_dropped"),
                    row.get("spool_queued_events"),
                    row.get("spool_queued_bytes"),
                ]
            )
        await log_event(
            state,
            action="metrics.history.export",
            ok=True,
            payload={"format": "csv", "count": len(rows)},
            request=request,
        )
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=metrics_history.csv"},
        )
    except Exception as e:
        await log_event(
            state,
            action="metrics.history.export",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def metrics_history_retention_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        stats = await db.metrics_history_stats()
        now = time.time()
        max_rows = int(getattr(state.settings, "metrics_history_max_rows", 0) or 0)
        max_days = int(getattr(state.settings, "metrics_history_max_days", 0) or 0)
        oldest = stats.get("oldest")
        oldest_age_s = max(0.0, now - float(oldest)) if oldest else None
        excess_rows = max(0, int(stats.get("count", 0)) - max_rows) if max_rows else 0
        excess_age_s = (
            max(0.0, float(oldest_age_s) - (max_days * 86400.0))
            if max_days and oldest_age_s is not None
            else 0.0
        )
        drift = bool(excess_rows > 0 or excess_age_s > 0)
        payload = {
            "ok": True,
            "stats": stats,
            "settings": {
                "interval_s": int(
                    getattr(state.settings, "metrics_history_interval_s", 0) or 0
                ),
                "maintenance_interval_s": int(
                    getattr(state.settings, "metrics_history_maintenance_interval_s", 0)
                    or 0
                ),
                "max_rows": max_rows,
                "max_days": max_days,
            },
            "drift": {
                "excess_rows": int(excess_rows),
                "excess_age_s": float(excess_age_s),
                "oldest_age_s": float(oldest_age_s) if oldest_age_s is not None else None,
                "drift": drift,
            },
            "last_retention": getattr(state, "metrics_history_retention_last", None),
        }
        await log_event(
            state,
            action="metrics.history.retention.status",
            ok=True,
            payload={
                "count": int(stats.get("count", 0)),
                "drift": drift,
            },
            request=request,
        )
        return payload
    except Exception as e:
        await log_event(
            state,
            action="metrics.history.retention.status",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))


async def metrics_history_retention_cleanup(
    request: Request,
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        cfg_max_rows = int(getattr(state.settings, "metrics_history_max_rows", 0) or 0)
        cfg_max_days = int(getattr(state.settings, "metrics_history_max_days", 0) or 0)
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await db.enforce_metrics_history_retention(
            max_rows=use_max_rows,
            max_days=use_max_days,
        )
        payload = {"ok": True, "result": result}
        state.metrics_history_retention_last = {
            "at": time.time(),
            "result": result,
        }
        await log_event(
            state,
            action="metrics.history.retention.cleanup",
            ok=True,
            payload={
                "max_rows": use_max_rows,
                "max_days": use_max_days,
                "deleted_by_rows": result.get("deleted_by_rows"),
                "deleted_by_days": result.get("deleted_by_days"),
            },
            request=request,
        )
        return payload
    except Exception as e:
        await log_event(
            state,
            action="metrics.history.retention.cleanup",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))

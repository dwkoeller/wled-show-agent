from __future__ import annotations

import csv
import io
import json
import time
from typing import Any, Dict

from fastapi import Depends, HTTPException
from fastapi.responses import PlainTextResponse, Response

from services.auth_service import require_a2a_auth, require_admin
from services.state import AppState, get_state


def _clamp_limit(limit: int, *, default: int = 200, max_limit: int = 2000) -> int:
    try:
        n = int(limit)
    except Exception:
        n = default
    return max(1, min(int(max_limit), n))


async def audit_logs(
    limit: int = 200,
    action: str | None = None,
    actor: str | None = None,
    agent_id: str | None = None,
    ok: bool | None = None,
    resource: str | None = None,
    ip: str | None = None,
    error: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = _clamp_limit(limit)
        off = max(0, int(offset))
        logs = await db.list_audit_logs(
            limit=lim,
            agent_id=agent_id,
            action=action,
            actor=actor,
            ok=ok,
            resource=resource,
            ip=ip,
            error_contains=error,
            since=since,
            until=until,
            offset=off,
        )
        count = len(logs)
        next_offset = off + count if count >= lim else None
        return {
            "ok": True,
            "logs": logs,
            "count": count,
            "limit": lim,
            "offset": off,
            "next_offset": next_offset,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def audit_logs_export(
    limit: int = 2000,
    action: str | None = None,
    actor: str | None = None,
    agent_id: str | None = None,
    ok: bool | None = None,
    resource: str | None = None,
    ip: str | None = None,
    error: str | None = None,
    since: float | None = None,
    until: float | None = None,
    offset: int = 0,
    format: str = "csv",
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Response:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        lim = _clamp_limit(limit, default=2000, max_limit=20000)
        logs = await db.list_audit_logs(
            limit=lim,
            agent_id=agent_id,
            action=action,
            actor=actor,
            ok=ok,
            resource=resource,
            ip=ip,
            error_contains=error,
            since=since,
            until=until,
            offset=offset,
        )
        fmt = str(format or "csv").strip().lower()
        if fmt == "json":
            payload = json.dumps({"ok": True, "logs": logs}, indent=2)
            return Response(content=payload, media_type="application/json")

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "id",
                "agent_id",
                "created_at",
                "actor",
                "action",
                "resource",
                "ok",
                "error",
                "ip",
                "user_agent",
                "request_id",
                "payload",
            ]
        )
        for row in logs:
            payload = row.get("payload") or {}
            writer.writerow(
                [
                    row.get("id"),
                    row.get("agent_id"),
                    row.get("created_at"),
                    row.get("actor"),
                    row.get("action"),
                    row.get("resource"),
                    row.get("ok"),
                    row.get("error"),
                    row.get("ip"),
                    row.get("user_agent"),
                    row.get("request_id"),
                    json.dumps(payload, separators=(",", ":")),
                ]
            )
        return PlainTextResponse(
            output.getvalue(),
            headers={"Content-Disposition": "attachment; filename=audit_logs.csv"},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def audit_retention_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        stats = await db.audit_log_stats()
        now = time.time()
        max_rows = int(getattr(state.settings, "audit_log_max_rows", 0) or 0)
        max_days = int(getattr(state.settings, "audit_log_max_days", 0) or 0)
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
                    getattr(state.settings, "audit_log_maintenance_interval_s", 0) or 0
                ),
            },
            "drift": {
                "excess_rows": int(excess_rows),
                "excess_age_s": float(excess_age_s),
                "oldest_age_s": float(oldest_age_s) if oldest_age_s is not None else None,
                "drift": drift,
            },
            "last_retention": getattr(state, "audit_log_retention_last", None),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def audit_retention_cleanup(
    max_rows: int | None = None,
    max_days: int | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    try:
        cfg_max_rows = int(getattr(state.settings, "audit_log_max_rows", 0) or 0)
        cfg_max_days = int(getattr(state.settings, "audit_log_max_days", 0) or 0)
        use_max_rows = max_rows if max_rows is not None else (cfg_max_rows or None)
        use_max_days = max_days if max_days is not None else (cfg_max_days or None)
        result = await db.enforce_audit_log_retention(
            max_rows=use_max_rows,
            max_days=use_max_days,
        )
        state.audit_log_retention_last = {
            "at": time.time(),
            "result": result,
        }
        return {"ok": True, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

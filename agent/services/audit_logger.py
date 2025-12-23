from __future__ import annotations

from typing import Any, Dict

from fastapi import Request

from services.state import AppState


def _actor_from_request(request: Request | None) -> str | None:
    if request is None:
        return None
    actor = getattr(request.state, "user", None)
    if actor:
        return str(actor)
    return None


def _request_meta(request: Request | None) -> Dict[str, str | None]:
    if request is None:
        return {
            "ip": None,
            "user_agent": None,
            "request_id": None,
        }
    ip = None
    try:
        ip = request.client.host if request.client else None
    except Exception:
        ip = None
    return {
        "ip": ip,
        "user_agent": request.headers.get("user-agent"),
        "request_id": getattr(request.state, "request_id", None),
    }


async def log_event(
    state: AppState,
    *,
    action: str,
    actor: str | None = None,
    ok: bool = True,
    resource: str | None = None,
    error: str | None = None,
    payload: Dict[str, Any] | None = None,
    request: Request | None = None,
    emit: bool = True,
) -> None:
    actor_val = str(actor or _actor_from_request(request) or "unknown")
    meta = _request_meta(request)
    db = getattr(state, "db", None)
    if db is not None:
        try:
            await db.add_audit_log(
                action=str(action or ""),
                actor=actor_val,
                ok=bool(ok),
                resource=str(resource or "") or None,
                error=str(error) if error else None,
                ip=meta.get("ip"),
                user_agent=meta.get("user_agent"),
                request_id=meta.get("request_id"),
                payload=dict(payload or {}),
            )
        except Exception:
            pass

    try:
        payload = {
            "action": str(action or ""),
            "actor": actor_val,
            "ok": bool(ok),
            "resource": str(resource or "") or None,
            "error": str(error) if error else None,
        }
        from services.events_service import emit_event_for_action

        if emit:
            await emit_event_for_action(state, action=str(action or ""), data=payload)
        await emit_event_for_action(state, action="audit.log", data=payload)
    except Exception:
        return

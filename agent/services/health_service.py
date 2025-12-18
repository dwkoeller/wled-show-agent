from __future__ import annotations

from typing import Any, Dict

from fastapi import Depends
from fastapi.responses import JSONResponse

from config.constants import APP_VERSION, SERVICE_NAME
from services.state import AppState, get_state


def root() -> Dict[str, Any]:
    return {"ok": True, "service": SERVICE_NAME, "version": APP_VERSION}


def health() -> Dict[str, Any]:
    return {"ok": True, "service": SERVICE_NAME, "version": APP_VERSION}


def livez() -> JSONResponse:
    return JSONResponse(status_code=200, content={"ok": True})


async def readyz(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    checks: Dict[str, Any] = {}
    ok = True

    # WLED reachability (best-effort; skip if not configured).
    wled = getattr(state, "wled", None)
    if wled is not None:
        try:
            info = await wled.get_info()
            checks["wled"] = {"ok": True, "name": (info or {}).get("name")}
        except Exception as e:
            ok = False
            checks["wled"] = {"ok": False, "error": str(e)}
    else:
        checks["wled"] = {"ok": True, "skipped": True}

    # DB reachability (if configured).
    if state.db is not None:
        try:
            h = await state.db.health()
            ok = ok and bool(h.ok)
            checks["db"] = {"ok": bool(h.ok), "detail": str(h.detail)}
        except Exception as e:
            ok = False
            checks["db"] = {"ok": False, "error": str(e)}
    else:
        checks["db"] = {"ok": True, "skipped": True}

    return JSONResponse(
        status_code=200 if ok else 503,
        content={"ok": ok, "checks": checks},
    )

from __future__ import annotations

from typing import Any, Dict

from fastapi import Depends
from fastapi.responses import JSONResponse

from config.constants import APP_VERSION, SERVICE_NAME
from services.state import AppState, get_state


async def root() -> Dict[str, Any]:
    return {"ok": True, "service": SERVICE_NAME, "version": APP_VERSION}


async def health() -> Dict[str, Any]:
    return {"ok": True, "service": SERVICE_NAME, "version": APP_VERSION}


async def livez() -> JSONResponse:
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

    # LedFx reachability (optional).
    if getattr(state.settings, "ledfx_base_url", ""):
        try:
            from ledfx_client import AsyncLedFxClient
            from utils.outbound_http import retry_policy_from_settings

            if state.peer_http is None:
                raise RuntimeError("HTTP client not initialized")
            client = AsyncLedFxClient(
                base_url=state.settings.ledfx_base_url,
                client=state.peer_http,
                timeout_s=float(state.settings.ledfx_http_timeout_s),
                headers={k: v for (k, v) in state.settings.ledfx_headers},
                retry=retry_policy_from_settings(state.settings),
            )
            res = await client.status()
            checks["ledfx"] = {"ok": True, "status": res.status_code}
        except Exception as e:
            ok = False
            checks["ledfx"] = {"ok": False, "error": str(e)}
    else:
        checks["ledfx"] = {"ok": True, "skipped": True}

    return JSONResponse(
        status_code=200 if ok else 503,
        content={"ok": ok, "checks": checks},
    )

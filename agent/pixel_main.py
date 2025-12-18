from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from auth import (
    AuthError,
    jwt_decode_hs256,
    jwt_encode_hs256,
    totp_verify,
    verify_password,
)
from config import Settings, load_settings
from geometry import TreeGeometry
from patterns import PatternFactory
from pixel_streamer import PixelStreamConfig, PixelStreamer


app = FastAPI(title="Pixel Streaming Agent", version="3.4.0")


SETTINGS: Settings = load_settings()
if SETTINGS.controller_kind != "pixel":
    raise RuntimeError("pixel_main requires CONTROLLER_KIND=pixel")


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    if not SETTINGS.auth_enabled:
        return await call_next(request)

    path = request.url.path or ""
    if (
        path.startswith("/v1/health")
        or path.startswith("/v1/auth/config")
        or path.startswith("/v1/auth/login")
        or path.startswith("/v1/auth/logout")
    ):
        return await call_next(request)

    if request.method.upper() == "OPTIONS":
        return await call_next(request)

    key = SETTINGS.a2a_api_key
    if key:
        cand = (request.headers.get("x-a2a-key") or "").strip()
        if cand == key:
            return await call_next(request)
        auth = request.headers.get("authorization") or ""
        parts = auth.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip() == key:
            return await call_next(request)

    tok = _jwt_from_request(request)
    if tok:
        try:
            jwt_decode_hs256(
                tok,
                secret=str(SETTINGS.auth_jwt_secret or ""),
                issuer=str(SETTINGS.auth_jwt_issuer or ""),
            )
            return await call_next(request)
        except AuthError:
            pass

    return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})


def _require_a2a_auth(
    request: Request,
    x_a2a_key: str | None = Header(default=None, alias="X-A2A-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> None:
    key = SETTINGS.a2a_api_key
    candidate = x_a2a_key
    if (not candidate) and authorization:
        parts = str(authorization).strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            candidate = parts[1].strip()

    # If JWT auth is enabled, allow either a valid A2A key (if set) or a valid JWT.
    if SETTINGS.auth_enabled:
        if key and candidate == key:
            return
        tok = _jwt_from_request(request)
        if not tok:
            raise HTTPException(status_code=401, detail="Missing token")
        try:
            jwt_decode_hs256(
                tok,
                secret=str(SETTINGS.auth_jwt_secret or ""),
                issuer=str(SETTINGS.auth_jwt_issuer or ""),
            )
            return
        except AuthError as e:
            raise HTTPException(status_code=401, detail=str(e))

    # Legacy mode: only enforce A2A key if configured.
    if not key:
        return
    if candidate != key:
        raise HTTPException(status_code=401, detail="Missing or invalid A2A key")


GEOM = TreeGeometry(
    runs=SETTINGS.tree_runs,
    pixels_per_run=SETTINGS.tree_pixels_per_run,
    segment_len=SETTINGS.tree_segment_len,
    segments_per_run=SETTINGS.tree_segments_per_run,
)

STREAMER = PixelStreamer(
    led_count=SETTINGS.pixel_count,
    geometry=GEOM,
    cfg=PixelStreamConfig(
        protocol=SETTINGS.pixel_protocol,
        host=SETTINGS.pixel_host,
        port=SETTINGS.pixel_port,
        universe_start=SETTINGS.pixel_universe_start,
        channels_per_universe=SETTINGS.pixel_channels_per_universe,
        priority=SETTINGS.pixel_priority,
        source_name=SETTINGS.pixel_source_name,
    ),
    fps_default=SETTINGS.ddp_fps_default,
    fps_max=SETTINGS.ddp_fps_max,
)


class DDPStartRequest(BaseModel):
    pattern: str
    params: Dict[str, Any] = Field(default_factory=dict)
    duration_s: float = Field(30.0, ge=0.1, le=600.0)
    brightness: int = Field(128, ge=1, le=255)
    fps: Optional[float] = Field(default=None, ge=1.0, le=60.0)
    direction: Optional[str] = Field(
        default=None, description="Optional: cw or ccw (passed through as param)"
    )
    start_pos: Optional[str] = Field(
        default=None,
        description="Optional: front/right/back/left (passed through as param)",
    )


class A2AInvokeRequest(BaseModel):
    action: str
    params: Dict[str, Any] = Field(default_factory=dict)
    request_id: Optional[str] = None


class AuthLoginRequest(BaseModel):
    username: str
    password: str
    totp: Optional[str] = None


def _jwt_from_request(request: Request) -> str | None:
    auth = request.headers.get("authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        tok = parts[1].strip()
        if tok and tok.count(".") == 2:
            return tok
    if SETTINGS.auth_cookie_name:
        tok = request.cookies.get(SETTINGS.auth_cookie_name)
        if tok:
            return str(tok).strip()
    return None


def _require_jwt_auth(request: Request) -> Dict[str, Any]:
    if not SETTINGS.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; JWT auth is not configured."
        )
    tok = _jwt_from_request(request)
    if not tok:
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        claims = jwt_decode_hs256(
            tok,
            secret=str(SETTINGS.auth_jwt_secret or ""),
            issuer=str(SETTINGS.auth_jwt_issuer or ""),
        )
        return {
            "subject": claims.subject,
            "expires_at": claims.expires_at,
            "issued_at": claims.issued_at,
            "claims": claims.raw,
        }
    except AuthError as e:
        raise HTTPException(status_code=401, detail=str(e))


@app.get("/v1/auth/config")
def auth_config() -> Dict[str, Any]:
    return {
        "ok": True,
        "version": app.version,
        "auth_enabled": bool(SETTINGS.auth_enabled),
        "totp_enabled": bool(SETTINGS.auth_totp_enabled),
        "openai_enabled": False,
        "fpp_enabled": False,
    }


@app.post("/v1/auth/login")
def auth_login(req: AuthLoginRequest, response: Response) -> Dict[str, Any]:
    if not SETTINGS.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; login is disabled."
        )
    user = (req.username or "").strip()
    if user != (SETTINGS.auth_username or "").strip():
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if not verify_password(req.password, str(SETTINGS.auth_password or "")):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if SETTINGS.auth_totp_enabled:
        if not totp_verify(
            secret_b32=str(SETTINGS.auth_totp_secret or ""), code=str(req.totp or "")
        ):
            raise HTTPException(status_code=401, detail="Invalid TOTP code")

    token = jwt_encode_hs256(
        {"sub": user, "role": "admin"},
        secret=str(SETTINGS.auth_jwt_secret or ""),
        ttl_s=int(SETTINGS.auth_jwt_ttl_s),
        issuer=str(SETTINGS.auth_jwt_issuer or ""),
    )

    response.set_cookie(
        key=str(SETTINGS.auth_cookie_name),
        value=token,
        httponly=True,
        secure=bool(SETTINGS.auth_cookie_secure),
        samesite="lax",
        max_age=int(SETTINGS.auth_jwt_ttl_s),
        path="/",
    )
    return {
        "ok": True,
        "user": {"username": user},
        "token": token,
        "expires_in": int(SETTINGS.auth_jwt_ttl_s),
    }


@app.post("/v1/auth/logout")
def auth_logout(response: Response) -> Dict[str, Any]:
    if SETTINGS.auth_cookie_name:
        response.delete_cookie(key=str(SETTINGS.auth_cookie_name), path="/")
    return {"ok": True}


@app.get("/v1/auth/me")
def auth_me(request: Request) -> Dict[str, Any]:
    info = _require_jwt_auth(request)
    return {
        "ok": True,
        "user": {"username": info["subject"]},
        "expires_at": info["expires_at"],
    }


def _action_start_pattern(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(kwargs.get("params") or {})
    if kwargs.get("direction") and "direction" not in params:
        params["direction"] = kwargs.get("direction")
    if kwargs.get("start_pos") and "start_pos" not in params:
        params["start_pos"] = kwargs.get("start_pos")
    st = STREAMER.start(
        pattern=str(kwargs.get("pattern")),
        params=params,
        duration_s=float(kwargs.get("duration_s", 30.0)),
        brightness=min(SETTINGS.wled_max_bri, int(kwargs.get("brightness", 128))),
        fps=float(kwargs.get("fps", SETTINGS.ddp_fps_default)),
    )
    return {"status": st.__dict__}


def _action_stop(_: Dict[str, Any]) -> Dict[str, Any]:
    return {"status": STREAMER.stop().__dict__}


def _action_status(_: Dict[str, Any]) -> Dict[str, Any]:
    return {"ddp": STREAMER.status().__dict__}


_ACTIONS: Dict[str, Any] = {
    "start_ddp_pattern": _action_start_pattern,
    "stop_ddp": _action_stop,
    "stop_all": _action_stop,
    "status": _action_status,
}

_CAPABILITIES: List[Dict[str, Any]] = [
    {
        "action": "start_ddp_pattern",
        "description": "Start a realtime procedural pattern stream (sACN/Art-Net) for a duration.",
        "params": {
            "pattern": "string",
            "params": "object",
            "duration_s": "number",
            "brightness": "int",
            "fps": "number",
        },
    },
    {
        "action": "stop_ddp",
        "description": "Stop any running realtime stream.",
        "params": {},
    },
    {"action": "stop_all", "description": "Alias for stop_ddp.", "params": {}},
    {"action": "status", "description": "Get current stream status.", "params": {}},
]


@app.get("/v1/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "pixel-stream-agent",
        "version": app.version,
        "controller_kind": SETTINGS.controller_kind,
        "protocol": SETTINGS.pixel_protocol,
        "pixel_count": SETTINGS.pixel_count,
    }


@app.get("/v1/ddp/patterns")
def ddp_patterns() -> Dict[str, Any]:
    factory = PatternFactory(
        led_count=SETTINGS.pixel_count, geometry=GEOM, segment_layout=None
    )
    return {
        "ok": True,
        "patterns": factory.available(),
        "geometry_enabled": GEOM.enabled_for(SETTINGS.pixel_count),
    }


@app.get("/v1/ddp/status")
def ddp_status() -> Dict[str, Any]:
    return {"ok": True, "status": STREAMER.status().__dict__}


@app.post("/v1/ddp/start")
def ddp_start(req: DDPStartRequest) -> Dict[str, Any]:
    try:
        params = dict(req.params or {})
        if req.direction and "direction" not in params:
            params["direction"] = req.direction
        if req.start_pos and "start_pos" not in params:
            params["start_pos"] = req.start_pos
        st = STREAMER.start(
            pattern=req.pattern,
            params=params,
            duration_s=req.duration_s,
            brightness=min(SETTINGS.wled_max_bri, req.brightness),
            fps=req.fps,
        )
        return {"ok": True, "status": st.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/ddp/stop")
def ddp_stop() -> Dict[str, Any]:
    st = STREAMER.stop()
    return {"ok": True, "status": st.__dict__}


@app.get("/v1/a2a/card")
def a2a_card(_: None = Depends(_require_a2a_auth)) -> Dict[str, Any]:
    return {
        "ok": True,
        "agent": {
            "id": SETTINGS.agent_id,
            "name": SETTINGS.agent_name,
            "role": SETTINGS.agent_role,
            "version": app.version,
            "controller_kind": SETTINGS.controller_kind,
            "endpoints": {"card": "/v1/a2a/card", "invoke": "/v1/a2a/invoke"},
            "pixel": {
                "protocol": SETTINGS.pixel_protocol,
                "host": SETTINGS.pixel_host,
                "port": SETTINGS.pixel_port,
                "universe_start": SETTINGS.pixel_universe_start,
                "channels_per_universe": SETTINGS.pixel_channels_per_universe,
                "pixel_count": SETTINGS.pixel_count,
            },
            "capabilities": _CAPABILITIES,
        },
    }


@app.post("/v1/a2a/invoke")
def a2a_invoke(
    req: A2AInvokeRequest, _: None = Depends(_require_a2a_auth)
) -> Dict[str, Any]:
    action = (req.action or "").strip()
    fn = _ACTIONS.get(action)
    if fn is None:
        return {
            "ok": False,
            "request_id": req.request_id,
            "error": f"Unknown action '{action}'",
        }
    try:
        res = fn(dict(req.params or {}))
        return {
            "ok": True,
            "request_id": req.request_id,
            "action": action,
            "result": res,
        }
    except Exception as e:
        return {
            "ok": False,
            "request_id": req.request_id,
            "action": action,
            "error": str(e),
        }

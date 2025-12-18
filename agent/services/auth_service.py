from __future__ import annotations

from typing import Any, Dict

from fastapi import Depends, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from auth import (
    AuthError,
    jwt_decode_hs256,
    jwt_encode_hs256,
    totp_verify,
    verify_password,
)
from config.constants import APP_VERSION
from models.requests import AuthLoginRequest
from services.state import AppState, get_state


def _jwt_from_request(request: Request, *, state: AppState) -> str | None:
    settings = state.settings
    auth = request.headers.get("authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        tok = parts[1].strip()
        # Only treat Bearer values that look like a JWT as JWTs; this avoids
        # conflicting with A2A keys or other non-JWT bearer tokens.
        if tok and tok.count(".") == 2:
            return tok
    if settings.auth_cookie_name:
        tok = request.cookies.get(settings.auth_cookie_name)
        if tok:
            return str(tok).strip()
    return None


def require_jwt_auth(
    request: Request,
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    settings = state.settings
    if not settings.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; JWT auth is not configured."
        )
    tok = _jwt_from_request(request, state=state)
    if not tok:
        raise HTTPException(status_code=401, detail="Missing token")
    try:
        claims = jwt_decode_hs256(
            tok,
            secret=str(settings.auth_jwt_secret or ""),
            issuer=str(settings.auth_jwt_issuer or ""),
        )
        return {
            "subject": claims.subject,
            "expires_at": claims.expires_at,
            "issued_at": claims.issued_at,
            "claims": claims.raw,
        }
    except AuthError as e:
        raise HTTPException(status_code=401, detail=str(e))


def require_a2a_auth(
    request: Request,
    x_a2a_key: str | None = Header(default=None, alias="X-A2A-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    state: AppState = Depends(get_state),
) -> None:
    settings = state.settings
    key = settings.a2a_api_key
    candidate = x_a2a_key
    if (not candidate) and authorization:
        parts = str(authorization).strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            candidate = parts[1].strip()

    # If JWT auth is enabled, allow either a valid A2A key (if set) or a valid JWT.
    if settings.auth_enabled:
        if key and candidate == key:
            return
        tok = _jwt_from_request(request, state=state)
        if not tok:
            raise HTTPException(status_code=401, detail="Missing token")
        try:
            jwt_decode_hs256(
                tok,
                secret=str(settings.auth_jwt_secret or ""),
                issuer=str(settings.auth_jwt_issuer or ""),
            )
            return
        except AuthError as e:
            raise HTTPException(status_code=401, detail=str(e))

    # Legacy mode: only enforce A2A key if configured.
    if not key:
        return
    if candidate != key:
        raise HTTPException(status_code=401, detail="Missing or invalid A2A key")


async def auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    st = getattr(request.app.state, "wsa", None)
    if st is None:
        return await call_next(request)

    settings = st.settings
    if not settings.auth_enabled:
        return await call_next(request)

    path = request.url.path or ""

    # Public endpoints.
    if (
        path == "/"
        or path == "/readyz"
        or path.startswith("/ui")
        or path.startswith("/v1/health")
        or path.startswith("/v1/auth/config")
        or path.startswith("/v1/auth/login")
        or path.startswith("/v1/auth/logout")
    ):
        return await call_next(request)

    # Allow Prometheus scraping based on config.
    if path == "/metrics":
        if settings.metrics_public:
            return await call_next(request)
        tok = (settings.metrics_scrape_token or "").strip()
        hdr = (settings.metrics_scrape_header or "X-Metrics-Token").strip()
        if tok and hdr and (request.headers.get(hdr) or "").strip() == tok:
            return await call_next(request)

    # Allow preflight requests to proceed.
    if request.method.upper() == "OPTIONS":
        return await call_next(request)

    # Allow either the configured A2A key (if set) or a valid JWT.
    key = settings.a2a_api_key
    if key:
        cand = (request.headers.get("x-a2a-key") or "").strip()
        if cand == key:
            return await call_next(request)
        auth = request.headers.get("authorization") or ""
        parts = auth.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip() == key:
            return await call_next(request)

    tok = _jwt_from_request(request, state=st)
    if tok:
        try:
            jwt_decode_hs256(
                tok,
                secret=str(settings.auth_jwt_secret or ""),
                issuer=str(settings.auth_jwt_issuer or ""),
            )
            return await call_next(request)
        except AuthError:
            pass

    return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})


def auth_config(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    settings = state.settings
    peers = state.peers or {}
    return {
        "ok": True,
        "version": APP_VERSION,
        "ui_enabled": bool(settings.ui_enabled),
        "auth_enabled": bool(settings.auth_enabled),
        "totp_enabled": bool(settings.auth_totp_enabled),
        "openai_enabled": bool(settings.openai_api_key),
        "fpp_enabled": bool(settings.fpp_base_url),
        "peers_configured": len(peers),
    }


def auth_login(
    req: AuthLoginRequest, response: Response, state: AppState = Depends(get_state)
) -> Dict[str, Any]:
    settings = state.settings
    if not settings.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; login is disabled."
        )
    user = (req.username or "").strip()
    if user != (settings.auth_username or "").strip():
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if not verify_password(req.password, str(settings.auth_password or "")):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if settings.auth_totp_enabled:
        if not totp_verify(
            secret_b32=str(settings.auth_totp_secret or ""), code=str(req.totp or "")
        ):
            raise HTTPException(status_code=401, detail="Invalid TOTP code")

    token = jwt_encode_hs256(
        {"sub": user, "role": "admin"},
        secret=str(settings.auth_jwt_secret or ""),
        ttl_s=int(settings.auth_jwt_ttl_s),
        issuer=str(settings.auth_jwt_issuer or ""),
    )

    response.set_cookie(
        key=str(settings.auth_cookie_name),
        value=token,
        httponly=True,
        secure=bool(settings.auth_cookie_secure),
        samesite="lax",
        max_age=int(settings.auth_jwt_ttl_s),
        path="/",
    )
    return {
        "ok": True,
        "user": {"username": user},
        "token": token,
        "expires_in": int(settings.auth_jwt_ttl_s),
    }


def auth_logout(
    response: Response, state: AppState = Depends(get_state)
) -> Dict[str, Any]:
    settings = state.settings
    if settings.auth_cookie_name:
        response.delete_cookie(key=str(settings.auth_cookie_name), path="/")
    return {"ok": True}


def auth_me(
    request: Request,
    info: Dict[str, Any] = Depends(require_jwt_auth),
    _: AppState = Depends(get_state),
) -> Dict[str, Any]:
    return {
        "ok": True,
        "user": {"username": info["subject"]},
        "expires_at": info["expires_at"],
    }

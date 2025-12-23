from __future__ import annotations

import hashlib
import ipaddress
import secrets
import time
from typing import Any, Dict, Iterable, Tuple

from fastapi import Depends, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from auth import (
    AuthError,
    hash_password_pbkdf2,
    jwt_decode_hs256,
    jwt_encode_hs256,
    totp_generate_secret,
    totp_provisioning_uri,
    totp_verify,
    verify_password,
)
from config.constants import APP_VERSION
from models.requests import (
    AuthApiKeyCreateRequest,
    AuthApiKeyRevokeRequest,
    AuthLoginRequest,
    AuthLoginAttemptClearRequest,
    AuthPasswordChangeRequest,
    AuthPasswordResetCreateRequest,
    AuthPasswordResetRequest,
    AuthSessionRevokeRequest,
    AuthUserCreateRequest,
    AuthUserUpdateRequest,
)
from services.audit_logger import log_event
from services.state import AppState, get_state


def _client_ip(request: Request | None) -> str:
    if request is None:
        return "unknown"
    try:
        if request.client and request.client.host:
            return str(request.client.host)
    except Exception:
        return "unknown"
    return "unknown"


def _normalize_ip_allowlist(raw: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    for item in raw or []:
        val = str(item or "").strip()
        if not val:
            continue
        out.append(val)
    # preserve order, remove duplicates
    deduped: list[str] = []
    seen = set()
    for val in out:
        if val in seen:
            continue
        deduped.append(val)
        seen.add(val)
    return deduped


def _ip_allowed(ip: str, allowlist: Iterable[str] | None) -> bool:
    rules = _normalize_ip_allowlist(allowlist)
    if not rules:
        return True
    ip_val = str(ip or "").strip()
    if not ip_val or ip_val == "unknown":
        return False
    for rule in rules:
        if rule == "*" or rule.lower() in {"any", "all"}:
            return True
        try:
            if "/" in rule:
                net = ipaddress.ip_network(rule, strict=False)
                if ipaddress.ip_address(ip_val) in net:
                    return True
            else:
                if ip_val == rule:
                    return True
        except Exception:
            continue
    return False


def _hash_token(raw: str) -> str:
    val = str(raw or "").strip()
    if not val:
        return ""
    return hashlib.sha256(val.encode("utf-8")).hexdigest()


def _jwt_from_request_with_source(
    request: Request, *, state: AppState
) -> Tuple[str | None, str | None]:
    settings = state.settings
    auth = request.headers.get("authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        tok = parts[1].strip()
        if tok and tok.count(".") == 2:
            return tok, "header"
    if settings.auth_cookie_name:
        tok = request.cookies.get(settings.auth_cookie_name)
        if tok:
            return str(tok).strip(), "cookie"
    return None, None


def _jwt_from_request(request: Request, *, state: AppState) -> str | None:
    tok, _ = _jwt_from_request_with_source(request, state=state)
    return tok


def _api_key_from_request(request: Request) -> str | None:
    candidate = (request.headers.get("x-api-key") or "").strip()
    if candidate:
        return candidate
    auth = request.headers.get("authorization") or ""
    parts = auth.strip().split(None, 1)
    if len(parts) == 2:
        scheme = parts[0].lower()
        token = parts[1].strip()
        if scheme in {"apikey", "api-key"}:
            return token
        if scheme == "bearer" and token.startswith("wsa_") and token.count(".") != 2:
            return token
    return None


def _csrf_header_ok(request: Request, *, state: AppState) -> bool:
    settings = state.settings
    if not settings.auth_csrf_enabled:
        return True
    cookie_name = str(settings.auth_csrf_cookie_name or "").strip() or "wsa_csrf"
    header_name = str(settings.auth_csrf_header_name or "X-CSRF-Token").strip()
    cookie_val = request.cookies.get(cookie_name)
    header_val = request.headers.get(header_name) or request.headers.get(
        header_name.lower()
    )
    if not cookie_val or not header_val:
        return False
    try:
        return secrets.compare_digest(str(cookie_val), str(header_val))
    except Exception:
        return False


VALID_ROLES = {"admin", "user", "viewer"}


def normalize_password_hash(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if value.startswith("pbkdf2_sha256$"):
        return value
    return hash_password_pbkdf2(value)


def _public_user(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "username": row.get("username"),
        "role": row.get("role") or "user",
        "disabled": bool(row.get("disabled")),
        "ip_allowlist": list(row.get("ip_allowlist") or []),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "last_login_at": row.get("last_login_at"),
    }


def _public_api_key(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "username": row.get("username"),
        "label": row.get("label"),
        "prefix": row.get("prefix"),
        "created_at": row.get("created_at"),
        "last_used_at": row.get("last_used_at"),
        "revoked_at": row.get("revoked_at"),
        "expires_at": row.get("expires_at"),
    }


def _public_password_reset(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "username": row.get("username"),
        "created_at": row.get("created_at"),
        "expires_at": row.get("expires_at"),
        "used_at": row.get("used_at"),
        "created_by": row.get("created_by"),
        "used_ip": row.get("used_ip"),
    }


async def _validate_jwt(
    *,
    token: str,
    request: Request | None,
    state: AppState,
) -> Dict[str, Any]:
    settings = state.settings
    claims = jwt_decode_hs256(
        token,
        secret=str(settings.auth_jwt_secret or ""),
        issuer=str(settings.auth_jwt_issuer or ""),
    )
    raw = dict(claims.raw or {})
    jti = str(raw.get("jti") or "").strip()
    sub = claims.subject
    role = str(raw.get("role") or "user").strip() or "user"
    ip = _client_ip(request)

    db = getattr(state, "db", None)
    if db is not None:
        if not jti:
            raise HTTPException(status_code=401, detail="Session missing")
        sess = await db.get_auth_session(jti)
        if not sess:
            raise HTTPException(status_code=401, detail="Session revoked")
        if sess.get("revoked_at"):
            raise HTTPException(status_code=401, detail="Session revoked")
        exp = float(sess.get("expires_at") or 0.0)
        if exp and exp <= time.time():
            raise HTTPException(status_code=401, detail="Session expired")
        try:
            await db.touch_auth_session(
                jti, min_interval_s=float(settings.auth_session_touch_interval_s)
            )
        except Exception:
            pass
        try:
            user_row = await db.get_auth_user(str(sub or ""))
        except Exception:
            user_row = None
        if not user_row:
            raise HTTPException(status_code=401, detail="User not found")
        if bool(user_row.get("disabled")):
            raise HTTPException(status_code=403, detail="User disabled")
        if not _ip_allowed(ip, user_row.get("ip_allowlist")):
            raise HTTPException(status_code=403, detail="IP not allowed")
        role = str(user_row.get("role") or role or "user").strip() or "user"

    return {
        "subject": sub,
        "expires_at": claims.expires_at,
        "issued_at": claims.issued_at,
        "claims": raw,
        "role": role,
        "jti": jti or None,
    }


async def _validate_api_key(
    *,
    token: str,
    request: Request | None,
    state: AppState,
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    key_hash = _hash_token(token)
    if not key_hash:
        raise HTTPException(status_code=401, detail="Missing API key")
    row = await db.get_auth_api_key_by_hash(key_hash)
    if not row:
        raise HTTPException(status_code=401, detail="Invalid API key")
    if row.get("revoked_at"):
        raise HTTPException(status_code=401, detail="API key revoked")
    exp = float(row.get("expires_at") or 0.0)
    if exp and exp <= time.time():
        raise HTTPException(status_code=401, detail="API key expired")
    username = str(row.get("username") or "")
    user_row = await db.get_auth_user(username)
    if not user_row:
        raise HTTPException(status_code=401, detail="User not found")
    if bool(user_row.get("disabled")):
        raise HTTPException(status_code=403, detail="User disabled")
    if not _ip_allowed(_client_ip(request), user_row.get("ip_allowlist")):
        raise HTTPException(status_code=403, detail="IP not allowed")
    try:
        await db.touch_auth_api_key(int(row.get("id")))
    except Exception:
        pass
    role = str(user_row.get("role") or "user").strip() or "user"
    return {
        "subject": username,
        "role": role,
        "api_key_id": row.get("id"),
    }


async def require_jwt_auth(
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
        return await _validate_jwt(token=tok, request=request, state=state)
    except AuthError as e:
        raise HTTPException(status_code=401, detail=str(e))


async def require_a2a_auth(
    request: Request,
    x_a2a_key: str | None = Header(default=None, alias="X-A2A-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    state: AppState = Depends(get_state),
) -> None:
    settings = state.settings
    key = settings.a2a_api_key
    api_key = _api_key_from_request(request)
    candidate = x_a2a_key
    if (not candidate) and authorization:
        parts = str(authorization).strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            candidate = parts[1].strip()

    # If JWT auth is enabled, allow either a valid A2A key (if set) or a valid JWT.
    if settings.auth_enabled:
        if api_key:
            info = await _validate_api_key(token=api_key, request=request, state=state)
            request.state.user = info.get("subject") or "user"
            request.state.role = info.get("role") or "user"
            request.state.api_key_id = info.get("api_key_id")
            return
        if key and candidate == key:
            return
        tok = _jwt_from_request(request, state=state)
        if not tok:
            raise HTTPException(status_code=401, detail="Missing token")
        try:
            info = await _validate_jwt(token=tok, request=request, state=state)
            request.state.user = info.get("subject") or "user"
            request.state.role = info.get("role") or "user"
            request.state.session_id = info.get("jti")
            return
        except AuthError as e:
            raise HTTPException(status_code=401, detail=str(e))

    # Legacy mode: only enforce A2A key if configured.
    if not key:
        return
    if candidate != key:
        raise HTTPException(status_code=401, detail="Missing or invalid A2A key")


async def require_admin(
    info: Dict[str, Any] = Depends(require_jwt_auth),
) -> Dict[str, Any]:
    role = str(info.get("role") or "user")
    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return info


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
        or path == "/livez"
        or path == "/readyz"
        or path.startswith("/ui")
        or path.startswith("/v1/health")
        or path.startswith("/v1/auth/config")
        or path.startswith("/v1/auth/login")
        or path.startswith("/v1/auth/logout")
        or path == "/v1/auth/password/reset"
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
            request.state.user = "a2a"
            request.state.role = "a2a"
            return await call_next(request)
        auth = request.headers.get("authorization") or ""
        parts = auth.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip() == key:
            request.state.user = "a2a"
            request.state.role = "a2a"
            return await call_next(request)

    api_key = _api_key_from_request(request)
    if api_key:
        try:
            info = await _validate_api_key(token=api_key, request=request, state=st)
            role = info.get("role") or "user"
            request.state.user = info.get("subject") or "user"
            request.state.role = role
            request.state.api_key_id = info.get("api_key_id")
            if role == "viewer" and request.method.upper() not in {
                "GET",
                "HEAD",
                "OPTIONS",
            }:
                if request.url.path == "/v1/auth/password/change":
                    return await call_next(request)
                return JSONResponse(
                    status_code=403,
                    content={"ok": False, "error": "read_only"},
                )
            return await call_next(request)
        except HTTPException as e:
            return JSONResponse(
                status_code=int(getattr(e, "status_code", 401) or 401),
                content={"ok": False, "error": str(getattr(e, "detail", "unauthorized"))},
            )

    tok, tok_source = _jwt_from_request_with_source(request, state=st)
    if tok:
        try:
            info = await _validate_jwt(token=tok, request=request, state=st)
            role = info.get("role") or "user"
            request.state.user = info.get("subject") or "user"
            request.state.role = role
            request.state.session_id = info.get("jti")
            if (
                tok_source == "cookie"
                and request.method.upper() not in {"GET", "HEAD", "OPTIONS"}
                and not _csrf_header_ok(request, state=st)
            ):
                return JSONResponse(
                    status_code=403,
                    content={"ok": False, "error": "csrf_required"},
                )
            if role == "viewer" and request.method.upper() not in {
                "GET",
                "HEAD",
                "OPTIONS",
            }:
                return JSONResponse(
                    status_code=403,
                    content={"ok": False, "error": "read_only"},
                )
            return await call_next(request)
        except HTTPException as e:
            return JSONResponse(
                status_code=int(getattr(e, "status_code", 401) or 401),
                content={"ok": False, "error": str(getattr(e, "detail", "unauthorized"))},
            )
        except AuthError:
            pass

    return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})


async def auth_config(state: AppState = Depends(get_state)) -> Dict[str, Any]:
    settings = state.settings
    peers = state.peers or {}
    return {
        "ok": True,
        "version": APP_VERSION,
        "ui_enabled": bool(settings.ui_enabled),
        "auth_enabled": bool(settings.auth_enabled),
        "totp_enabled": bool(settings.auth_totp_enabled),
        "csrf_enabled": bool(settings.auth_csrf_enabled),
        "csrf_cookie_name": str(settings.auth_csrf_cookie_name or ""),
        "csrf_header_name": str(settings.auth_csrf_header_name or ""),
        "roles": sorted(list(VALID_ROLES)),
        "openai_enabled": bool(settings.openai_api_key),
        "fpp_enabled": bool(settings.fpp_base_url),
        "ledfx_enabled": bool(settings.ledfx_base_url),
        "mqtt_enabled": bool(settings.mqtt_enabled and settings.mqtt_url),
        "peers_configured": len(peers),
    }


async def auth_login(
    req: AuthLoginRequest,
    response: Response,
    request: Request,
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    settings = state.settings
    if not settings.auth_enabled:
        raise HTTPException(
            status_code=400, detail="AUTH_ENABLED is false; login is disabled."
        )
    user = (req.username or "").strip()
    if not user:
        raise HTTPException(status_code=400, detail="Username is required")

    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")

    ip = _client_ip(request)
    now = time.time()

    lock_state = await db.get_auth_login_state(
        username=user,
        ip=ip,
        window_s=float(settings.auth_login_window_s),
    )
    locked_until = float(lock_state.get("locked_until") or 0.0) if lock_state else 0.0
    if locked_until > now:
        await log_event(
            state,
            action="auth.login",
            actor=user or "unknown",
            ok=False,
            error="locked_out",
            request=request,
        )
        raise HTTPException(
            status_code=429,
            detail="Too many failed attempts. Try again later.",
        )

    async def _record_failure(reason: str) -> None:
        state_rec = await db.record_auth_login_failure(
            username=user,
            ip=ip,
            max_attempts=int(settings.auth_login_max_attempts),
            window_s=float(settings.auth_login_window_s),
            lockout_s=float(settings.auth_login_lockout_s),
        )
        locked = float(state_rec.get("locked_until") or 0.0) > time.time()
        await log_event(
            state,
            action="auth.login",
            actor=user or "unknown",
            ok=False,
            error=reason,
            request=request,
        )
        if locked:
            raise HTTPException(
                status_code=429,
                detail="Too many failed attempts. Try again later.",
            )
        raise HTTPException(status_code=401, detail="Invalid username or password")

    row = await db.get_auth_user(user)
    if not row or bool(row.get("disabled")):
        await _record_failure("invalid_credentials")
    pwd_hash = str(row.get("password_hash") or "")
    if not verify_password(req.password, pwd_hash):
        await _record_failure("invalid_credentials")
    if settings.auth_totp_enabled:
        if not totp_verify(
            secret_b32=str(row.get("totp_secret") or ""),
            code=str(req.totp or ""),
        ):
            await _record_failure("invalid_totp")

    if not _ip_allowed(ip, row.get("ip_allowlist")):
        await log_event(
            state,
            action="auth.login",
            actor=user or "unknown",
            ok=False,
            error="ip_not_allowed",
            request=request,
        )
        raise HTTPException(status_code=403, detail="IP not allowed")

    await db.clear_auth_login_attempts(username=user, ip=ip)
    await db.touch_auth_user_login(user)

    role = str(row.get("role") or "user") or "user"
    jti = secrets.token_hex(16)
    token = jwt_encode_hs256(
        {"sub": user, "role": role, "jti": jti},
        secret=str(settings.auth_jwt_secret or ""),
        ttl_s=int(settings.auth_jwt_ttl_s),
        issuer=str(settings.auth_jwt_issuer or ""),
    )
    expires_at = now + float(settings.auth_jwt_ttl_s)
    await db.create_auth_session(
        jti=jti,
        username=user,
        expires_at=expires_at,
        ip=ip,
        user_agent=request.headers.get("user-agent"),
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
    csrf_token = None
    if settings.auth_csrf_enabled:
        csrf_token = secrets.token_urlsafe(32)
        response.set_cookie(
            key=str(settings.auth_csrf_cookie_name or "wsa_csrf"),
            value=csrf_token,
            httponly=False,
            secure=bool(settings.auth_cookie_secure),
            samesite="lax",
            max_age=int(settings.auth_jwt_ttl_s),
            path="/",
        )
    await log_event(
        state,
        action="auth.login",
        actor=user or "unknown",
        ok=True,
        request=request,
        payload={"totp": bool(req.totp), "role": role},
    )
    return {
        "ok": True,
        "user": {"username": user, "role": role},
        "token": token,
        "expires_in": int(settings.auth_jwt_ttl_s),
        "csrf_token": csrf_token,
    }


async def auth_logout(
    response: Response,
    request: Request,
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    settings = state.settings
    tok = _jwt_from_request(request, state=state)
    if tok:
        try:
            info = await _validate_jwt(token=tok, request=request, state=state)
            jti = info.get("jti")
            if jti and getattr(state, "db", None) is not None:
                try:
                    await state.db.revoke_auth_session(str(jti))
                except Exception:
                    pass
        except Exception:
            pass
    if settings.auth_cookie_name:
        response.delete_cookie(key=str(settings.auth_cookie_name), path="/")
    if settings.auth_csrf_cookie_name:
        response.delete_cookie(key=str(settings.auth_csrf_cookie_name), path="/")
    await log_event(state, action="auth.logout", ok=True, request=request)
    return {"ok": True}


async def auth_me(
    request: Request,
    info: Dict[str, Any] = Depends(require_jwt_auth),
    _: AppState = Depends(get_state),
) -> Dict[str, Any]:
    return {
        "ok": True,
        "user": {"username": info["subject"], "role": info.get("role") or "user"},
        "expires_at": info["expires_at"],
        "session_id": info.get("jti"),
    }


async def auth_users(
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    rows = await db.list_auth_users()
    await log_event(
        state,
        action="auth.users.list",
        ok=True,
        payload={"count": len(rows)},
        request=request,
    )
    return {"ok": True, "users": [_public_user(r) for r in rows]}


async def auth_user_create(
    req: AuthUserCreateRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    username = str(req.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")
    password_hash = normalize_password_hash(req.password)
    if not password_hash:
        raise HTTPException(status_code=400, detail="password is required")
    totp_secret = str(req.totp_secret or "").strip().replace(" ", "")
    if not totp_secret:
        totp_secret = totp_generate_secret()
    role = str(req.role or "user").strip() or "user"
    if role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    ip_allowlist = _normalize_ip_allowlist(req.ip_allowlist)
    try:
        await db.create_auth_user(
            username=username,
            password_hash=password_hash,
            totp_secret=totp_secret,
            role=role,
            disabled=bool(req.disabled),
            ip_allowlist=ip_allowlist,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    row = await db.get_auth_user(username)
    await log_event(
        state,
        action="auth.user.create",
        actor=username,
        ok=True,
        request=request,
        payload={"role": role},
    )
    return {
        "ok": True,
        "user": _public_user(row or {"username": username, "role": role}),
        "totp_secret": totp_secret,
        "provisioning_uri": totp_provisioning_uri(
            issuer=str(state.settings.auth_totp_issuer),
            account=username,
            secret_b32=totp_secret,
        ),
    }


async def auth_user_update(
    username: str,
    req: AuthUserUpdateRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    uname = str(username or "").strip()
    if not uname:
        raise HTTPException(status_code=400, detail="username is required")
    prev = await db.get_auth_user(uname)
    totp_secret = None
    if req.regenerate_totp:
        totp_secret = totp_generate_secret()
    elif req.totp_secret is not None:
        totp_secret = str(req.totp_secret or "").strip().replace(" ", "")

    password_hash = None
    if req.password is not None:
        password_hash = normalize_password_hash(req.password)
        if not password_hash:
            raise HTTPException(status_code=400, detail="password is required")

    role = req.role
    if role is not None:
        role = str(role or "").strip() or "user"
        if role not in VALID_ROLES:
            raise HTTPException(status_code=400, detail="Invalid role")

    ip_allowlist = None
    if req.ip_allowlist is not None:
        ip_allowlist = _normalize_ip_allowlist(req.ip_allowlist)

    try:
        await db.update_auth_user(
            username=uname,
            password_hash=password_hash,
            totp_secret=totp_secret,
            role=role,
            disabled=req.disabled,
            ip_allowlist=ip_allowlist,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    row = await db.get_auth_user(uname)
    if prev:
        should_revoke = False
        if password_hash is not None:
            should_revoke = True
        if req.disabled is True and not bool(prev.get("disabled")):
            should_revoke = True
        if role is not None and str(prev.get("role") or "") != str(role):
            should_revoke = True
        if ip_allowlist is not None and list(prev.get("ip_allowlist") or []) != list(
            ip_allowlist
        ):
            should_revoke = True
        if should_revoke:
            try:
                await db.revoke_auth_sessions_for_user(uname)
            except Exception:
                pass
            try:
                await db.revoke_auth_api_keys_for_user(uname)
            except Exception:
                pass
    await log_event(
        state,
        action="auth.user.update",
        actor=uname,
        ok=True,
        request=request,
        payload={"role": role, "disabled": req.disabled},
    )
    resp: Dict[str, Any] = {"ok": True, "user": _public_user(row or {"username": uname})}
    if totp_secret:
        resp["totp_secret"] = totp_secret
        resp["provisioning_uri"] = totp_provisioning_uri(
            issuer=str(state.settings.auth_totp_issuer),
            account=uname,
            secret_b32=totp_secret,
        )
    return resp


async def auth_user_delete(
    username: str,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    uname = str(username or "").strip()
    if not uname:
        raise HTTPException(status_code=400, detail="username is required")
    rows = await db.list_auth_users()
    admins = [r for r in rows if str(r.get("role") or "") == "admin" and not r.get("disabled")]
    if any(r.get("username") == uname for r in admins) and len(admins) <= 1:
        raise HTTPException(status_code=400, detail="Cannot delete last admin user")
    try:
        await db.revoke_auth_sessions_for_user(uname)
    except Exception:
        pass
    try:
        await db.revoke_auth_api_keys_for_user(uname)
    except Exception:
        pass
    await db.delete_auth_user(uname)
    await log_event(
        state,
        action="auth.user.delete",
        actor=uname,
        ok=True,
        request=request,
    )
    return {"ok": True}


async def auth_sessions(
    username: str | None = None,
    active_only: bool = False,
    limit: int = 200,
    offset: int = 0,
    request: Request | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    rows = await db.list_auth_sessions(
        username=username,
        active_only=bool(active_only),
        limit=int(limit),
        offset=int(offset),
    )
    await log_event(
        state,
        action="auth.sessions.list",
        ok=True,
        payload={"count": len(rows), "username": username, "active_only": active_only},
        request=request,
    )
    next_offset = int(offset) + len(rows) if len(rows) >= int(limit) else None
    return {
        "ok": True,
        "sessions": rows,
        "count": len(rows),
        "limit": int(limit),
        "offset": int(offset),
        "next_offset": next_offset,
    }


async def auth_sessions_revoke(
    req: AuthSessionRevokeRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    revoked = 0
    if req.jti:
        await db.revoke_auth_session(str(req.jti))
        revoked = 1
    elif req.username:
        revoked = await db.revoke_auth_sessions_for_user(str(req.username))
    else:
        raise HTTPException(status_code=400, detail="Provide jti or username")
    await log_event(
        state,
        action="auth.session.revoke",
        actor=str(req.username or req.jti or "unknown"),
        ok=True,
        request=request,
        payload={"count": revoked},
    )
    return {"ok": True, "revoked": revoked}


async def auth_login_attempts(
    username: str | None = None,
    ip: str | None = None,
    locked_only: bool = False,
    limit: int = 200,
    offset: int = 0,
    request: Request | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    rows = await db.list_auth_login_attempts(
        username=username,
        ip=ip,
        locked_only=bool(locked_only),
        limit=int(limit),
        offset=int(offset),
    )
    await log_event(
        state,
        action="auth.login_attempts.list",
        ok=True,
        payload={
            "count": len(rows),
            "username": username,
            "ip": ip,
            "locked_only": locked_only,
        },
        request=request,
    )
    next_offset = int(offset) + len(rows) if len(rows) >= int(limit) else None
    return {
        "ok": True,
        "attempts": rows,
        "count": len(rows),
        "limit": int(limit),
        "offset": int(offset),
        "next_offset": next_offset,
    }


async def auth_login_attempts_clear(
    req: AuthLoginAttemptClearRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    if not req.username and not req.ip and not req.all:
        raise HTTPException(
            status_code=400, detail="Provide username, ip, or set all=true"
        )
    deleted = await db.clear_auth_login_attempts_bulk(
        username=req.username if not req.all else None,
        ip=req.ip if not req.all else None,
    )
    await log_event(
        state,
        action="auth.login_attempts.clear",
        ok=True,
        payload={"deleted": deleted, "username": req.username, "ip": req.ip},
        request=request,
    )
    return {"ok": True, "deleted": deleted}


async def auth_api_keys(
    username: str | None = None,
    active_only: bool = False,
    limit: int = 200,
    offset: int = 0,
    request: Request | None = None,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    rows = await db.list_auth_api_keys(
        username=username,
        active_only=bool(active_only),
        limit=int(limit),
        offset=int(offset),
    )
    public_rows = [_public_api_key(r) for r in rows]
    await log_event(
        state,
        action="auth.api_keys.list",
        ok=True,
        payload={
            "count": len(public_rows),
            "username": username,
            "active_only": active_only,
        },
        request=request,
    )
    next_offset = int(offset) + len(rows) if len(rows) >= int(limit) else None
    return {
        "ok": True,
        "api_keys": public_rows,
        "count": len(public_rows),
        "limit": int(limit),
        "offset": int(offset),
        "next_offset": next_offset,
    }


async def auth_api_key_create(
    req: AuthApiKeyCreateRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    username = str(req.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")
    user_row = await db.get_auth_user(username)
    if not user_row:
        raise HTTPException(status_code=404, detail="User not found")
    if bool(user_row.get("disabled")):
        raise HTTPException(status_code=400, detail="User is disabled")
    token = f"wsa_{secrets.token_urlsafe(32)}"
    key_hash = _hash_token(token)
    expires_at = None
    if req.expires_in_s:
        expires_at = time.time() + float(req.expires_in_s)
    record = await db.create_auth_api_key(
        username=username,
        key_hash=key_hash,
        label=req.label,
        prefix=token[:8],
        expires_at=expires_at,
    )
    await log_event(
        state,
        action="auth.api_keys.create",
        actor=username,
        ok=True,
        payload={"label": req.label, "expires_at": expires_at},
        request=request,
    )
    return {"ok": True, "api_key": token, "record": _public_api_key(record)}


async def auth_api_key_revoke(
    req: AuthApiKeyRevokeRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    revoked = 0
    if req.id:
        await db.revoke_auth_api_key(int(req.id))
        revoked = 1
    elif req.username:
        revoked = await db.revoke_auth_api_keys_for_user(str(req.username))
    else:
        raise HTTPException(status_code=400, detail="Provide id or username")
    await log_event(
        state,
        action="auth.api_keys.revoke",
        actor=str(req.username or req.id or "unknown"),
        ok=True,
        payload={"count": revoked},
        request=request,
    )
    return {"ok": True, "revoked": revoked}


async def auth_password_change(
    req: AuthPasswordChangeRequest,
    request: Request,
    info: Dict[str, Any] = Depends(require_jwt_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    username = str(info.get("subject") or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="Invalid session")
    row = await db.get_auth_user(username)
    if not row or bool(row.get("disabled")):
        raise HTTPException(status_code=403, detail="User disabled")
    if not verify_password(req.current_password, str(row.get("password_hash") or "")):
        raise HTTPException(status_code=401, detail="Invalid password")
    if state.settings.auth_totp_enabled:
        if not totp_verify(
            secret_b32=str(row.get("totp_secret") or ""),
            code=str(req.totp or ""),
        ):
            raise HTTPException(status_code=401, detail="Invalid TOTP")
    new_hash = normalize_password_hash(req.new_password)
    if not new_hash:
        raise HTTPException(status_code=400, detail="new_password is required")
    await db.update_auth_user(username=username, password_hash=new_hash)
    if req.revoke_sessions:
        try:
            await db.revoke_auth_sessions_for_user(
                username, skip_jti=str(info.get("jti") or "")
            )
        except Exception:
            pass
    if req.revoke_api_keys:
        try:
            await db.revoke_auth_api_keys_for_user(username)
        except Exception:
            pass
    await log_event(
        state,
        action="auth.password.change",
        actor=username,
        ok=True,
        request=request,
        payload={"revoke_sessions": req.revoke_sessions},
    )
    return {"ok": True}


async def auth_password_reset_request(
    req: AuthPasswordResetCreateRequest,
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    username = str(req.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username is required")
    row = await db.get_auth_user(username)
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    token = secrets.token_urlsafe(32)
    token_hash = _hash_token(token)
    ttl_s = float(req.ttl_s or 3600)
    expires_at = time.time() + max(60.0, ttl_s)
    record = await db.create_auth_password_reset(
        username=username,
        token_hash=token_hash,
        expires_at=expires_at,
        created_by=str(getattr(request.state, "user", "") or ""),
    )
    await log_event(
        state,
        action="auth.password.reset.request",
        actor=username,
        ok=True,
        request=request,
        payload={"expires_at": expires_at},
    )
    return {"ok": True, "token": token, "record": _public_password_reset(record)}


async def auth_password_reset(
    req: AuthPasswordResetRequest,
    request: Request,
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    db = getattr(state, "db", None)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    token_hash = _hash_token(req.token)
    if not token_hash:
        raise HTTPException(status_code=400, detail="token is required")
    record = await db.get_auth_password_reset_by_hash(token_hash)
    if not record:
        raise HTTPException(status_code=400, detail="Invalid or expired token")
    if record.get("used_at"):
        raise HTTPException(status_code=400, detail="Token already used")
    if float(record.get("expires_at") or 0.0) <= time.time():
        raise HTTPException(status_code=400, detail="Token expired")
    username = str(record.get("username") or "")
    if not username:
        raise HTTPException(status_code=400, detail="Invalid token")
    new_hash = normalize_password_hash(req.new_password)
    if not new_hash:
        raise HTTPException(status_code=400, detail="new_password is required")
    totp_secret = None
    if req.rotate_totp:
        totp_secret = totp_generate_secret()
    await db.update_auth_user(
        username=username,
        password_hash=new_hash,
        totp_secret=totp_secret,
    )
    await db.mark_auth_password_reset_used(
        int(record.get("id") or 0), used_ip=_client_ip(request)
    )
    try:
        await db.revoke_auth_sessions_for_user(username)
    except Exception:
        pass
    try:
        await db.revoke_auth_api_keys_for_user(username)
    except Exception:
        pass
    await log_event(
        state,
        action="auth.password.reset",
        actor=username,
        ok=True,
        request=request,
    )
    resp: Dict[str, Any] = {"ok": True}
    if totp_secret:
        resp["totp_secret"] = totp_secret
        resp["provisioning_uri"] = totp_provisioning_uri(
            issuer=str(state.settings.auth_totp_issuer),
            account=username,
            secret_b32=totp_secret,
        )
    return resp

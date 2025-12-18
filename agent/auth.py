from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


class AuthError(RuntimeError):
    pass


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    s = (data or "").strip()
    pad = "=" * (-len(s) % 4)
    try:
        return base64.urlsafe_b64decode(s + pad)
    except Exception as e:
        raise AuthError(f"Invalid base64url: {e}")


def jwt_sign_hs256(message: bytes, secret: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).digest()
    return _b64url_encode(mac)


def jwt_encode_hs256(
    payload: Dict[str, Any], *, secret: str, ttl_s: int, issuer: Optional[str] = None
) -> str:
    now = int(time.time())
    ttl_s = max(10, int(ttl_s))

    header = {"typ": "JWT", "alg": "HS256"}
    body: Dict[str, Any] = dict(payload or {})
    body.setdefault("iat", now)
    body.setdefault("exp", now + ttl_s)
    if issuer:
        body.setdefault("iss", str(issuer))

    h = _b64url_encode(
        json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    p = _b64url_encode(
        json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    sig = jwt_sign_hs256(f"{h}.{p}".encode("ascii"), secret)
    return f"{h}.{p}.{sig}"


@dataclass(frozen=True)
class JWTClaims:
    subject: str
    issued_at: int
    expires_at: int
    raw: Dict[str, Any]


def jwt_decode_hs256(
    token: str,
    *,
    secret: str,
    issuer: Optional[str] = None,
    leeway_s: int = 15,
) -> JWTClaims:
    t = (token or "").strip()
    parts = t.split(".")
    if len(parts) != 3:
        raise AuthError("Token is not a JWT")
    h_b64, p_b64, sig_b64 = parts

    header_raw = _b64url_decode(h_b64)
    payload_raw = _b64url_decode(p_b64)
    try:
        header = json.loads(header_raw.decode("utf-8"))
        payload = json.loads(payload_raw.decode("utf-8"))
    except Exception as e:
        raise AuthError(f"Invalid JWT JSON: {e}")

    if not isinstance(header, dict) or header.get("alg") != "HS256":
        raise AuthError("Unsupported JWT alg")
    if not isinstance(payload, dict):
        raise AuthError("Invalid JWT payload")

    expected = jwt_sign_hs256(f"{h_b64}.{p_b64}".encode("ascii"), secret)
    if not hmac.compare_digest(str(sig_b64), str(expected)):
        raise AuthError("Invalid JWT signature")

    now = int(time.time())
    try:
        exp = int(payload.get("exp"))
        iat = int(payload.get("iat", 0))
    except Exception:
        raise AuthError("Invalid exp/iat claim")

    if exp <= now - int(leeway_s):
        raise AuthError("JWT expired")
    if issuer and str(payload.get("iss") or "") != str(issuer):
        raise AuthError("Invalid JWT issuer")

    sub = str(payload.get("sub") or "").strip()
    if not sub:
        raise AuthError("Missing sub claim")

    return JWTClaims(subject=sub, issued_at=iat, expires_at=exp, raw=payload)


def verify_password(supplied: str, expected: str) -> bool:
    """
    Verify a supplied password against an expected password string.

    Supported formats:
    - Plain string (recommended only for LAN deployments).
    - pbkdf2_sha256$<iterations>$<salt_b64>$<hash_b64> (recommended).
    """
    supplied_s = str(supplied or "")
    expected_s = str(expected or "")

    if expected_s.startswith("pbkdf2_sha256$"):
        parts = expected_s.split("$")
        if len(parts) != 4:
            return False
        _, it_s, salt_b64, hash_b64 = parts
        try:
            iterations = int(it_s)
            salt = _b64url_decode(salt_b64)
            expected_hash = _b64url_decode(hash_b64)
        except Exception:
            return False
        dk = hashlib.pbkdf2_hmac(
            "sha256",
            supplied_s.encode("utf-8"),
            salt,
            iterations,
            dklen=len(expected_hash),
        )
        return hmac.compare_digest(dk, expected_hash)

    return hmac.compare_digest(supplied_s, expected_s)


def hash_password_pbkdf2(
    password: str, *, iterations: int = 210_000, salt_bytes: int = 16
) -> str:
    salt = secrets.token_bytes(max(8, int(salt_bytes)))
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt,
        max(10_000, int(iterations)),
        dklen=32,
    )
    return f"pbkdf2_sha256${max(10_000, int(iterations))}${_b64url_encode(salt)}${_b64url_encode(dk)}"


def _b32_decode(data: str) -> bytes:
    s = (data or "").strip().replace(" ", "").upper()
    if not s:
        raise AuthError("Missing TOTP secret")
    pad = "=" * (-len(s) % 8)
    try:
        return base64.b32decode(s + pad, casefold=True)
    except Exception as e:
        raise AuthError(f"Invalid base32 TOTP secret: {e}")


def totp_generate_secret(*, bytes_len: int = 20) -> str:
    raw = secrets.token_bytes(max(10, int(bytes_len)))
    return base64.b32encode(raw).decode("ascii").rstrip("=")


def totp_code(
    *, secret_b32: str, at_time: Optional[int] = None, step_s: int = 30, digits: int = 6
) -> str:
    step = max(5, int(step_s))
    digs = max(6, min(10, int(digits)))
    t = int(time.time()) if at_time is None else int(at_time)

    key = _b32_decode(secret_b32)
    counter = int(t // step)
    msg = counter.to_bytes(8, "big")
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    off = digest[-1] & 0x0F
    code_int = (int.from_bytes(digest[off : off + 4], "big") & 0x7FFFFFFF) % (10**digs)
    return str(code_int).zfill(digs)


def totp_verify(
    *,
    secret_b32: str,
    code: str,
    at_time: Optional[int] = None,
    step_s: int = 30,
    digits: int = 6,
    window_steps: int = 1,
) -> bool:
    supplied = "".join([c for c in str(code or "").strip() if c.isdigit()])
    if not supplied:
        return False
    t = int(time.time()) if at_time is None else int(at_time)
    win = max(0, int(window_steps))
    step = max(5, int(step_s))
    digs = max(6, min(10, int(digits)))
    if len(supplied) != digs:
        return False

    for w in range(-win, win + 1):
        expected = totp_code(
            secret_b32=secret_b32, at_time=t + (w * step), step_s=step, digits=digs
        )
        if hmac.compare_digest(supplied, expected):
            return True
    return False


def totp_provisioning_uri(
    *, issuer: str, account: str, secret_b32: str, digits: int = 6, period_s: int = 30
) -> str:
    iss = (issuer or "").strip() or "wled-show-agent"
    acct = (account or "").strip() or "admin"
    secret = (secret_b32 or "").strip().replace(" ", "")
    digs = max(6, min(10, int(digits)))
    per = max(5, int(period_s))
    label = f"{iss}:{acct}"
    # Basic otpauth URI (RFC 6238 / Google Authenticator conventions)
    from urllib.parse import quote

    return (
        "otpauth://totp/"
        + quote(label)
        + f"?secret={quote(secret)}&issuer={quote(iss)}&algorithm=SHA1&digits={digs}&period={per}"
    )

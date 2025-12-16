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


def jwt_encode_hs256(payload: Dict[str, Any], *, secret: str, ttl_s: int, issuer: Optional[str] = None) -> str:
    now = int(time.time())
    ttl_s = max(10, int(ttl_s))

    header = {"typ": "JWT", "alg": "HS256"}
    body: Dict[str, Any] = dict(payload or {})
    body.setdefault("iat", now)
    body.setdefault("exp", now + ttl_s)
    if issuer:
        body.setdefault("iss", str(issuer))

    h = _b64url_encode(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    p = _b64url_encode(json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8"))
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
        dk = hashlib.pbkdf2_hmac("sha256", supplied_s.encode("utf-8"), salt, iterations, dklen=len(expected_hash))
        return hmac.compare_digest(dk, expected_hash)

    return hmac.compare_digest(supplied_s, expected_s)


def hash_password_pbkdf2(password: str, *, iterations: int = 210_000, salt_bytes: int = 16) -> str:
    salt = secrets.token_bytes(max(8, int(salt_bytes)))
    dk = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, max(10_000, int(iterations)), dklen=32)
    return f"pbkdf2_sha256${max(10_000, int(iterations))}${_b64url_encode(salt)}${_b64url_encode(dk)}"


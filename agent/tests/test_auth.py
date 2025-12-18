from __future__ import annotations

import pytest

import auth


def test_jwt_round_trip() -> None:
    token = auth.jwt_encode_hs256(
        {"sub": "admin"}, secret="secret", ttl_s=60, issuer="issuer"
    )
    claims = auth.jwt_decode_hs256(token, secret="secret", issuer="issuer")
    assert claims.subject == "admin"
    assert claims.expires_at > claims.issued_at


def test_jwt_expired(monkeypatch) -> None:
    monkeypatch.setattr(auth.time, "time", lambda: 1000)
    token = auth.jwt_encode_hs256(
        {"sub": "admin"}, secret="secret", ttl_s=60, issuer="issuer"
    )

    monkeypatch.setattr(auth.time, "time", lambda: 2000)
    with pytest.raises(auth.AuthError):
        auth.jwt_decode_hs256(token, secret="secret", issuer="issuer", leeway_s=0)


def test_verify_password_plain() -> None:
    assert auth.verify_password("pw", "pw") is True
    assert auth.verify_password("pw", "nope") is False


def test_verify_password_pbkdf2() -> None:
    hashed = auth.hash_password_pbkdf2("pw", iterations=10_000, salt_bytes=16)
    assert hashed.startswith("pbkdf2_sha256$")
    assert auth.verify_password("pw", hashed) is True
    assert auth.verify_password("nope", hashed) is False


def test_totp_round_trip(monkeypatch) -> None:
    secret = auth.totp_generate_secret(bytes_len=20)
    # Freeze time so we have deterministic codes.
    monkeypatch.setattr(auth.time, "time", lambda: 1_700_000_000)
    code = auth.totp_code(secret_b32=secret)
    assert auth.totp_verify(secret_b32=secret, code=code) is True
    assert auth.totp_verify(secret_b32=secret, code="000000") is False

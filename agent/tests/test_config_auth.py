from __future__ import annotations

import pytest

from config import load_settings


def test_auth_disabled_by_default(monkeypatch) -> None:
    monkeypatch.setenv("WLED_TREE_URL", "http://172.16.200.50")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    s = load_settings()
    assert s.auth_enabled is False
    assert s.ui_enabled is True


def test_auth_enabled_requires_password_and_secret(monkeypatch) -> None:
    monkeypatch.setenv("WLED_TREE_URL", "http://172.16.200.50")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AUTH_ENABLED", "true")

    with pytest.raises(RuntimeError):
        load_settings()

    monkeypatch.setenv("AUTH_PASSWORD", "pw")
    with pytest.raises(RuntimeError):
        load_settings()

    monkeypatch.setenv("AUTH_JWT_SECRET", "secret")
    s = load_settings()
    assert s.auth_enabled is True
    assert s.auth_username == "admin"


def test_totp_requires_secret(monkeypatch) -> None:
    monkeypatch.setenv("WLED_TREE_URL", "http://172.16.200.50")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("AUTH_PASSWORD", "pw")
    monkeypatch.setenv("AUTH_JWT_SECRET", "secret")
    monkeypatch.setenv("AUTH_TOTP_ENABLED", "true")

    with pytest.raises(RuntimeError):
        load_settings()

    monkeypatch.setenv("AUTH_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    s = load_settings()
    assert s.auth_totp_enabled is True

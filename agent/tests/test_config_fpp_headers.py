from __future__ import annotations

from config import load_settings


def test_config_parses_fpp_headers_json(monkeypatch) -> None:
    monkeypatch.setenv("WLED_TREE_URL", "http://172.16.200.50")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("AUTH_PASSWORD", "pw")
    monkeypatch.setenv("AUTH_JWT_SECRET", "secret")
    monkeypatch.setenv("AUTH_TOTP_ENABLED", "true")
    monkeypatch.setenv("AUTH_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    monkeypatch.setenv("FPP_BASE_URL", "http://172.16.200.20")
    monkeypatch.setenv(
        "FPP_HEADERS_JSON", '{"Authorization":"Bearer token","X-Test":"1"}'
    )

    s = load_settings()
    assert s.fpp_base_url == "http://172.16.200.20"
    assert ("Authorization", "Bearer token") in s.fpp_headers
    assert ("X-Test", "1") in s.fpp_headers

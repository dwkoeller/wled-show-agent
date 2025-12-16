from __future__ import annotations

from config import load_settings


def test_config_parses_fpp_headers_json(monkeypatch) -> None:
    monkeypatch.setenv("WLED_TREE_URL", "http://172.16.200.50")
    monkeypatch.setenv("FPP_BASE_URL", "http://172.16.200.20")
    monkeypatch.setenv("FPP_HEADERS_JSON", '{"Authorization":"Bearer token","X-Test":"1"}')

    s = load_settings()
    assert s.fpp_base_url == "http://172.16.200.20"
    assert ("Authorization", "Bearer token") in s.fpp_headers
    assert ("X-Test", "1") in s.fpp_headers


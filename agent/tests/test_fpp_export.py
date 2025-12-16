from __future__ import annotations

from fpp_export import render_http_post_script


def test_render_http_post_script_contains_url_path_and_payload() -> None:
    script = render_http_post_script(
        coordinator_base_url="http://172.16.200.10:8088",
        path="/v1/fleet/stop_all",
        payload={"targets": ["roofline1"], "include_self": True},
        a2a_api_key=None,
    )
    assert "AGENT_URL='http://172.16.200.10:8088'" in script
    assert "/v1/fleet/stop_all" in script
    assert "PAYLOAD_B64='" in script
    assert 'A2A_KEY=' in script


def test_render_http_post_script_embeds_a2a_key_when_provided() -> None:
    script = render_http_post_script(
        coordinator_base_url="http://172.16.200.10:8088",
        path="/v1/fleet/stop_all",
        payload={"targets": None, "include_self": True},
        a2a_api_key="secret",
    )
    assert "A2A_KEY='secret'" in script


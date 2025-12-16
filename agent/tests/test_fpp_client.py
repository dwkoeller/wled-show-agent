from __future__ import annotations

import pytest

from fpp_client import FPPClient


def test_fpp_client_normalizes_base_url() -> None:
    c = FPPClient(base_url="172.16.200.20/", timeout_s=1.0)
    assert c.base_url == "http://172.16.200.20"


def test_fpp_client_requires_playlist_name() -> None:
    c = FPPClient(base_url="http://172.16.200.20", timeout_s=1.0)
    with pytest.raises(ValueError):
        c.start_playlist("")


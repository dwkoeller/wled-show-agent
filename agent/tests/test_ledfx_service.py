from __future__ import annotations

import pytest
from fastapi import HTTPException

from services import ledfx_service


def test_ledfx_brightness_values() -> None:
    primary, fallback = ledfx_service._brightness_values(0.5)
    assert primary == 0.5
    assert fallback is None

    primary, fallback = ledfx_service._brightness_values(128)
    assert round(primary, 4) == round(128 / 255.0, 4)
    assert fallback == 128


def test_ledfx_proxy_path_allowlist() -> None:
    assert ledfx_service._normalize_proxy_path("api/virtuals") == "/api/virtuals"
    assert ledfx_service._normalize_proxy_path("/api/virtuals") == "/api/virtuals"
    with pytest.raises(HTTPException):
        ledfx_service._normalize_proxy_path("/status")

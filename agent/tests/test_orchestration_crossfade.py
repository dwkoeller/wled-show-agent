from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from models.requests import OrchestrationCrossfadeRequest
from services import orchestration_service


class _Settings:
    wled_max_bri = 100


class _DummyLooks:
    def __init__(self) -> None:
        self.row = None
        self.brightness = None
        self.transition = None

    async def apply_look(self, row, *, brightness_override=None, transition_ms=None):
        self.row = dict(row)
        self.brightness = brightness_override
        self.transition = transition_ms
        return {"ok": True}


class _DummyWLED:
    def __init__(self) -> None:
        self.payload = None

    async def apply_state(self, payload, *, verbose=False):
        _ = verbose
        self.payload = dict(payload)
        return {"ok": True, "payload": payload}


@pytest.mark.asyncio
async def test_orchestration_crossfade_requires_payload() -> None:
    state = SimpleNamespace(
        settings=_Settings(),
        looks=None,
        wled=None,
        wled_cooldown=None,
        db=None,
        runtime_state_path="",
    )
    req = OrchestrationCrossfadeRequest()
    with pytest.raises(HTTPException) as exc:
        await orchestration_service.orchestration_crossfade(
            req, request=None, state=state, _=None
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_orchestration_crossfade_look_path() -> None:
    looks = _DummyLooks()
    state = SimpleNamespace(
        settings=_Settings(),
        looks=looks,
        wled=None,
        wled_cooldown=None,
        db=None,
        runtime_state_path="",
    )
    req = OrchestrationCrossfadeRequest(
        look={"type": "wled_look", "name": "Warm", "seg": {"fx": "Solid"}},
        brightness=200,
        transition_ms=1500,
    )

    res = await orchestration_service.orchestration_crossfade(
        req, request=None, state=state, _=None
    )

    assert res["ok"] is True
    assert looks.brightness == 100
    assert looks.transition == 1500
    assert looks.row["name"] == "Warm"


@pytest.mark.asyncio
async def test_orchestration_crossfade_state_path() -> None:
    wled = _DummyWLED()
    state = SimpleNamespace(
        settings=_Settings(),
        looks=None,
        wled=wled,
        wled_cooldown=None,
        db=None,
        runtime_state_path="",
    )
    req = OrchestrationCrossfadeRequest(
        state={"on": True, "bri": 255},
        transition_ms=2000,
    )

    res = await orchestration_service.orchestration_crossfade(
        req, request=None, state=state, _=None
    )

    assert res["ok"] is True
    assert wled.payload["bri"] == 100
    assert wled.payload["tt"] == 20
    assert wled.payload["transition"] == 20

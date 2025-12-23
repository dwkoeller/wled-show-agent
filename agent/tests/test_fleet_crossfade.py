from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from models.requests import FleetCrossfadeRequest
from services import fleet_service


class _Settings:
    wled_max_bri = 255
    a2a_http_timeout_s = 1.0
    fleet_db_discovery_enabled = False


@pytest.mark.asyncio
async def test_fleet_crossfade_requires_payload() -> None:
    state = SimpleNamespace(settings=_Settings(), peers={}, db=None)
    req = FleetCrossfadeRequest(include_self=False)
    with pytest.raises(HTTPException) as exc:
        await fleet_service.fleet_crossfade(req, request=None, state=state)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_fleet_crossfade_no_peers_ok() -> None:
    state = SimpleNamespace(settings=_Settings(), peers={}, db=None)
    req = FleetCrossfadeRequest(
        include_self=False,
        look={"type": "wled_look", "seg": {"fx": "Solid", "pal": "Default"}},
    )
    res = await fleet_service.fleet_crossfade(req, request=None, state=state)
    assert res["ok"] is True
    assert res["results"] == {}

from __future__ import annotations

import asyncio

import httpx
import pytest

from ledfx_client import AsyncLedFxClient


def test_ledfx_client_normalizes_base_url() -> None:
    async def _run() -> None:
        async with httpx.AsyncClient() as client:
            c = AsyncLedFxClient(
                base_url="localhost:8888/", client=client, timeout_s=1.0
            )
            assert c.base_url == "http://localhost:8888"

    asyncio.run(_run())


def test_ledfx_client_requires_scene_id() -> None:
    async def _run() -> None:
        async with httpx.AsyncClient() as client:
            c = AsyncLedFxClient(
                base_url="http://localhost:8888", client=client, timeout_s=1.0
            )
            with pytest.raises(ValueError):
                await c.activate_scene("")
            with pytest.raises(ValueError):
                await c.deactivate_scene("")

    asyncio.run(_run())


def test_ledfx_client_requires_virtual_effect() -> None:
    async def _run() -> None:
        async with httpx.AsyncClient() as client:
            c = AsyncLedFxClient(
                base_url="http://localhost:8888", client=client, timeout_s=1.0
            )
            with pytest.raises(ValueError):
                await c.set_virtual_effect(virtual_id="", effect="rainbow")
            with pytest.raises(ValueError):
                await c.set_virtual_effect(virtual_id="v1", effect="")

    asyncio.run(_run())

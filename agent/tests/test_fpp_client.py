from __future__ import annotations

import asyncio

import httpx
import pytest

from fpp_client import AsyncFPPClient


def test_fpp_client_normalizes_base_url() -> None:
    async def _run() -> None:
        async with httpx.AsyncClient() as client:
            c = AsyncFPPClient(base_url="172.16.200.20/", client=client, timeout_s=1.0)
            assert c.base_url == "http://172.16.200.20"

    asyncio.run(_run())


def test_fpp_client_requires_playlist_name() -> None:
    async def _run() -> None:
        async with httpx.AsyncClient() as client:
            c = AsyncFPPClient(
                base_url="http://172.16.200.20", client=client, timeout_s=1.0
            )
            with pytest.raises(ValueError):
                await c.start_playlist("")

    asyncio.run(_run())

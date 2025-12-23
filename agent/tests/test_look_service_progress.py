from __future__ import annotations

import asyncio

import pytest

from look_service import LookService
from wled_mapper import WLEDMapper


class _DummyWLED:
    async def get_effects(self, refresh: bool = True):  # type: ignore[override]
        _ = refresh
        return ["Solid", "Blink"]

    async def get_palettes(self, refresh: bool = True):  # type: ignore[override]
        _ = refresh
        return ["Default", "Rainbow"]

    async def get_segments(self, refresh: bool = True):  # type: ignore[override]
        _ = refresh
        return [{"id": 0, "start": 0, "stop": 50}]


class _DummyCpuPool:
    async def run(self, func, *args, **kwargs):  # type: ignore[no-untyped-def]
        return await asyncio.to_thread(func, *args, **kwargs)


@pytest.mark.asyncio
async def test_generate_pack_reports_progress(tmp_path) -> None:
    wled = _DummyWLED()
    mapper = WLEDMapper()
    pool = _DummyCpuPool()
    svc = LookService(
        wled=wled,
        mapper=mapper,
        data_dir=str(tmp_path),
        max_bri=255,
        cpu_pool=pool,
    )

    progress: list[tuple[int, int, str]] = []

    def progress_cb(cur: int, total: int, msg: str) -> None:
        progress.append((cur, total, msg))

    summary = await svc.generate_pack(
        total_looks=3,
        themes=["classic"],
        brightness=120,
        seed=123,
        write_files=False,
        include_multi_segment=False,
        progress_cb=progress_cb,
        cancel_cb=lambda: False,
    )

    assert summary.total > 0
    assert progress

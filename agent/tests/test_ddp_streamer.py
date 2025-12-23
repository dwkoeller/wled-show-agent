from __future__ import annotations

import asyncio

import pytest

import ddp_streamer as ddp_mod
from ddp_sender import DDPConfig
from geometry import TreeGeometry
from segment_layout import SegmentLayout, SegmentRange


class _DummyWLED:
    async def enter_live_mode(self) -> None:
        return None

    async def set_brightness(self, _: int) -> None:
        return None

    async def exit_live_mode(self) -> None:
        return None


class _DummySender:
    def __init__(self, _: DDPConfig) -> None:
        self.frames: list[bytes] = []

    async def send_frame(self, rgb: bytes) -> None:
        self.frames.append(rgb)

    def close(self) -> None:
        return None


class _DummyPool:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, func, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.calls += 1
        return func(*args, **kwargs)


@pytest.mark.asyncio
async def test_ddp_streamer_uses_compute_pool(monkeypatch) -> None:
    async def _fake_layout(*_, **__) -> SegmentLayout:
        return SegmentLayout(
            led_count=3,
            segments=[SegmentRange(id=0, start=0, stop=3)],
            kind="equal",
        )

    monkeypatch.setattr(ddp_mod, "DDPAsyncSender", _DummySender)
    monkeypatch.setattr(ddp_mod, "fetch_segment_layout_async", _fake_layout)

    pool = _DummyPool()
    ddp = ddp_mod.DDPStreamer(
        wled=_DummyWLED(),
        geometry=TreeGeometry(runs=1, pixels_per_run=3, segment_len=3, segments_per_run=1),
        ddp_cfg=DDPConfig(host="127.0.0.1", port=4048),
        cpu_pool=pool,
        fps_default=5.0,
    )

    await ddp.start(pattern="solid", duration_s=0.25, brightness=128, fps=5.0)
    await asyncio.sleep(0.35)
    await ddp.stop()

    assert pool.calls > 0


@pytest.mark.asyncio
async def test_ddp_streamer_falls_back_when_pattern_not_picklable(
    monkeypatch,
) -> None:
    async def _fake_layout(*_, **__) -> SegmentLayout:
        return SegmentLayout(
            led_count=3,
            segments=[SegmentRange(id=0, start=0, stop=3)],
            kind="equal",
        )

    monkeypatch.setattr(ddp_mod, "DDPAsyncSender", _DummySender)
    monkeypatch.setattr(ddp_mod, "fetch_segment_layout_async", _fake_layout)
    monkeypatch.setattr(ddp_mod.pickle, "dumps", lambda _: (_ for _ in ()).throw(Exception("no")))

    cpu_pool = _DummyPool()
    blocking_pool = _DummyPool()
    ddp = ddp_mod.DDPStreamer(
        wled=_DummyWLED(),
        geometry=TreeGeometry(runs=1, pixels_per_run=3, segment_len=3, segments_per_run=1),
        ddp_cfg=DDPConfig(host="127.0.0.1", port=4048),
        cpu_pool=cpu_pool,
        blocking=blocking_pool,
        fps_default=5.0,
    )

    await ddp.start(pattern="solid", duration_s=0.25, brightness=128, fps=5.0)
    await asyncio.sleep(0.35)
    await ddp.stop()

    assert blocking_pool.calls > 0
    assert cpu_pool.calls == 0

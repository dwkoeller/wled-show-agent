from __future__ import annotations

from pathlib import Path

from fseq import write_fseq_v1_file


def _u16le(b: bytes) -> int:
    return int.from_bytes(b, "little")


def _u32le(b: bytes) -> int:
    return int.from_bytes(b, "little")


def test_write_fseq_v1_header_and_size(tmp_path: Path) -> None:
    out = tmp_path / "test.fseq"
    channels = 9
    frames = 3
    step_ms = 50

    def gen():
        for i in range(frames):
            yield bytes([i]) * channels

    res = write_fseq_v1_file(
        out_path=str(out),
        channel_count=channels,
        num_frames=frames,
        step_ms=step_ms,
        frame_generator=gen(),
    )
    assert res.frames == frames
    assert res.channels == channels
    assert out.is_file()

    raw = out.read_bytes()
    assert raw[0:4] == b"PSEQ"
    assert _u16le(raw[4:6]) == 28
    assert raw[6] == 0  # minor
    assert raw[7] == 1  # major
    assert _u16le(raw[8:10]) == 28
    assert _u32le(raw[10:14]) == channels
    assert _u32le(raw[14:18]) == frames
    assert raw[18] == step_ms
    assert len(raw) == 28 + (frames * channels)

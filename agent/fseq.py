from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Optional


class FSEQError(RuntimeError):
    pass


def _u16le(v: int) -> bytes:
    return int(v).to_bytes(2, "little", signed=False)


def _u32le(v: int) -> bytes:
    return int(v).to_bytes(4, "little", signed=False)


def _round4(n: int) -> int:
    x = int(n)
    r = x % 4
    if r == 0:
        return x
    return x + (4 - r)


@dataclass(frozen=True)
class FSEQV1Header:
    channel_count: int
    num_frames: int
    step_ms: int
    gamma: int = 1
    color_order: int = 2
    version_major: int = 1
    version_minor: int = 0

    @property
    def channel_data_offset(self) -> int:
        # V1 with no variable headers = 28 bytes, already 4-byte aligned.
        return _round4(28)


class FSEQV1Writer:
    """
    Minimal xLights/FPP compatible FSEQ v1 writer (uncompressed).

    Reference: xLights `V1FSEQFile::writeHeader()` and `V1FSEQFile::addFrame()`.
    """

    def __init__(self, fp: BinaryIO, header: FSEQV1Header) -> None:
        self.fp = fp
        self.header = header
        self._frames_written = 0

        if header.channel_count <= 0:
            raise ValueError("channel_count must be > 0")
        if header.num_frames <= 0:
            raise ValueError("num_frames must be > 0")
        if header.step_ms <= 0 or header.step_ms > 255:
            raise ValueError("step_ms must be 1..255 for FSEQ v1")

    @property
    def frames_written(self) -> int:
        return self._frames_written

    def write_header(self) -> None:
        h = self.header
        fixed_header_len = 28
        offset = h.channel_data_offset
        buf = bytearray(offset)

        # Signature
        buf[0:4] = b"PSEQ"
        # Channel data start offset (u16)
        buf[4:6] = _u16le(offset)
        # Version (minor, major)
        buf[6] = int(h.version_minor) & 0xFF
        buf[7] = int(h.version_major) & 0xFF
        # Fixed header length (u16)
        buf[8:10] = _u16le(fixed_header_len)
        # Channel count (u32)
        buf[10:14] = _u32le(h.channel_count)
        # Number of frames (u32)
        buf[14:18] = _u32le(h.num_frames)
        # Step time in ms (u8)
        buf[18] = int(h.step_ms) & 0xFF
        # Flags (unused)
        buf[19] = 0
        # Universe count/size (unused)
        buf[20:22] = _u16le(0)
        buf[22:24] = _u16le(0)
        # Gamma + Color order (unused by FPP but expected)
        buf[24] = int(h.gamma) & 0xFF
        buf[25] = int(h.color_order) & 0xFF
        # Reserved
        buf[26] = 0
        buf[27] = 0

        self.fp.write(bytes(buf))

    def add_frame(self, frame_bytes: bytes) -> None:
        if self._frames_written >= int(self.header.num_frames):
            raise FSEQError("All frames already written")

        if len(frame_bytes) != int(self.header.channel_count):
            raise FSEQError(
                f"Frame size {len(frame_bytes)} != channel_count {self.header.channel_count}"
            )

        self.fp.write(frame_bytes)
        self._frames_written += 1

    def finalize(self) -> None:
        if self._frames_written != int(self.header.num_frames):
            raise FSEQError(
                f"frames_written={self._frames_written} != num_frames={self.header.num_frames}"
            )
        try:
            self.fp.flush()
        except Exception:
            pass


@dataclass(frozen=True)
class ExportedFSEQ:
    filename: str
    rel_path: str
    bytes_written: int
    frames: int
    channels: int
    step_ms: int


def write_fseq_v1_file(
    *,
    out_path: str,
    channel_count: int,
    num_frames: int,
    step_ms: int,
    frame_generator,
) -> ExportedFSEQ:
    """
    Write an uncompressed FSEQ v1 file to disk.

    `frame_generator` yields exactly `num_frames` bytes objects, each of length `channel_count`.
    """
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    header = FSEQV1Header(
        channel_count=int(channel_count),
        num_frames=int(num_frames),
        step_ms=int(step_ms),
    )
    bytes_written = 0

    with p.open("wb") as f:
        w = FSEQV1Writer(f, header)
        w.write_header()
        bytes_written += header.channel_data_offset

        for fb in frame_generator:
            w.add_frame(bytes(fb))
            bytes_written += int(channel_count)

        w.finalize()

    return ExportedFSEQ(
        filename=p.name,
        rel_path=str(p),
        bytes_written=int(bytes_written),
        frames=int(num_frames),
        channels=int(channel_count),
        step_ms=int(step_ms),
    )

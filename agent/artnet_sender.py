from __future__ import annotations

import socket
import struct
import threading
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ArtNetConfig:
    host: str
    port: int = 6454
    universe_start: int = 0  # Art-Net Port-Address (0-based is common)
    channels_per_universe: int = 510  # 170 RGB pixels


class ArtNetSender:
    """
    Minimal Art-Net (ArtDMX) sender compatible with ESPixelStick.

    Packet format:
      ID[8] + OpCode[2 LE] + ProtVer[2 BE] + Seq[1] + Phys[1] + Universe[2 LE] + Length[2 BE] + Data[n]
    """

    def __init__(self, cfg: ArtNetConfig) -> None:
        if not cfg.host:
            raise ValueError("Art-Net host is required")
        self.cfg = cfg
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._lock = threading.Lock()
        self._seq = 0

        ch = int(cfg.channels_per_universe)
        if ch <= 0 or ch > 512:
            raise ValueError("channels_per_universe must be 1..512")
        self._channels_per_universe = ch

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass

    def _next_seq(self) -> int:
        with self._lock:
            self._seq = (self._seq % 255) + 1
            return self._seq

    def send_frame(self, rgb: bytes) -> None:
        if not isinstance(rgb, (bytes, bytearray, memoryview)):
            raise TypeError("rgb must be bytes-like")
        data = memoryview(rgb)
        total = len(data)
        if total <= 0:
            return

        seq = self._next_seq()
        ch_per = self._channels_per_universe
        universes = (total + ch_per - 1) // ch_per
        if universes <= 0:
            universes = 1

        for u in range(universes):
            start = u * ch_per
            end = min(total, start + ch_per)
            chunk = bytes(data[start:end])
            if len(chunk) < ch_per:
                chunk += b"\x00" * (ch_per - len(chunk))

            universe = int(self.cfg.universe_start) + u
            header = (
                b"Art-Net\x00"
                + struct.pack("<H", 0x5000)  # OpCode ArtDMX (little-endian)
                + struct.pack(">H", 14)  # ProtVer (big-endian)
                + struct.pack("BB", seq & 0xFF, 0)  # Seq, Physical
                + struct.pack("<H", universe & 0xFFFF)  # Universe (Port-Address), little-endian
                + struct.pack(">H", len(chunk) & 0xFFFF)  # Length, big-endian
            )
            self._sock.sendto(header + chunk, (self.cfg.host, int(self.cfg.port)))


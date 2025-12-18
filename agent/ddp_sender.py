from __future__ import annotations

import socket
import struct
import threading
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DDPConfig:
    host: str
    port: int = 4048
    destination_id: int = 1
    max_pixels_per_packet: int = 480  # 480*3 = 1440 bytes (nice MTU fit)
    # DDP constants (subset compatible with WLED / LedFx)
    ver1_flag: int = 0x40
    push_flag: int = 0x01
    datatype_rgb: int = 0x0B  # RGB, 8-bit


class DDPSender:
    """
    Minimal DDP sender compatible with WLED.

    Header format used (10 bytes):
      !BBBBLH
      byte0: version/flags (0x40 + optional PUSH on last packet)
      byte1: sequence (1..15)
      byte2: datatype (0x0B for RGB 8-bit in LedFx implementation)
      byte3: destination id
      uint32: offset in bytes
      uint16: data length in bytes
    """

    def __init__(self, cfg: DDPConfig) -> None:
        if not cfg.host:
            raise ValueError("DDP host is required")
        self.cfg = cfg
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._lock = threading.Lock()
        self._seq = 0

        self._max_data_len = max(1, int(cfg.max_pixels_per_packet)) * 3

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass

    def _next_seq(self) -> int:
        # DDP sequence commonly uses 1..15 rolling
        with self._lock:
            self._seq = (self._seq % 15) + 1
            return self._seq

    def send_frame(self, rgb: bytes) -> None:
        if not isinstance(rgb, (bytes, bytearray, memoryview)):
            raise TypeError("rgb must be bytes-like")
        data = memoryview(rgb)
        total = len(data)
        if total == 0:
            return
        seq = self._next_seq()

        # Number of packets
        max_len = self._max_data_len
        packets, rem = divmod(total, max_len)
        if rem == 0:
            packets -= 1  # exactly fits; divmod gave an extra 0 remainder

        for i in range(packets + 1):
            start = i * max_len
            end = min(total, start + max_len)
            chunk = data[start:end]
            last = i == packets
            flags = self.cfg.ver1_flag | (self.cfg.push_flag if last else 0)
            header = struct.pack(
                "!BBBBLH",
                flags & 0xFF,
                seq & 0xFF,
                self.cfg.datatype_rgb & 0xFF,
                self.cfg.destination_id & 0xFF,
                start,  # byte offset
                len(chunk) & 0xFFFF,
            )
            self._sock.sendto(
                header + bytes(chunk), (self.cfg.host, int(self.cfg.port))
            )

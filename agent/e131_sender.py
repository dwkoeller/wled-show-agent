from __future__ import annotations

import socket
import struct
import threading
import uuid
from dataclasses import dataclass


ACN_PID = b"ASC-E1.17\x00\x00\x00"  # 12 bytes


@dataclass(frozen=True)
class E131Config:
    host: str
    port: int = 5568
    universe_start: int = 1
    channels_per_universe: int = 510  # 170 RGB pixels
    priority: int = 100
    source_name: str = "wled-show-agent"


class E131Sender:
    """
    Minimal E1.31 (sACN) sender compatible with ESPixelStick (unicast).

    Uses the E1.31 "Data Packet" (Root Vector 0x00000004) with:
      - Framing Vector 0x00000002
      - DMP Vector 0x02, Address/Data Type 0xA1
    """

    def __init__(self, cfg: E131Config) -> None:
        if not cfg.host:
            raise ValueError("E1.31 host is required")
        if cfg.universe_start <= 0 or cfg.universe_start > 63999:
            raise ValueError("universe_start must be 1..63999")

        ch = int(cfg.channels_per_universe)
        if ch <= 0 or ch > 512:
            raise ValueError("channels_per_universe must be 1..512")

        pri = int(cfg.priority)
        if pri < 0 or pri > 200:
            raise ValueError("priority must be 0..200")

        self.cfg = cfg
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._lock = threading.Lock()
        self._seq = 0

        self._slots_len = ch
        # Precompute flags/length values for fixed slot length.
        self._dmp_flags_len = 0x7000 | (9 + self._slots_len)
        self._framing_flags_len = 0x7000 | (86 + self._slots_len)
        self._root_flags_len = 0x7000 | (108 + self._slots_len)

        name = (cfg.source_name or "wled-show-agent").encode("utf-8", errors="ignore")[
            :64
        ]
        self._source_name = name + (b"\x00" * (64 - len(name)))
        self._cid = uuid.uuid4().bytes  # 16 bytes

    def close(self) -> None:
        try:
            self._sock.close()
        except Exception:
            pass

    def _next_seq(self) -> int:
        with self._lock:
            self._seq = (self._seq + 1) % 256
            return self._seq

    def send_frame(self, rgb: bytes) -> None:
        if not isinstance(rgb, (bytes, bytearray, memoryview)):
            raise TypeError("rgb must be bytes-like")
        data = memoryview(rgb)
        total = len(data)
        if total <= 0:
            return

        seq = self._next_seq()
        ch_per = self._slots_len
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
            pkt = self._build_packet(universe=universe, sequence=seq, dmx=chunk)
            self._sock.sendto(pkt, (self.cfg.host, int(self.cfg.port)))

    def _build_packet(self, *, universe: int, sequence: int, dmx: bytes) -> bytes:
        # Root layer
        preamble = struct.pack(">HH", 0x0010, 0x0000)
        root = (
            preamble
            + ACN_PID
            + struct.pack(">H", self._root_flags_len)
            + struct.pack(">I", 0x00000004)
            + self._cid
        )

        # Framing layer
        framing = (
            struct.pack(">H", self._framing_flags_len)
            + struct.pack(">I", 0x00000002)
            + self._source_name
            + struct.pack("B", int(self.cfg.priority) & 0xFF)
            + struct.pack(">H", 0x0000)  # reserved
            + struct.pack("B", int(sequence) & 0xFF)
            + struct.pack("B", 0x00)  # options
            + struct.pack(">H", int(universe) & 0xFFFF)
        )

        # DMP layer
        dmp = (
            struct.pack(">H", self._dmp_flags_len)
            + struct.pack("B", 0x02)  # DMP vector
            + struct.pack("B", 0xA1)  # addr/type
            + struct.pack(">H", 0x0000)  # first property address
            + struct.pack(">H", 0x0001)  # address increment
            + struct.pack(">H", (1 + self._slots_len) & 0xFFFF)  # property value count
            + struct.pack("B", 0x00)  # start code
            + dmx
        )

        return root + framing + dmp

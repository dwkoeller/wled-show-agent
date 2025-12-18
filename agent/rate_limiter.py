from __future__ import annotations

import threading
import time


class Cooldown:
    """
    Simple thread-safe cooldown gate. Prevents calling "fire" more often than every `cooldown_ms`.
    """

    def __init__(self, cooldown_ms: int) -> None:
        self.cooldown_ms = max(0, int(cooldown_ms))
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self.cooldown_ms <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed:
                    self._next_allowed = now + (self.cooldown_ms / 1000.0)
                    return
                sleep_for = max(0.0, self._next_allowed - now)
            time.sleep(min(0.25, sleep_for))

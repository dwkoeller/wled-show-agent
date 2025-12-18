from __future__ import annotations

import asyncio
import concurrent.futures
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


class AsyncCooldown:
    """
    Simple async cooldown gate. Prevents calling "fire" more often than every `cooldown_ms`.
    """

    def __init__(self, cooldown_ms: int) -> None:
        self.cooldown_ms = max(0, int(cooldown_ms))
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        if self.cooldown_ms <= 0:
            return
        while True:
            async with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed:
                    self._next_allowed = now + (self.cooldown_ms / 1000.0)
                    return
                sleep_for = max(0.0, self._next_allowed - now)
            await asyncio.sleep(min(0.25, sleep_for))


class AsyncCooldownSyncAdapter:
    """
    Sync adapter for AsyncCooldown, intended for use from worker threads.
    """

    def __init__(
        self,
        cooldown: AsyncCooldown,
        *,
        loop: asyncio.AbstractEventLoop,
        timeout_s: float = 30.0,
    ) -> None:
        self._cooldown = cooldown
        self._loop = loop
        self._timeout_s = float(timeout_s)

    def wait(self) -> None:
        try:
            running = asyncio.get_running_loop()
        except Exception:
            running = None
        if running is self._loop:
            raise RuntimeError(
                "AsyncCooldownSyncAdapter cannot be used on the event loop thread"
            )
        fut = asyncio.run_coroutine_threadsafe(self._cooldown.wait(), self._loop)
        try:
            fut.result(timeout=self._timeout_s)
        except concurrent.futures.TimeoutError as e:
            raise TimeoutError(str(e)) from e

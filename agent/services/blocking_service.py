from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable


class BlockingQueueFull(RuntimeError):
    pass


@dataclass(frozen=True)
class BlockingStats:
    max_workers: int
    max_queue: int
    inflight: int


class BlockingService:
    """
    Bounded worker pool for CPU-bound or sync I/O tasks.

    This provides backpressure by limiting the number of in-flight jobs and
    runs blocking work via asyncio.to_thread with bounded concurrency.
    """

    def __init__(
        self,
        *,
        max_workers: int = 4,
        max_queue: int = 16,
        acquire_timeout_s: float = 2.0,
    ) -> None:
        self._max_workers = max(1, int(max_workers))
        self._max_queue = max(self._max_workers, int(max_queue))
        self._acquire_timeout_s = max(0.1, float(acquire_timeout_s))
        self._queue_slots = asyncio.BoundedSemaphore(self._max_queue)
        self._worker_slots = asyncio.BoundedSemaphore(self._max_workers)
        self._inflight = 0
        self._lock = asyncio.Lock()
        self._closed = False

    async def run(self, func: Callable[..., Any], *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if self._closed:
            raise RuntimeError("BlockingService is closed")
        try:
            await asyncio.wait_for(
                self._queue_slots.acquire(), timeout=self._acquire_timeout_s
            )
        except asyncio.TimeoutError as e:
            raise BlockingQueueFull("Blocking queue is full") from e

        async with self._lock:
            self._inflight += 1

        try:
            await self._worker_slots.acquire()
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            finally:
                self._worker_slots.release()
        finally:
            async with self._lock:
                self._inflight = max(0, int(self._inflight) - 1)
            self._queue_slots.release()

    async def stats(self) -> BlockingStats:
        async with self._lock:
            inflight = int(self._inflight)
        return BlockingStats(
            max_workers=self._max_workers, max_queue=self._max_queue, inflight=inflight
        )

    async def shutdown(self) -> None:
        self._closed = True


class ProcessService:
    """
    Bounded process pool for CPU-heavy tasks.

    Uses a ProcessPoolExecutor with a bounded queue to apply backpressure.
    """

    def __init__(
        self,
        *,
        max_workers: int = 2,
        max_queue: int = 8,
        acquire_timeout_s: float = 2.0,
    ) -> None:
        self._max_workers = max(1, int(max_workers))
        self._max_queue = max(self._max_workers, int(max_queue))
        self._acquire_timeout_s = max(0.1, float(acquire_timeout_s))
        self._queue_slots = asyncio.BoundedSemaphore(self._max_queue)
        self._worker_slots = asyncio.BoundedSemaphore(self._max_workers)
        self._inflight = 0
        self._lock = asyncio.Lock()
        self._closed = False
        self._executor = ProcessPoolExecutor(max_workers=self._max_workers)

    async def run(self, func: Callable[..., Any], *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        if self._closed:
            raise RuntimeError("ProcessService is closed")
        try:
            await asyncio.wait_for(
                self._queue_slots.acquire(), timeout=self._acquire_timeout_s
            )
        except asyncio.TimeoutError as e:
            raise BlockingQueueFull("Process queue is full") from e

        async with self._lock:
            self._inflight += 1

        try:
            await self._worker_slots.acquire()
            try:
                loop = asyncio.get_running_loop()
                call = partial(func, *args, **kwargs)
                return await loop.run_in_executor(self._executor, call)
            finally:
                self._worker_slots.release()
        finally:
            async with self._lock:
                self._inflight = max(0, int(self._inflight) - 1)
            self._queue_slots.release()

    async def stats(self) -> BlockingStats:
        async with self._lock:
            inflight = int(self._inflight)
        return BlockingStats(
            max_workers=self._max_workers, max_queue=self._max_queue, inflight=inflight
        )

    async def shutdown(self) -> None:
        self._closed = True
        try:
            self._executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            self._executor.shutdown(wait=False)

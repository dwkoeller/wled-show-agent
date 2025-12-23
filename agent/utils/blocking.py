from __future__ import annotations

import asyncio
from typing import Any, Callable

from services.blocking_service import BlockingService, ProcessService


async def run_blocking(
    blocking: BlockingService | None,
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    if blocking is None:
        return await asyncio.to_thread(func, *args, **kwargs)
    return await blocking.run(func, *args, **kwargs)


async def run_blocking_state(
    state: Any, func: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    return await run_blocking(getattr(state, "blocking", None), func, *args, **kwargs)


async def run_ddp_blocking_state(
    state: Any, func: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    return await run_blocking(
        getattr(state, "ddp_blocking", None), func, *args, **kwargs
    )


async def run_cpu_blocking(
    cpu_pool: ProcessService | None,
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    if cpu_pool is None:
        return await asyncio.to_thread(func, *args, **kwargs)
    return await cpu_pool.run(func, *args, **kwargs)


async def run_cpu_blocking_state(
    state: Any, func: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    return await run_cpu_blocking(getattr(state, "cpu_pool", None), func, *args, **kwargs)

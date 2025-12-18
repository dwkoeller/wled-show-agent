from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, TypeVar, cast

from fastapi.concurrency import run_in_threadpool


F = TypeVar("F", bound=Callable[..., Any])


def asyncify(func: F) -> F:
    """
    Wrap a sync FastAPI endpoint so it becomes `async def` while preserving its
    signature for dependency injection and OpenAPI docs.
    """
    if inspect.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        return await run_in_threadpool(func, *args, **kwargs)

    # FastAPI inspects `__signature__` when present; this preserves request parsing.
    wrapper.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return cast(F, wrapper)

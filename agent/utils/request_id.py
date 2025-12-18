from __future__ import annotations

import logging
import time
import uuid
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.types import ASGIApp


REQUEST_ID_HEADER = "X-Request-Id"

_log = logging.getLogger("wsa.request")


def _new_request_id() -> str:
    return uuid.uuid4().hex


class RequestIdMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        *,
        header_name: str = REQUEST_ID_HEADER,
        generator: Optional[Callable[[], str]] = None,
        log_requests: bool = True,
    ) -> None:
        super().__init__(app)
        self._header_name = str(header_name or REQUEST_ID_HEADER)
        self._generator = generator or _new_request_id
        self._log_requests = bool(log_requests)

    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        rid = (
            request.headers.get(self._header_name) or ""
        ).strip() or self._generator()
        request.state.request_id = rid

        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            dur_ms = (time.perf_counter() - start) * 1000.0
            if self._log_requests:
                _log.exception(
                    "request failed request_id=%s method=%s path=%s duration_ms=%.3f",
                    rid,
                    request.method,
                    request.url.path,
                    dur_ms,
                )
            raise

        dur_ms = (time.perf_counter() - start) * 1000.0
        response.headers[self._header_name] = rid
        if self._log_requests:
            _log.info(
                "request request_id=%s method=%s path=%s status_code=%s duration_ms=%.3f",
                rid,
                request.method,
                request.url.path,
                response.status_code,
                dur_ms,
            )
        return response

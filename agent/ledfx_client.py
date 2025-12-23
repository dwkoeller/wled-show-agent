from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urlparse

import httpx

from utils.outbound_http import RetryPolicy, request_with_retry


class LedFxError(RuntimeError):
    pass


@dataclass(frozen=True)
class LedFxResponse:
    status_code: int
    body: Any

    def as_dict(self) -> Dict[str, Any]:
        return {"status_code": self.status_code, "body": self.body}


def _clean_base_url(base_url: str) -> str:
    url = (base_url or "").strip()
    if not url:
        return ""
    if not (url.startswith("http://") or url.startswith("https://")):
        url = "http://" + url
    return url.rstrip("/")


class AsyncLedFxClient:
    """
    Async LedFx HTTP client based on httpx.AsyncClient.
    """

    def __init__(
        self,
        *,
        base_url: str,
        client: httpx.AsyncClient,
        timeout_s: float = 2.5,
        headers: Optional[Dict[str, str]] = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        self.base_url = _clean_base_url(base_url)
        if not self.base_url:
            raise ValueError("LedFx base_url is required")
        self._client = client
        self.timeout_s = max(0.5, float(timeout_s))
        self.headers = dict(headers or {})
        self._retry = retry
        try:
            self._target = (
                urlparse(self.base_url).netloc or ""
            ).strip() or self.base_url
        except Exception:
            self._target = self.base_url

    def _url(self, path: str) -> str:
        p = (path or "").strip()
        if not p.startswith("/"):
            p = "/" + p
        return self.base_url + p

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
        data: Any = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> LedFxResponse:
        url = self._url(path)
        hdrs: Dict[str, str] = dict(self.headers)
        if headers:
            hdrs.update({str(k): str(v) for k, v in headers.items() if v is not None})

        try:
            resp = await request_with_retry(
                client=self._client,
                method=str(method).upper(),
                url=url,
                target_kind="ledfx",
                target=str(self._target),
                timeout_s=self.timeout_s,
                retry=self._retry,
                params=params,
                json_body=json_body,
                data=data,
                headers=hdrs,
            )
        except Exception as e:
            raise LedFxError(f"LedFx request failed: {e}")

        body: Any
        try:
            body = resp.json()
        except Exception:
            body = (resp.text or "").strip()

        if resp.status_code >= 400:
            snippet = body if isinstance(body, str) else str(body)[:300]
            raise LedFxError(f"LedFx HTTP {resp.status_code} for {path}: {snippet}")

        return LedFxResponse(status_code=int(resp.status_code), body=body)

    async def _try(
        self, attempts: Iterable[Tuple[str, str, Any | None]]
    ) -> LedFxResponse:
        errors: List[str] = []
        last: Optional[Exception] = None
        for method, path, payload in attempts:
            try:
                return await self.request(method, path, json_body=payload)
            except Exception as e:
                last = e
                errors.append(str(e))
                continue
        if last is not None:
            raise LedFxError(errors[-1])
        raise LedFxError("No attempts provided")

    async def status(self) -> LedFxResponse:
        return await self._try(
            [
                ("GET", "/api/info", None),
                ("GET", "/api/config", None),
                ("GET", "/api/virtuals", None),
                ("GET", "/api/scenes", None),
            ]
        )

    async def virtuals(self) -> LedFxResponse:
        return await self._try([("GET", "/api/virtuals", None)])

    async def scenes(self) -> LedFxResponse:
        return await self._try([("GET", "/api/scenes", None)])

    async def effects(self) -> LedFxResponse:
        return await self._try(
            [
                ("GET", "/api/effects", None),
                ("GET", "/api/effects/list", None),
                ("GET", "/api/effects/available", None),
            ]
        )

    async def activate_scene(self, scene_id: str) -> LedFxResponse:
        sid = quote((scene_id or "").strip())
        if not sid:
            raise ValueError("scene_id is required")
        return await self._try(
            [
                ("POST", "/api/scenes/activate", {"id": scene_id}),
                ("POST", "/api/scenes/activate", {"scene_id": scene_id}),
                ("POST", f"/api/scenes/{sid}/activate", None),
                ("PUT", f"/api/scenes/{sid}/activate", None),
                ("POST", f"/api/scenes/{sid}", {"active": True}),
            ]
        )

    async def deactivate_scene(self, scene_id: str) -> LedFxResponse:
        sid = quote((scene_id or "").strip())
        if not sid:
            raise ValueError("scene_id is required")
        return await self._try(
            [
                ("POST", "/api/scenes/deactivate", {"id": scene_id}),
                ("POST", "/api/scenes/deactivate", {"scene_id": scene_id}),
                ("POST", f"/api/scenes/{sid}/deactivate", None),
                ("PUT", f"/api/scenes/{sid}/deactivate", None),
                ("POST", f"/api/scenes/{sid}", {"active": False}),
                ("DELETE", f"/api/scenes/{sid}", None),
            ]
        )

    async def set_virtual_effect(
        self,
        *,
        virtual_id: str,
        effect: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> LedFxResponse:
        vid = quote((virtual_id or "").strip())
        if not vid:
            raise ValueError("virtual_id is required")
        eff = (effect or "").strip()
        if not eff:
            raise ValueError("effect is required")
        payload: Dict[str, Any] = {"type": eff}
        if config:
            payload["config"] = dict(config)
        return await self._try(
            [
                ("PUT", f"/api/virtuals/{vid}/effects", payload),
                ("POST", f"/api/virtuals/{vid}/effects", payload),
                ("PUT", f"/api/virtuals/{vid}/effect", payload),
                ("POST", f"/api/virtuals/{vid}/effect", payload),
            ]
        )

    async def set_virtual_brightness(
        self,
        *,
        virtual_id: str,
        brightness: float,
        fallback_brightness: float | None = None,
    ) -> LedFxResponse:
        vid = quote((virtual_id or "").strip())
        if not vid:
            raise ValueError("virtual_id is required")
        values: List[float] = [float(brightness)]
        if fallback_brightness is not None:
            values.append(float(fallback_brightness))

        attempts: List[Tuple[str, str, Any | None]] = []
        for val in values:
            payload = {"brightness": val}
            config_payload = {"config": {"brightness": val}}
            attempts.extend(
                [
                    ("PUT", f"/api/virtuals/{vid}/brightness", payload),
                    ("POST", f"/api/virtuals/{vid}/brightness", payload),
                    ("PATCH", f"/api/virtuals/{vid}", payload),
                    ("PUT", f"/api/virtuals/{vid}", config_payload),
                    ("PATCH", f"/api/virtuals/{vid}/config", payload),
                    ("PUT", f"/api/virtuals/{vid}/config", payload),
                ]
            )
        return await self._try(attempts)

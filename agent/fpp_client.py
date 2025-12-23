from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote
from urllib.parse import urlparse

import httpx

from utils.outbound_http import RetryPolicy, request_with_retry


class FPPError(RuntimeError):
    pass


@dataclass(frozen=True)
class FPPResponse:
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


class AsyncFPPClient:
    """
    Async Falcon Player (FPP) HTTP client based on httpx.AsyncClient.
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
            raise ValueError("FPP base_url is required")
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
        files: Any = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> FPPResponse:
        url = self._url(path)
        hdrs: Dict[str, str] = dict(self.headers)
        if headers:
            hdrs.update({str(k): str(v) for k, v in headers.items() if v is not None})

        try:
            resp = await request_with_retry(
                client=self._client,
                method=str(method).upper(),
                url=url,
                target_kind="fpp",
                target=str(self._target),
                timeout_s=self.timeout_s,
                retry=self._retry,
                params=params,
                json_body=json_body,
                data=data,
                files=files,
                headers=hdrs,
            )
        except Exception as e:
            raise FPPError(f"FPP request failed: {e}")

        body: Any
        try:
            body = resp.json()
        except Exception:
            body = (resp.text or "").strip()

        if resp.status_code >= 400:
            snippet = body if isinstance(body, str) else str(body)[:300]
            raise FPPError(f"FPP HTTP {resp.status_code} for {path}: {snippet}")

        return FPPResponse(status_code=int(resp.status_code), body=body)

    async def _try(
        self,
        attempts: Iterable[Tuple[str, str]],
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Any = None,
    ) -> FPPResponse:
        errors: List[str] = []
        last: Optional[Exception] = None
        for method, path in attempts:
            try:
                return await self.request(
                    method, path, params=params, json_body=json_body
                )
            except Exception as e:
                last = e
                errors.append(str(e))
                continue
        if last is not None:
            raise FPPError(errors[-1])
        raise FPPError("No attempts provided")

    async def status(self) -> FPPResponse:
        return await self._try(
            [
                ("GET", "/api/fppd/status"),
                ("GET", "/api/status"),
                ("GET", "/api/system/status"),
            ]
        )

    async def playlists(self) -> FPPResponse:
        return await self._try(
            [
                ("GET", "/api/playlists"),
                ("GET", "/api/playlist"),
            ]
        )

    async def start_playlist(self, name: str, *, repeat: bool = False) -> FPPResponse:
        n = quote((name or "").strip())
        if not n:
            raise ValueError("playlist name is required")
        params = {"repeat": 1 if repeat else 0}
        return await self._try(
            [
                ("GET", f"/api/playlist/{n}/start"),
                ("POST", f"/api/playlist/{n}/start"),
                ("GET", f"/api/playlists/{n}/start"),
                ("POST", f"/api/playlists/{n}/start"),
            ],
            params=params,
        )

    async def stop_playlist(self) -> FPPResponse:
        return await self._try(
            [
                ("GET", "/api/playlist/stop"),
                ("POST", "/api/playlist/stop"),
                ("GET", "/api/playlists/stop"),
                ("POST", "/api/playlists/stop"),
            ]
        )

    async def trigger_event(self, event_id: int) -> FPPResponse:
        eid = int(event_id)
        if eid <= 0:
            raise ValueError("event_id must be > 0")

        # Common URL patterns seen in the wild; FPP versions vary.
        return await self._try(
            [
                ("GET", f"/api/event/{eid}/trigger"),
                ("POST", f"/api/event/{eid}/trigger"),
                ("GET", f"/api/event/trigger/{eid}"),
                ("POST", f"/api/event/trigger/{eid}"),
                ("GET", f"/api/trigger/event/{eid}"),
                ("POST", f"/api/trigger/event/{eid}"),
            ]
        )

    async def system_info(self) -> FPPResponse:
        return await self._try(
            [
                ("GET", "/api/system/info"),
                ("GET", "/api/system/status"),
                ("GET", "/api/fppd/status"),
            ]
        )

    async def upload_file(
        self,
        *,
        dir: str,
        filename: str,
        content: bytes,
        subdir: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> FPPResponse:
        """
        Upload a file into the FPP media folder.

        Newer FPP builds support `POST /api/file/:DirName(/:SubDir)/:Filename` with raw file contents.
        """
        d = str(dir or "").strip().strip("/")
        if not d:
            raise ValueError("dir is required")
        fn = (filename or "").strip().strip("/")
        if not fn:
            raise ValueError("filename is required")
        sd = (subdir or "").strip().strip("/") or None

        path = f"/api/file/{quote(d)}"
        if sd:
            path += f"/{quote(sd)}"
        path += f"/{quote(fn)}"

        hdrs = {"Content-Type": "application/octet-stream"}
        return await self.request(
            "POST", path, params=params, data=content, headers=hdrs
        )

    async def discover(self) -> Dict[str, Any]:
        """
        Best-effort discovery: return system info + which common endpoints appear reachable.

        This is intentionally lightweight; if your FPP version differs, use /v1/fpp/request.
        """
        out: Dict[str, Any] = {"base_url": self.base_url}
        try:
            out["system"] = (await self.system_info()).as_dict()
        except Exception as e:
            out["system"] = {"error": str(e)}

        checks = {
            "status": ("GET", "/api/fppd/status"),
            "system_status": ("GET", "/api/system/status"),
            "system_info": ("GET", "/api/system/info"),
            "playlists": ("GET", "/api/playlists"),
        }

        reachable: Dict[str, Any] = {}
        for name, (m, p) in checks.items():
            try:
                reachable[name] = {
                    "ok": True,
                    "resp": (await self.request(m, p)).as_dict(),
                }
            except Exception as e:
                reachable[name] = {"ok": False, "error": str(e)}
        out["reachable"] = reachable
        return out

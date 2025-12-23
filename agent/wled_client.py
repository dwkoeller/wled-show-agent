from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx

from utils.outbound_http import RetryPolicy, request_with_retry


class WLEDError(RuntimeError):
    pass


@dataclass
class WLEDDeviceInfo:
    name: str
    version: str
    led_count: int
    fps: int | None


class AsyncWLEDClient:
    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient,
        timeout_s: float = 2.5,
        retry: RetryPolicy | None = None,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_s = float(timeout_s)
        self._client = client
        self._retry = retry
        try:
            self._target = (
                urlparse(self.base_url).netloc or ""
            ).strip() or self.base_url
        except Exception:
            self._target = self.base_url
        self._effects_cache: Optional[List[str]] = None
        self._palettes_cache: Optional[List[str]] = None
        self._presets_cache: Optional[Dict[str, Any]] = None
        self._segment_ids_cache: Optional[List[int]] = None

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    async def get_json(self, path: str) -> Any:
        url = self._url(path)
        try:
            resp = await request_with_retry(
                client=self._client,
                method="GET",
                url=url,
                target_kind="wled",
                target=str(self._target),
                timeout_s=self.timeout_s,
                retry=self._retry,
            )
        except Exception as e:
            raise WLEDError(f"GET {url} failed: {e}") from e
        if resp.status_code != 200:
            raise WLEDError(f"GET {url} -> HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            return resp.json()
        except Exception as e:
            raise WLEDError(f"GET {url} did not return JSON: {e}") from e

    async def post_json(self, path: str, payload: Dict[str, Any]) -> Any:
        url = self._url(path)
        try:
            resp = await request_with_retry(
                client=self._client,
                method="POST",
                url=url,
                target_kind="wled",
                target=str(self._target),
                timeout_s=self.timeout_s,
                json_body=payload,
                retry=self._retry,
            )
        except Exception as e:
            raise WLEDError(f"POST {url} failed: {e}") from e
        if resp.status_code not in (200, 204):
            raise WLEDError(f"POST {url} -> HTTP {resp.status_code}: {resp.text[:200]}")
        if resp.status_code == 204 or not resp.text.strip():
            return {"ok": True}
        try:
            return resp.json()
        except Exception:
            # Some versions return plain text or empty; treat as ok
            return {"ok": True, "raw": resp.text[:200]}

    async def get_full(self) -> Dict[str, Any]:
        out = await self.get_json("/json")
        if not isinstance(out, dict):
            raise WLEDError("Unexpected /json response (expected object)")
        return out

    async def get_state(self) -> Dict[str, Any]:
        out = await self.get_json("/json/state")
        if not isinstance(out, dict):
            raise WLEDError("Unexpected /json/state response (expected object)")
        return out

    async def get_segments(self, *, refresh: bool = False) -> List[Dict[str, Any]]:
        _ = refresh  # parity with sync client
        st = await self.get_state()
        seg = st.get("seg", [])
        if not isinstance(seg, list):
            return []
        out: List[Dict[str, Any]] = []
        for s in seg:
            if isinstance(s, dict):
                out.append(s)
        return out

    async def get_segment_ids(self, *, refresh: bool = False) -> List[int]:
        if self._segment_ids_cache is not None and not refresh:
            return list(self._segment_ids_cache)
        try:
            seg = (await self.get_state()).get("seg", [])
        except Exception:
            seg = []
        ids: List[int] = []
        if isinstance(seg, list):
            for idx, s in enumerate(seg):
                if isinstance(s, dict) and "id" in s:
                    try:
                        ids.append(int(s.get("id")))
                    except Exception:
                        ids.append(idx)
                else:
                    ids.append(idx)
        # de-dup while preserving order
        seen: set[int] = set()
        uniq: List[int] = []
        for i in ids:
            if i in seen:
                continue
            seen.add(i)
            uniq.append(i)
        self._segment_ids_cache = uniq
        return list(uniq)

    async def get_info(self) -> Dict[str, Any]:
        out = await self.get_json("/json/info")
        if not isinstance(out, dict):
            raise WLEDError("Unexpected /json/info response (expected object)")
        return out

    async def apply_state(self, state: Dict[str, Any], *, verbose: bool = False) -> Any:
        payload = dict(state)
        if verbose:
            payload["v"] = True
        return await self.post_json("/json/state", payload)

    async def set_preset(self, preset_id: int, *, verbose: bool = False) -> Any:
        return await self.apply_state(
            {"ps": int(preset_id), "v": bool(verbose)}, verbose=False
        )

    async def turn_off(self) -> Any:
        return await self.apply_state({"on": False})

    async def turn_on(self) -> Any:
        return await self.apply_state({"on": True})

    async def set_brightness(self, bri: int) -> Any:
        return await self.apply_state({"bri": int(bri), "on": True})

    async def enter_live_mode(self) -> Any:
        return await self.apply_state({"live": True})

    async def exit_live_mode(self) -> Any:
        return await self.apply_state({"live": False})

    async def get_effects(self, *, refresh: bool = False) -> List[str]:
        if self._effects_cache is None or refresh:
            eff = await self.get_json("/json/eff")
            if isinstance(eff, dict) and "effects" in eff:
                eff = eff["effects"]
            if not isinstance(eff, list):
                raise WLEDError("Unexpected /json/eff response (expected list)")
            self._effects_cache = [str(x) for x in eff]
        return list(self._effects_cache)

    async def get_palettes(self, *, refresh: bool = False) -> List[str]:
        if self._palettes_cache is None or refresh:
            pal = await self.get_json("/json/pal")
            if isinstance(pal, dict) and "palettes" in pal:
                pal = pal["palettes"]
            if not isinstance(pal, list):
                raise WLEDError("Unexpected /json/pal response (expected list)")
            self._palettes_cache = [str(x) for x in pal]
        return list(self._palettes_cache)

    async def get_presets_json(self, *, refresh: bool = False) -> Dict[str, Any]:
        if self._presets_cache is None or refresh:
            out = await self.get_json("/presets.json")
            if not isinstance(out, dict):
                raise WLEDError("Unexpected /presets.json response (expected object)")
            self._presets_cache = out
        return dict(self._presets_cache)

    async def device_info(self) -> WLEDDeviceInfo:
        info = await self.get_info()
        name = str(info.get("name", "WLED"))
        ver = str(info.get("ver", ""))
        leds = info.get("leds", {}) or {}
        led_count = int(leds.get("count", 0))
        fps = leds.get("fps")
        fps_i = int(fps) if isinstance(fps, (int, float)) else None
        return WLEDDeviceInfo(name=name, version=ver, led_count=led_count, fps=fps_i)

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
import requests

from utils.outbound_http import request_with_retry


class WLEDError(RuntimeError):
    pass


@dataclass
class WLEDDeviceInfo:
    name: str
    version: str
    led_count: int
    fps: int | None


class WLEDClient:
    def __init__(self, base_url: str, timeout_s: float = 2.5) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self._session = requests.Session()
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

    def get_json(self, path: str) -> Any:
        url = self._url(path)
        try:
            resp = self._session.get(url, timeout=self.timeout_s)
        except Exception as e:
            raise WLEDError(f"GET {url} failed: {e}") from e
        if resp.status_code != 200:
            raise WLEDError(f"GET {url} -> HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            return resp.json()
        except Exception as e:
            raise WLEDError(f"GET {url} did not return JSON: {e}") from e

    def post_json(self, path: str, payload: Dict[str, Any]) -> Any:
        url = self._url(path)
        try:
            resp = self._session.post(url, json=payload, timeout=self.timeout_s)
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

    # ---- Convenience wrappers ----

    def get_full(self) -> Dict[str, Any]:
        return self.get_json("/json")

    def get_state(self) -> Dict[str, Any]:
        return self.get_json("/json/state")

    def get_segments(self, *, refresh: bool = False) -> List[Dict[str, Any]]:
        st = (
            self.get_state()
            if refresh or self._segment_ids_cache is None
            else self.get_state()
        )
        seg = st.get("seg", [])
        if not isinstance(seg, list):
            return []
        out: List[Dict[str, Any]] = []
        for s in seg:
            if isinstance(s, dict):
                out.append(s)
        return out

    def get_segment_ids(self, *, refresh: bool = False) -> List[int]:
        if self._segment_ids_cache is not None and not refresh:
            return list(self._segment_ids_cache)
        try:
            seg = self.get_state().get("seg", [])
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

    def get_info(self) -> Dict[str, Any]:
        return self.get_json("/json/info")

    def get_effects(self, *, refresh: bool = False) -> List[str]:
        if self._effects_cache is None or refresh:
            # /json/eff returns just the array
            eff = self.get_json("/json/eff")
            if isinstance(eff, dict) and "effects" in eff:
                eff = eff["effects"]
            if not isinstance(eff, list):
                raise WLEDError("Unexpected /json/eff response (expected list)")
            self._effects_cache = [str(x) for x in eff]
        return list(self._effects_cache)

    def get_palettes(self, *, refresh: bool = False) -> List[str]:
        if self._palettes_cache is None or refresh:
            pal = self.get_json("/json/pal")
            if isinstance(pal, dict) and "palettes" in pal:
                pal = pal["palettes"]
            if not isinstance(pal, list):
                raise WLEDError("Unexpected /json/pal response (expected list)")
            self._palettes_cache = [str(x) for x in pal]
        return list(self._palettes_cache)

    def get_presets_json(self, *, refresh: bool = False) -> Dict[str, Any]:
        if self._presets_cache is None or refresh:
            self._presets_cache = self.get_json("/presets.json")
            if not isinstance(self._presets_cache, dict):
                raise WLEDError("Unexpected /presets.json response (expected object)")
        return dict(self._presets_cache)

    def apply_state(self, state: Dict[str, Any], *, verbose: bool = False) -> Any:
        payload = dict(state)
        if verbose:
            payload["v"] = True
        return self.post_json("/json/state", payload)

    def set_preset(self, preset_id: int, *, verbose: bool = False) -> Any:
        return self.apply_state(
            {"ps": int(preset_id), "v": bool(verbose)}, verbose=False
        )

    def turn_off(self) -> Any:
        return self.apply_state({"on": False})

    def turn_on(self) -> Any:
        return self.apply_state({"on": True})

    def set_brightness(self, bri: int) -> Any:
        return self.apply_state({"bri": int(bri), "on": True})

    def enter_live_mode(self) -> Any:
        # live:true enters realtime and blanks LEDs until effect/segments change; expects live:false when done.
        return self.apply_state({"live": True})

    def exit_live_mode(self) -> Any:
        return self.apply_state({"live": False})

    def device_info(self) -> WLEDDeviceInfo:
        info = self.get_info()
        name = str(info.get("name", "WLED"))
        ver = str(info.get("ver", ""))
        leds = info.get("leds", {}) or {}
        led_count = int(leds.get("count", 0))
        fps = leds.get("fps")
        fps_i = int(fps) if isinstance(fps, (int, float)) else None
        return WLEDDeviceInfo(name=name, version=ver, led_count=led_count, fps=fps_i)


class AsyncWLEDClient:
    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient,
        timeout_s: float = 2.5,
    ) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.timeout_s = float(timeout_s)
        self._client = client
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


class AsyncWLEDClientSyncAdapter:
    """
    Sync adapter for AsyncWLEDClient, intended for use from worker threads.

    This lets thread-based services (DDP streamer, sequence player, etc.) share the
    same async httpx client + metrics/retries implemented in AsyncWLEDClient callers.
    """

    def __init__(
        self,
        client: AsyncWLEDClient,
        *,
        loop: asyncio.AbstractEventLoop,
        timeout_s: float = 30.0,
    ) -> None:
        self._client = client
        self._loop = loop
        self._timeout_s = float(timeout_s)

    def _run(self, coro):  # type: ignore[no-untyped-def]
        try:
            running = asyncio.get_running_loop()
        except Exception:
            running = None
        if running is self._loop:
            raise RuntimeError(
                "AsyncWLEDClientSyncAdapter cannot be used on the event loop thread"
            )
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=self._timeout_s)

    # ---- WLEDClient-compatible subset ----

    def get_full(self) -> Dict[str, Any]:
        return dict(self._run(self._client.get_full()))

    def get_state(self) -> Dict[str, Any]:
        return dict(self._run(self._client.get_state()))

    def get_segments(self, *, refresh: bool = False) -> List[Dict[str, Any]]:
        return list(self._run(self._client.get_segments(refresh=refresh)))

    def get_segment_ids(self, *, refresh: bool = False) -> List[int]:
        return list(self._run(self._client.get_segment_ids(refresh=refresh)))

    def get_info(self) -> Dict[str, Any]:
        return dict(self._run(self._client.get_info()))

    def get_effects(self, *, refresh: bool = False) -> List[str]:
        return list(self._run(self._client.get_effects(refresh=refresh)))

    def get_palettes(self, *, refresh: bool = False) -> List[str]:
        return list(self._run(self._client.get_palettes(refresh=refresh)))

    def get_presets_json(self, *, refresh: bool = False) -> Dict[str, Any]:
        return dict(self._run(self._client.get_presets_json(refresh=refresh)))

    def apply_state(self, state: Dict[str, Any], *, verbose: bool = False) -> Any:
        return self._run(self._client.apply_state(state, verbose=verbose))

    def set_preset(self, preset_id: int, *, verbose: bool = False) -> Any:
        return self._run(self._client.set_preset(preset_id, verbose=verbose))

    def turn_off(self) -> Any:
        return self._run(self._client.turn_off())

    def turn_on(self) -> Any:
        return self._run(self._client.turn_on())

    def set_brightness(self, bri: int) -> Any:
        return self._run(self._client.set_brightness(bri))

    def enter_live_mode(self) -> Any:
        return self._run(self._client.enter_live_mode())

    def exit_live_mode(self) -> Any:
        return self._run(self._client.exit_live_mode())

    def device_info(self) -> WLEDDeviceInfo:
        return self._run(self._client.device_info())

from __future__ import annotations

import time
from typing import Any

from services.state import AppState


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _num(value: Any, cast=float) -> Any:
    try:
        return cast(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def extract(info: dict[str, Any], state: dict[str, Any], latency_ms: float) -> dict[str, Any]:
    leds = info.get("leds") if isinstance(info.get("leds"), dict) else {}
    wifi = info.get("wifi") if isinstance(info.get("wifi"), dict) else {}
    segs = state.get("seg") if isinstance(state.get("seg"), list) else []
    led_count = _num(_first(leds.get("count"), leds.get("lc")), int)
    if led_count is None:
        led_count = sum(int(s.get("len", 0) or 0) for s in segs if isinstance(s, dict)) or None
    sample = {
        "ok": True,
        "latency_ms": round(float(latency_ms), 2),
        "uptime_s": _num(info.get("uptime")),
        "free_heap": _num(_first(info.get("freeheap"), info.get("free_heap")), int),
        "rssi_dbm": _num(_first(wifi.get("signal"), wifi.get("rssi"), info.get("signal"), info.get("rssi")), int),
        "temperature_c": _num(_first(wifi.get("temperature"), info.get("temperature"), info.get("temp"))),
        "led_count": led_count,
        "power_w": _num(_first(leds.get("pwr"), leds.get("power"))),
        "fps": _num(_first(leds.get("fps"), info.get("fps"))),
    }
    alerts: list[str] = []
    if sample["rssi_dbm"] is not None and sample["rssi_dbm"] <= -75: alerts.append("weak_wifi")
    if sample["free_heap"] is not None and sample["free_heap"] < 20_000: alerts.append("low_heap")
    if sample["temperature_c"] is not None and sample["temperature_c"] >= 70: alerts.append("high_temperature")
    if sample["latency_ms"] >= 1000: alerts.append("high_latency")
    sample["alerts"] = alerts
    return sample


async def sample(state: AppState) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        if getattr(state, "wled", None) is None:
            raise RuntimeError("WLED client not initialized")
        info, current = await state.wled.get_info(), await state.wled.get_state()
        return extract(info or {}, current or {}, (time.perf_counter() - started) * 1000)
    except Exception as exc:
        return {"ok": False, "latency_ms": round((time.perf_counter() - started) * 1000, 2), "error": str(exc)[:512], "alerts": ["offline"]}


async def record(state: AppState) -> dict[str, Any]:
    result = await sample(state)
    state.controller_telemetry_last = {**result, "at": time.time()}
    if getattr(state, "db", None) is not None:
        try:
            await state.db.add_controller_telemetry(created_at=time.time(), sample=result)
        except Exception:
            # Telemetry must never stop the application metrics sampler.
            pass
    return result

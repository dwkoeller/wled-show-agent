from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from fastapi import Depends

from services import a2a_service
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


log = logging.getLogger(__name__)


def _normalize_topic(topic: str) -> str:
    t = str(topic or "").strip().strip("/")
    return t


def _parse_bool(val: Any) -> Optional[bool]:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(int(val))
    s = str(val or "").strip().lower()
    if s in ("1", "true", "yes", "y", "on", "enable", "enabled"):
        return True
    if s in ("0", "false", "no", "n", "off", "disable", "disabled"):
        return False
    return None


def _parse_brightness(val: Any, *, max_bri: int) -> Optional[int]:
    if isinstance(val, (int, float)):
        bri = int(val)
    else:
        s = str(val or "").strip().lower()
        if s in ("low", "dim"):
            bri = 64
        elif s in ("med", "medium"):
            bri = 128
        elif s in ("high", "bright"):
            bri = 192
        elif s in ("max", "full"):
            bri = max_bri
        else:
            try:
                bri = int(float(s))
            except Exception:
                return None
    return max(1, min(int(max_bri), int(bri)))


def _parse_payload(payload: bytes) -> Any:
    raw = payload.decode("utf-8", errors="ignore").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw


@dataclass(frozen=True)
class MQTTConfig:
    host: str
    port: int
    username: str | None
    password: str | None
    tls: bool


def _parse_mqtt_url(url: str) -> MQTTConfig:
    raw = str(url or "").strip()
    if not raw:
        raise ValueError("MQTT_URL is required")
    if "://" not in raw:
        raw = "mqtt://" + raw
    parsed = urlparse(raw)
    host = parsed.hostname or ""
    if not host:
        raise ValueError("MQTT_URL missing hostname")
    port = int(parsed.port or (8883 if parsed.scheme == "mqtts" else 1883))
    user = parsed.username or None
    pwd = parsed.password or None
    tls = parsed.scheme == "mqtts"
    return MQTTConfig(host=host, port=port, username=user, password=pwd, tls=tls)


_COMMAND_TOPICS = (
    "sequence/start",
    "sequence/stop",
    "brightness",
    "blackout",
    "scheduler/enable",
    "scheduler/start",
    "scheduler/stop",
    "stop_all",
    "ledfx/scene/activate",
    "ledfx/scene/deactivate",
    "ledfx/virtual/effect",
    "ledfx/virtual/brightness",
)

_STATE_TOPICS = (
    "status",
    "result",
    "availability",
    "state/sequence_running",
    "state/sequence_file",
    "state/brightness",
    "state/brightness_preset",
    "state/scheduler_enabled",
)


class MQTTBridge:
    def __init__(self, *, state: AppState) -> None:
        self._state = state
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()
        self._connected = False
        self._last_error: str | None = None
        self._last_error_at: float | None = None
        self._last_connect_at: float | None = None
        self._last_disconnect_at: float | None = None
        self._last_message_at: float | None = None
        self._last_action: str | None = None
        self._messages_received = 0
        self._actions_ok = 0
        self._actions_failed = 0
        self._base_topic = _normalize_topic(state.settings.mqtt_base_topic)
        self._qos = max(0, min(2, int(state.settings.mqtt_qos)))
        self._status_interval_s = int(state.settings.mqtt_status_interval_s or 0)
        self._reconnect_s = max(
            1.0, float(state.settings.mqtt_reconnect_interval_s or 5.0)
        )
        self._ha_discovery_enabled = bool(
            getattr(state.settings, "ha_mqtt_discovery_enabled", False)
        )
        self._ha_discovery_prefix = str(
            getattr(state.settings, "ha_mqtt_discovery_prefix", "homeassistant")
        ).strip() or "homeassistant"
        self._ha_entity_prefix = str(
            getattr(state.settings, "ha_mqtt_entity_prefix", "")
        ).strip()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="mqtt_bridge")

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        if task is not None:
            try:
                task.cancel()
            except Exception:
                pass
            try:
                await task
            except Exception:
                pass
        self._task = None
        await self._set_connected(False)

    async def _set_connected(self, connected: bool) -> None:
        now = time.time()
        async with self._lock:
            if self._connected == connected:
                return
            self._connected = connected
            if connected:
                self._last_connect_at = now
            else:
                self._last_disconnect_at = now

    async def _note_error(self, message: str) -> None:
        async with self._lock:
            self._last_error = str(message)
            self._last_error_at = time.time()

    async def _note_message(self, action: str | None) -> None:
        async with self._lock:
            self._last_message_at = time.time()
            self._messages_received += 1
            if action:
                self._last_action = str(action)

    async def _note_action(self, *, ok: bool, action: str | None, error: str | None) -> None:
        async with self._lock:
            if action:
                self._last_action = str(action)
            if ok:
                self._actions_ok += 1
            else:
                self._actions_failed += 1
                if error:
                    self._last_error = str(error)
                    self._last_error_at = time.time()

    async def status(self) -> Dict[str, Any]:
        settings = self._state.settings
        enabled = bool(settings.mqtt_enabled and settings.mqtt_url)
        running = bool(self._task and not self._task.done())
        cfg_error = None
        broker: Dict[str, Any] | None = None
        if enabled:
            try:
                cfg = _parse_mqtt_url(settings.mqtt_url)
                broker = {
                    "host": cfg.host,
                    "port": cfg.port,
                    "tls": cfg.tls,
                    "username": cfg.username or None,
                }
            except Exception as e:
                cfg_error = str(e)

        async with self._lock:
            return {
                "enabled": enabled,
                "running": running,
                "connected": bool(self._connected),
                "base_topic": self._base_topic,
                "qos": int(self._qos),
                "status_interval_s": int(self._status_interval_s),
                "reconnect_interval_s": float(self._reconnect_s),
                "ha_discovery": {
                    "enabled": bool(self._ha_discovery_enabled),
                    "prefix": str(self._ha_discovery_prefix),
                    "entity_prefix": str(self._ha_entity_prefix or ""),
                },
                "broker": broker,
                "broker_error": cfg_error,
                "last_error": self._last_error,
                "last_error_at": self._last_error_at,
                "last_connect_at": self._last_connect_at,
                "last_disconnect_at": self._last_disconnect_at,
                "last_message_at": self._last_message_at,
                "last_action": self._last_action,
                "counters": {
                    "messages_received": int(self._messages_received),
                    "actions_ok": int(self._actions_ok),
                    "actions_failed": int(self._actions_failed),
                },
                "topics": {
                    "base": self._base_topic,
                    "commands": list(_COMMAND_TOPICS),
                    "state": list(_STATE_TOPICS),
                },
            }

    async def _publish(
        self, client, suffix: str, payload: Any, *, retain: bool = False
    ) -> None:  # type: ignore[no-untyped-def]
        topic = f"{self._base_topic}/{suffix}"
        if isinstance(payload, str):
            data = payload
        else:
            try:
                data = json.dumps(payload, ensure_ascii=False)
            except Exception:
                data = str(payload)
        await client.publish(topic, data, qos=self._qos, retain=retain)

    async def _publish_topic(
        self, client, topic: str, payload: Any, *, retain: bool = False
    ) -> None:  # type: ignore[no-untyped-def]
        if isinstance(payload, str):
            data = payload
        else:
            try:
                data = json.dumps(payload, ensure_ascii=False)
            except Exception:
                data = str(payload)
        await client.publish(str(topic), data, qos=self._qos, retain=retain)

    def _ha_object_id(self, name: str) -> str:
        base = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
        return base.strip("_").lower() or "wsa"

    def _ha_device_info(self) -> Dict[str, Any]:
        from config.constants import APP_VERSION

        settings = self._state.settings
        device: Dict[str, Any] = {
            "identifiers": [f"wsa_{settings.agent_id}"],
            "name": str(settings.agent_name or settings.agent_id),
            "manufacturer": "WLED Show Agent",
            "model": "WLED Show Agent",
            "sw_version": str(APP_VERSION),
        }
        if settings.agent_base_url:
            device["configuration_url"] = str(settings.agent_base_url)
        return device

    async def _publish_ha_discovery(self, client) -> None:  # type: ignore[no-untyped-def]
        if not self._ha_discovery_enabled:
            return
        if not self._ha_entity_prefix:
            self._ha_entity_prefix = str(self._state.settings.agent_name or "")

        prefix = self._ha_discovery_prefix.rstrip("/")
        base = self._base_topic
        if not prefix or not base:
            return

        device = self._ha_device_info()
        avail = f"{base}/availability"
        agent_id = str(self._state.settings.agent_id)
        name_prefix = self._ha_entity_prefix or agent_id
        max_bri = int(self._state.settings.wled_max_bri)

        entities: list[tuple[str, str, Dict[str, Any]]] = []

        entities.append(
            (
                "switch",
                f"{agent_id}_scheduler_enabled",
                {
                    "name": f"{name_prefix} Scheduler",
                    "unique_id": f"{agent_id}_scheduler_enabled",
                    "command_topic": f"{base}/scheduler/enable",
                    "state_topic": f"{base}/state/scheduler_enabled",
                    "payload_on": "true",
                    "payload_off": "false",
                    "availability_topic": avail,
                    "device": device,
                },
            )
        )

        entities.append(
            (
                "button",
                f"{agent_id}_stop_all",
                {
                    "name": f"{name_prefix} Stop all",
                    "unique_id": f"{agent_id}_stop_all",
                    "command_topic": f"{base}/stop_all",
                    "payload_press": "1",
                    "availability_topic": avail,
                    "device": device,
                },
            )
        )
        entities.append(
            (
                "button",
                f"{agent_id}_blackout",
                {
                    "name": f"{name_prefix} Blackout",
                    "unique_id": f"{agent_id}_blackout",
                    "command_topic": f"{base}/blackout",
                    "payload_press": "1",
                    "availability_topic": avail,
                    "device": device,
                },
            )
        )
        entities.append(
            (
                "button",
                f"{agent_id}_sequence_stop",
                {
                    "name": f"{name_prefix} Stop sequence",
                    "unique_id": f"{agent_id}_sequence_stop",
                    "command_topic": f"{base}/sequence/stop",
                    "payload_press": "1",
                    "availability_topic": avail,
                    "device": device,
                },
            )
        )
        entities.append(
            (
                "text",
                f"{agent_id}_sequence_file",
                {
                    "name": f"{name_prefix} Sequence file",
                    "unique_id": f"{agent_id}_sequence_file",
                    "command_topic": f"{base}/sequence/start",
                    "state_topic": f"{base}/state/sequence_file",
                    "availability_topic": avail,
                    "device": device,
                },
            )
        )
        entities.append(
            (
                "select",
                f"{agent_id}_brightness_preset",
                {
                    "name": f"{name_prefix} Brightness preset",
                    "unique_id": f"{agent_id}_brightness_preset",
                    "command_topic": f"{base}/brightness",
                    "state_topic": f"{base}/state/brightness_preset",
                    "options": ["low", "med", "high", "max"],
                    "availability_topic": avail,
                    "device": device,
                },
            )
        )
        entities.append(
            (
                "number",
                f"{agent_id}_brightness",
                {
                    "name": f"{name_prefix} Brightness",
                    "unique_id": f"{agent_id}_brightness",
                    "command_topic": f"{base}/brightness",
                    "state_topic": f"{base}/state/brightness",
                    "min": 1,
                    "max": max_bri,
                    "step": 1,
                    "availability_topic": avail,
                    "device": device,
                },
            )
        )

        if getattr(self._state.settings, "ledfx_base_url", ""):
            entities.append(
                (
                    "text",
                    f"{agent_id}_ledfx_scene_activate",
                    {
                        "name": f"{name_prefix} LedFx Scene Activate",
                        "unique_id": f"{agent_id}_ledfx_scene_activate",
                        "command_topic": f"{base}/ledfx/scene/activate",
                        "availability_topic": avail,
                        "device": device,
                    },
                )
            )
            entities.append(
                (
                    "text",
                    f"{agent_id}_ledfx_scene_deactivate",
                    {
                        "name": f"{name_prefix} LedFx Scene Deactivate",
                        "unique_id": f"{agent_id}_ledfx_scene_deactivate",
                        "command_topic": f"{base}/ledfx/scene/deactivate",
                        "availability_topic": avail,
                        "device": device,
                    },
                )
            )
            entities.append(
                (
                    "text",
                    f"{agent_id}_ledfx_virtual_effect",
                    {
                        "name": f"{name_prefix} LedFx Effect",
                        "unique_id": f"{agent_id}_ledfx_virtual_effect",
                        "command_topic": f"{base}/ledfx/virtual/effect",
                        "availability_topic": avail,
                        "device": device,
                    },
                )
            )
            entities.append(
                (
                    "number",
                    f"{agent_id}_ledfx_virtual_brightness",
                    {
                        "name": f"{name_prefix} LedFx Brightness",
                        "unique_id": f"{agent_id}_ledfx_virtual_brightness",
                        "command_topic": f"{base}/ledfx/virtual/brightness",
                        "min": 0,
                        "max": 255,
                        "step": 1,
                        "availability_topic": avail,
                        "device": device,
                    },
                )
            )

        for component, obj_id, payload in entities:
            topic = f"{prefix}/{component}/{self._ha_object_id(obj_id)}/config"
            await self._publish_topic(client, topic, payload, retain=True)

    async def _publish_state(self, client, suffix: str, payload: Any) -> None:  # type: ignore[no-untyped-def]
        await self._publish(client, f"state/{suffix}", payload, retain=False)

    async def _publish_result(
        self, client, *, action: str, ok: bool, result: Any = None, error: str | None = None
    ) -> None:  # type: ignore[no-untyped-def]
        payload: Dict[str, Any] = {"ok": bool(ok), "action": str(action)}
        if ok:
            payload["result"] = result
        if error:
            payload["error"] = str(error)
        await self._publish(client, "result", payload, retain=False)

    async def _status_payload(self) -> Dict[str, Any]:
        st = self._state
        out: Dict[str, Any] = {"agent_id": str(st.settings.agent_id)}
        try:
            seq = getattr(st, "sequences", None)
            if seq is not None and hasattr(seq, "status"):
                out["sequence"] = (await seq.status()).__dict__
        except Exception:
            pass
        try:
            ddp = getattr(st, "ddp", None)
            if ddp is not None and hasattr(ddp, "status"):
                out["ddp"] = (await ddp.status()).__dict__
        except Exception:
            pass
        try:
            sched = getattr(st, "scheduler", None)
            if sched is not None and hasattr(sched, "status"):
                out["scheduler"] = await sched.status()
        except Exception:
            pass
        return out

    async def _publish_ha_state(self, client) -> None:  # type: ignore[no-untyped-def]
        try:
            sched = getattr(self._state, "scheduler", None)
            if sched is not None and hasattr(sched, "get_config"):
                cfg = await sched.get_config()
                await self._publish_state(
                    client, "scheduler_enabled", "true" if cfg.enabled else "false"
                )
        except Exception:
            pass
        try:
            seq = getattr(self._state, "sequences", None)
            if seq is not None and hasattr(seq, "status"):
                st = await seq.status()
                await self._publish_state(
                    client, "sequence_running", "true" if st.running else "false"
                )
                await self._publish_state(
                    client, "sequence_file", str(st.file or "")
                )
        except Exception:
            pass

    async def _handle_message(self, client, topic: str, payload: bytes) -> None:  # type: ignore[no-untyped-def]
        if not topic.startswith(self._base_topic + "/"):
            return
        suffix = topic[len(self._base_topic) + 1 :]
        if suffix in ("status", "result", "availability"):
            return
        data = _parse_payload(payload)

        st = self._state
        action = suffix
        await self._note_message(action)
        try:
            if suffix == "sequence/start":
                file = None
                loop = False
                if isinstance(data, dict):
                    file = data.get("file") or data.get("sequence")
                    loop = bool(data.get("loop", False))
                else:
                    file = str(data or "").strip()
                if not file:
                    raise ValueError("file is required")
                res = await a2a_service.actions()["start_sequence"](
                    st, {"file": str(file), "loop": bool(loop)}
                )
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                await self._publish_state(client, "sequence_file", str(file))
                await self._publish_state(client, "sequence_running", "true")
                return

            if suffix == "sequence/stop":
                res = await a2a_service.actions()["stop_sequence"](st, {})
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                await self._publish_state(client, "sequence_running", "false")
                await self._publish_state(client, "sequence_file", "")
                return

            if suffix == "brightness":
                if isinstance(data, dict):
                    raw = data.get("brightness", data.get("bri", data.get("value")))
                else:
                    raw = data
                bri = _parse_brightness(raw, max_bri=st.settings.wled_max_bri)
                if bri is None:
                    raise ValueError("brightness is required")
                if st.wled_cooldown is not None:
                    await st.wled_cooldown.wait()
                res = await st.wled.set_brightness(int(bri))
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                if isinstance(raw, str):
                    raw_s = raw.strip().lower()
                    if raw_s in ("low", "med", "medium", "high", "bright", "max", "full"):
                        preset = "med" if raw_s in ("medium",) else raw_s
                        if preset == "bright":
                            preset = "high"
                        if preset == "full":
                            preset = "max"
                        await self._publish_state(client, "brightness_preset", preset)
                await self._publish_state(client, "brightness", int(bri))
                return

            if suffix == "blackout":
                if st.wled_cooldown is not None:
                    await st.wled_cooldown.wait()
                res = await st.wled.turn_off()
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                return

            if suffix == "scheduler/enable":
                enable = _parse_bool(data if not isinstance(data, dict) else data.get("enabled"))
                if enable is None:
                    raise ValueError("enabled must be true/false")
                sched = getattr(st, "scheduler", None)
                if sched is None:
                    raise RuntimeError("Scheduler not initialized")
                cfg = await sched.get_config()
                cfg.enabled = bool(enable)
                await sched.set_config(cfg)
                if enable:
                    await sched.start()
                else:
                    await sched.stop()
                await self._publish_result(
                    client, action=action, ok=True, result={"enabled": bool(enable)}
                )
                await self._note_action(ok=True, action=action, error=None)
                await self._publish_state(
                    client, "scheduler_enabled", "true" if enable else "false"
                )
                return

            if suffix == "scheduler/start":
                sched = getattr(st, "scheduler", None)
                if sched is None:
                    raise RuntimeError("Scheduler not initialized")
                await sched.start()
                await self._publish_result(
                    client, action=action, ok=True, result={"running": True}
                )
                await self._note_action(ok=True, action=action, error=None)
                await self._publish_state(client, "scheduler_enabled", "true")
                return

            if suffix == "scheduler/stop":
                sched = getattr(st, "scheduler", None)
                if sched is None:
                    raise RuntimeError("Scheduler not initialized")
                await sched.stop()
                await self._publish_result(
                    client, action=action, ok=True, result={"running": False}
                )
                await self._note_action(ok=True, action=action, error=None)
                await self._publish_state(client, "scheduler_enabled", "false")
                return

            if suffix == "stop_all":
                res = await a2a_service.actions()["stop_all"](st, {})
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                return

            if suffix == "ledfx/scene/activate":
                if isinstance(data, dict):
                    scene_id = data.get("scene_id") or data.get("scene") or data.get("id")
                else:
                    scene_id = data
                scene_id = str(scene_id or "").strip()
                if not scene_id:
                    raise ValueError("scene_id is required")
                res = await a2a_service.actions()["ledfx_activate_scene"](
                    st, {"scene_id": scene_id}
                )
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                return

            if suffix == "ledfx/scene/deactivate":
                if isinstance(data, dict):
                    scene_id = data.get("scene_id") or data.get("scene") or data.get("id")
                else:
                    scene_id = data
                scene_id = str(scene_id or "").strip()
                if not scene_id:
                    raise ValueError("scene_id is required")
                res = await a2a_service.actions()["ledfx_deactivate_scene"](
                    st, {"scene_id": scene_id}
                )
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                return

            if suffix == "ledfx/virtual/effect":
                params: Dict[str, Any] = {}
                if isinstance(data, dict):
                    params["effect"] = data.get("effect") or data.get("name")
                    if data.get("virtual_id") or data.get("virtual"):
                        params["virtual_id"] = data.get("virtual_id") or data.get("virtual")
                    if isinstance(data.get("config"), dict):
                        params["config"] = data.get("config")
                else:
                    params["effect"] = str(data or "").strip()
                if not params.get("effect"):
                    raise ValueError("effect is required")
                res = await a2a_service.actions()["ledfx_virtual_effect"](st, params)
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                return

            if suffix == "ledfx/virtual/brightness":
                params = {}
                if isinstance(data, dict):
                    val = data.get("brightness") or data.get("value")
                    if data.get("virtual_id") or data.get("virtual"):
                        params["virtual_id"] = data.get("virtual_id") or data.get("virtual")
                else:
                    val = data
                try:
                    params["brightness"] = float(val)
                except Exception:
                    raise ValueError("brightness must be a number")
                res = await a2a_service.actions()["ledfx_virtual_brightness"](st, params)
                await self._publish_result(client, action=action, ok=True, result=res)
                await self._note_action(ok=True, action=action, error=None)
                return

            raise ValueError(f"Unsupported action: {suffix}")
        except Exception as e:
            await self._publish_result(client, action=action, ok=False, error=str(e))
            await self._note_action(ok=False, action=action, error=str(e))

    async def _run(self) -> None:
        try:
            from asyncio_mqtt import Client, MqttError
        except Exception as e:
            log.error("MQTT bridge unavailable: %s", e)
            await self._note_error(str(e))
            return

        if not self._base_topic:
            log.warning("MQTT base topic is empty; skipping bridge startup.")
            return

        try:
            cfg = _parse_mqtt_url(self._state.settings.mqtt_url)
        except Exception as e:
            log.error("MQTT config error: %s", e)
            await self._note_error(str(e))
            return

        tls_ctx = ssl.create_default_context() if cfg.tls else None
        topic_filter = f"{self._base_topic}/#"

        while not self._stop.is_set():
            try:
                async with Client(
                    hostname=cfg.host,
                    port=int(cfg.port),
                    username=self._state.settings.mqtt_username or cfg.username,
                    password=self._state.settings.mqtt_password or cfg.password,
                    tls_context=tls_ctx,
                ) as client:
                    await self._set_connected(True)
                    await self._publish(client, "availability", "online", retain=True)
                    if self._ha_discovery_enabled:
                        await self._publish_ha_discovery(client)
                        await self._publish_ha_state(client)
                    await client.subscribe(topic_filter, qos=self._qos)

                    async def _status_loop() -> None:
                        if self._status_interval_s <= 0:
                            return
                        while not self._stop.is_set():
                            payload = await self._status_payload()
                            await self._publish(
                                client, "status", payload, retain=False
                            )
                            await self._publish_ha_state(client)
                            await asyncio.sleep(float(self._status_interval_s))

                    status_task = asyncio.create_task(_status_loop())

                    async with client.unfiltered_messages() as messages:
                        async for msg in messages:
                            if self._stop.is_set():
                                break
                            try:
                                await self._handle_message(
                                    client,
                                    str(getattr(msg, "topic", "")),
                                    bytes(getattr(msg, "payload", b"") or b""),
                                )
                            except Exception:
                                continue
                    try:
                        status_task.cancel()
                    except Exception:
                        pass
                    await asyncio.gather(status_task, return_exceptions=True)
                await self._set_connected(False)
            except MqttError as e:
                log.warning("MQTT connection failed: %s", e)
                await self._note_error(str(e))
                await self._set_connected(False)
            except Exception as e:
                log.warning("MQTT loop error: %s", e)
                await self._note_error(str(e))
                await self._set_connected(False)

            if self._stop.is_set():
                break
            await asyncio.sleep(float(self._reconnect_s))


async def mqtt_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    mqtt = getattr(state, "mqtt", None)
    enabled = bool(state.settings.mqtt_enabled and state.settings.mqtt_url)
    if mqtt is None:
        return {
            "ok": True,
            "enabled": enabled,
            "running": False,
            "connected": False,
            "base_topic": str(state.settings.mqtt_base_topic or ""),
            "qos": int(state.settings.mqtt_qos or 0),
            "status_interval_s": int(state.settings.mqtt_status_interval_s or 0),
            "reconnect_interval_s": float(
                state.settings.mqtt_reconnect_interval_s or 0
            ),
            "ha_discovery": {
                "enabled": bool(
                    getattr(state.settings, "ha_mqtt_discovery_enabled", False)
                ),
                "prefix": str(
                    getattr(state.settings, "ha_mqtt_discovery_prefix", "homeassistant")
                ),
                "entity_prefix": str(
                    getattr(state.settings, "ha_mqtt_entity_prefix", "")
                ),
            },
            "broker": None,
            "broker_error": None,
            "last_error": None,
            "last_error_at": None,
            "last_connect_at": None,
            "last_disconnect_at": None,
            "last_message_at": None,
            "last_action": None,
            "counters": {"messages_received": 0, "actions_ok": 0, "actions_failed": 0},
            "topics": {
                "base": str(state.settings.mqtt_base_topic or ""),
                "commands": list(_COMMAND_TOPICS),
                "state": list(_STATE_TOPICS),
            },
        }
    res = await mqtt.status()
    res["ok"] = True
    return res

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from fpp_client import AsyncFPPClient
from models.requests import CommandRequest
from services import a2a_service
from services.auth_service import require_a2a_auth
from services.runtime_state_service import persist_runtime_state
from services.state import AppState, get_state
from utils.outbound_http import request_with_retry


def _peer_headers(state: AppState) -> Dict[str, str]:
    key = state.settings.a2a_api_key
    return {"X-A2A-Key": str(key)} if key else {}


async def _peer_get_json(
    *,
    state: AppState,
    peer: Any,
    path: str,
    timeout_s: float,
) -> Dict[str, Any]:
    base_url = str(getattr(peer, "base_url", "") or "").rstrip("/")
    url = base_url + path
    client = state.peer_http
    if client is None:
        return {"ok": False, "error": "peer_http is not initialized"}
    try:
        resp = await request_with_retry(
            client=client,
            method="GET",
            url=url,
            target_kind="peer",
            target=str(getattr(peer, "name", "") or base_url),
            timeout_s=float(timeout_s),
            headers=_peer_headers(state),
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


async def _peer_post_json(
    *,
    state: AppState,
    peer: Any,
    path: str,
    payload: Dict[str, Any],
    timeout_s: float,
) -> Dict[str, Any]:
    base_url = str(getattr(peer, "base_url", "") or "").rstrip("/")
    url = base_url + path
    client = state.peer_http
    if client is None:
        return {"ok": False, "error": "peer_http is not initialized"}
    try:
        resp = await request_with_retry(
            client=client,
            method="POST",
            url=url,
            target_kind="peer",
            target=str(getattr(peer, "name", "") or base_url),
            timeout_s=float(timeout_s),
            headers=_peer_headers(state),
            json_body=payload,
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        body = resp.json()
    except Exception:
        body = {"ok": False, "error": resp.text[:300]}
    if (
        resp.status_code >= 400
        and isinstance(body, dict)
        and body.get("ok") is not False
    ):
        body = {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    return (
        body
        if isinstance(body, dict)
        else {"ok": False, "error": "Non-object response"}
    )


async def _peer_supported_actions(
    *, state: AppState, peer: Any, timeout_s: float
) -> set[str]:
    card = await _peer_get_json(
        state=state, peer=peer, path="/v1/a2a/card", timeout_s=timeout_s
    )
    if not isinstance(card, dict) or card.get("ok") is not True:
        return set()
    agent = card.get("agent") or {}
    caps = agent.get("capabilities") or []
    actions: set[str] = set()
    if isinstance(caps, list):
        for c in caps:
            if isinstance(c, dict) and "action" in c:
                actions.add(str(c.get("action")))
            elif isinstance(c, str):
                actions.add(c)
    return actions


async def _local_command(state: AppState, text: str) -> Dict[str, Any]:
    command_text = (text or "").strip()
    if not command_text:
        return {"ok": False, "error": "Empty command"}
    command_lower = command_text.lower().strip()

    def _parse_int_arg(keys: List[str], *, min_v: int, max_v: int) -> Optional[int]:
        for k in keys:
            m = re.search(
                rf"(?:{re.escape(k)})\\s*(?:=|:)?\\s*(\\d{{1,3}})", command_lower
            )
            if not m:
                continue
            try:
                v = int(m.group(1))
            except Exception:
                continue
            if min_v <= v <= max_v:
                return v
        return None

    def _parse_float_arg(
        keys: List[str], *, min_v: float, max_v: float
    ) -> Optional[float]:
        for k in keys:
            m = re.search(
                rf"(?:{re.escape(k)})\\s*(?:=|:)?\\s*(\\d+(?:\\.\\d+)?)", command_lower
            )
            if not m:
                continue
            try:
                v = float(m.group(1))
            except Exception:
                continue
            if min_v <= v <= max_v:
                return v
        return None

    if command_lower in {"help", "/help", "?"} or command_lower.startswith("help "):
        return {
            "ok": True,
            "mode": "local",
            "response": (
                "Supported commands: status; stop all; apply look [theme] [brightness=1..255]; "
                "start pattern <name> [duration_s=..] [brightness=..] [fps=..] [cw/ccw] [front/right/back/left]; "
                "start sequence <file> [loop]; stop sequence; fpp status; start playlist <name>; stop playlist; trigger event <id>."
            ),
        }

    peers = state.peers or {}

    if "status" in command_lower and "fpp" not in command_lower:
        res = await a2a_service.actions()["status"](state, {})
        return {"ok": True, "mode": "local", "action": "status", "result": res}

    if "stop" in command_lower and "all" in command_lower:
        results: Dict[str, Any] = {}
        try:
            results["self"] = {
                "ok": True,
                "result": await a2a_service.actions()["stop_all"](state, {}),
            }
        except Exception as e:
            results["self"] = {"ok": False, "error": str(e)}

        if peers:
            payload = {"action": "stop_all", "params": {}}
            timeout_s = float(state.settings.a2a_http_timeout_s)
            sem = asyncio.Semaphore(min(8, len(peers)))

            async def _call(peer: Any) -> None:
                async with sem:
                    out = await _peer_post_json(
                        state=state,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=timeout_s,
                    )
                    results[str(getattr(peer, "name", ""))] = out

            await asyncio.gather(*[_call(p) for p in peers.values()])

        try:
            await persist_runtime_state(state, "fleet_stop_all", {"targets": None})
        except Exception:
            pass
        return {"ok": True, "mode": "local", "action": "stop_all", "result": results}

    if ("stop" in command_lower) and (
        "sequence" in command_lower or "seq" in command_lower
    ):
        fleet = getattr(state, "fleet_sequences", None)
        if fleet is not None:
            st = await asyncio.to_thread(fleet.stop)
            return {
                "ok": True,
                "mode": "local",
                "action": "fleet_stop_sequence",
                "status": st.__dict__,
            }
        seq = getattr(state, "sequences", None)
        if seq is None:
            raise RuntimeError("Sequence service not initialized")
        st = await asyncio.to_thread(seq.stop)
        return {
            "ok": True,
            "mode": "local",
            "action": "stop_sequence",
            "status": st.__dict__,
        }

    sequence_match = re.search(
        r"(?:start|play)\\s+(?:fleet\\s+)?(?:sequence|seq)\\s+([^\\s]+)",
        command_text,
        flags=re.IGNORECASE,
    )
    if sequence_match:
        file = sequence_match.group(1).strip()
        loop = (" loop" in command_lower) or (" repeat" in command_lower)
        fleet = getattr(state, "fleet_sequences", None)
        if fleet is not None and "fleet" in command_lower:
            st = await asyncio.to_thread(
                fleet.start,
                file=file,
                loop=loop,
                targets=None,
                include_self=True,
                timeout_s=None,
            )
            return {
                "ok": True,
                "mode": "local",
                "action": "fleet_start_sequence",
                "status": st.__dict__,
            }
        seq = getattr(state, "sequences", None)
        if seq is None:
            raise RuntimeError("Sequence service not initialized")
        st = await asyncio.to_thread(seq.play, file=file, loop=loop)
        return {
            "ok": True,
            "mode": "local",
            "action": "start_sequence",
            "status": st.__dict__,
        }

    if "apply" in command_lower and (
        "look" in command_lower or "theme" in command_lower
    ):
        looks = getattr(state, "looks", None)
        if looks is None:
            raise RuntimeError("Look service not initialized")

        theme: Optional[str] = None
        theme_map = {
            "candy cane": "candy_cane",
            "candy_cane": "candy_cane",
            "classic": "classic",
            "icy": "icy",
            "warm white": "warm_white",
            "warm_white": "warm_white",
            "rainbow": "rainbow",
            "halloween": "halloween",
        }
        for k, v in theme_map.items():
            if k in command_lower:
                theme = v
                break
        brightness = _parse_int_arg(["brightness", "bri"], min_v=1, max_v=255)

        bri_i: Optional[int] = None
        if brightness is not None:
            bri_i = min(state.settings.wled_max_bri, max(1, int(brightness)))

        pack, row = await asyncio.to_thread(
            looks.choose_random, theme=theme, pack_file=None, seed=None
        )

        out: Dict[str, Any] = {
            "picked": {
                "pack_file": pack,
                "id": row.get("id"),
                "name": row.get("name"),
                "theme": row.get("theme"),
            }
        }

        try:
            if state.wled_cooldown is not None:
                await state.wled_cooldown.wait()
            res = await asyncio.to_thread(
                looks.apply_look, row, brightness_override=bri_i
            )
            out["self"] = {"ok": True, "result": res}
        except Exception as e:
            out["self"] = {"ok": False, "error": str(e)}

        if peers:
            timeout_s = float(state.settings.a2a_http_timeout_s)
            caps = await asyncio.gather(
                *[
                    _peer_supported_actions(state=state, peer=p, timeout_s=timeout_s)
                    for p in peers.values()
                ]
            )
            eligible: List[Any] = []
            for peer, actions in zip(peers.values(), caps):
                if "apply_look_spec" in actions:
                    eligible.append(peer)
                else:
                    out[str(getattr(peer, "name", ""))] = {
                        "ok": False,
                        "skipped": True,
                        "reason": "Peer does not support apply_look_spec",
                    }
            if eligible:
                payload = {
                    "action": "apply_look_spec",
                    "params": {"look_spec": row, "brightness_override": bri_i},
                }
                sem = asyncio.Semaphore(min(8, len(eligible)))

                async def _call(peer: Any) -> None:
                    async with sem:
                        res2 = await _peer_post_json(
                            state=state,
                            peer=peer,
                            path="/v1/a2a/invoke",
                            payload=payload,
                            timeout_s=timeout_s,
                        )
                        out[str(getattr(peer, "name", ""))] = res2

                await asyncio.gather(*[_call(p) for p in eligible])

        return {"ok": True, "mode": "local", "action": "apply_look", "result": out}

    ddp_match = re.search(
        r"(?:start|run)\\s+(?:pattern|ddp)\\s+([^\\s]+)",
        command_text,
        flags=re.IGNORECASE,
    )
    if ddp_match:
        pattern = ddp_match.group(1).strip()
        duration_s = _parse_float_arg(
            ["duration", "duration_s"], min_v=0.1, max_v=3600.0
        )
        brightness = _parse_int_arg(["brightness", "bri"], min_v=1, max_v=255)
        fps = _parse_float_arg(["fps"], min_v=1.0, max_v=120.0)

        direction = (
            "cw"
            if " cw" in command_lower
            else ("ccw" if " ccw" in command_lower else None)
        )
        start_pos = None
        for p in ("front", "right", "back", "left"):
            if f" {p}" in command_lower:
                start_pos = p
                break

        params: Dict[str, Any] = {"pattern": pattern}
        if duration_s is not None:
            params["duration_s"] = float(duration_s)
        if brightness is not None:
            params["brightness"] = int(brightness)
        if fps is not None:
            params["fps"] = float(fps)
        if direction:
            params["direction"] = direction
        if start_pos:
            params["start_pos"] = start_pos

        out: Dict[str, Any] = {}
        try:
            out["self"] = {
                "ok": True,
                "result": await a2a_service.actions()["start_ddp_pattern"](
                    state, params
                ),
            }
        except Exception as e:
            out["self"] = {"ok": False, "error": str(e)}

        if peers:
            timeout_s = float(state.settings.a2a_http_timeout_s)
            payload = {"action": "start_ddp_pattern", "params": params}
            sem = asyncio.Semaphore(min(8, len(peers)))

            async def _call(peer: Any) -> None:
                async with sem:
                    res2 = await _peer_post_json(
                        state=state,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=timeout_s,
                    )
                    out[str(getattr(peer, "name", ""))] = res2

            await asyncio.gather(*[_call(p) for p in peers.values()])

        return {
            "ok": True,
            "mode": "local",
            "action": "start_ddp_pattern",
            "result": out,
        }

    if "stop" in command_lower and (
        "pattern" in command_lower or "ddp" in command_lower
    ):
        out: Dict[str, Any] = {}
        try:
            out["self"] = {
                "ok": True,
                "result": await a2a_service.actions()["stop_ddp"](state, {}),
            }
        except Exception as e:
            out["self"] = {"ok": False, "error": str(e)}

        if peers:
            timeout_s = float(state.settings.a2a_http_timeout_s)
            payload = {"action": "stop_ddp", "params": {}}
            sem = asyncio.Semaphore(min(8, len(peers)))

            async def _call(peer: Any) -> None:
                async with sem:
                    res2 = await _peer_post_json(
                        state=state,
                        peer=peer,
                        path="/v1/a2a/invoke",
                        payload=payload,
                        timeout_s=timeout_s,
                    )
                    out[str(getattr(peer, "name", ""))] = res2

            await asyncio.gather(*[_call(p) for p in peers.values()])

        return {"ok": True, "mode": "local", "action": "stop_ddp", "result": out}

    # ---- FPP helpers ----

    if "fpp" in command_lower and "status" in command_lower:
        if not state.settings.fpp_base_url:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        if state.peer_http is None:
            raise RuntimeError("HTTP client not initialized")
        fpp = AsyncFPPClient(
            base_url=state.settings.fpp_base_url,
            client=state.peer_http,
            timeout_s=float(state.settings.fpp_http_timeout_s),
            headers={k: v for (k, v) in state.settings.fpp_headers},
        )
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_status",
            "result": (await fpp.status()).as_dict(),
        }

    playlist_match = re.search(
        r"start\\s+playlist\\s+(.+)$", command_text, flags=re.IGNORECASE
    )
    if playlist_match:
        if not state.settings.fpp_base_url:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        if state.peer_http is None:
            raise RuntimeError("HTTP client not initialized")
        name = playlist_match.group(1).strip()
        if not name:
            return {"ok": False, "error": "Playlist name is required."}
        fpp = AsyncFPPClient(
            base_url=state.settings.fpp_base_url,
            client=state.peer_http,
            timeout_s=float(state.settings.fpp_http_timeout_s),
            headers={k: v for (k, v) in state.settings.fpp_headers},
        )
        res = (
            await fpp.start_playlist(name, repeat=("repeat" in command_lower))
        ).as_dict()
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_start_playlist",
            "result": res,
        }

    if "stop" in command_lower and "playlist" in command_lower:
        if not state.settings.fpp_base_url:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        if state.peer_http is None:
            raise RuntimeError("HTTP client not initialized")
        fpp = AsyncFPPClient(
            base_url=state.settings.fpp_base_url,
            client=state.peer_http,
            timeout_s=float(state.settings.fpp_http_timeout_s),
            headers={k: v for (k, v) in state.settings.fpp_headers},
        )
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_stop_playlist",
            "result": (await fpp.stop_playlist()).as_dict(),
        }

    event_match = re.search(r"(?:trigger\\s+event|event)\\s+(\\d+)", command_lower)
    if event_match:
        if not state.settings.fpp_base_url:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        if state.peer_http is None:
            raise RuntimeError("HTTP client not initialized")
        eid = int(event_match.group(1))
        fpp = AsyncFPPClient(
            base_url=state.settings.fpp_base_url,
            client=state.peer_http,
            timeout_s=float(state.settings.fpp_http_timeout_s),
            headers={k: v for (k, v) in state.settings.fpp_headers},
        )
        return {
            "ok": True,
            "mode": "local",
            "action": "fpp_trigger_event",
            "result": (await fpp.trigger_event(eid)).as_dict(),
        }

    return {"ok": False, "error": "Unrecognized command (try 'help')"}


async def command(
    req: CommandRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    director = getattr(state, "director", None)
    if director is not None:
        try:
            return await asyncio.to_thread(director.run, req.text)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    try:
        return await _local_command(state, req.text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

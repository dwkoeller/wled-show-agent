from __future__ import annotations

from typing import Any, Dict

from services import a2a_service, fleet_service, fleet_sequences_service
from services.state import AppState
from utils.outbound_http import retry_policy_from_settings


def create_director(*, state: AppState) -> Any | None:
    """
    Optional OpenAI director wiring.

    Returns an `openai_agent.SimpleDirectorAgent` instance when OPENAI_API_KEY is set,
    otherwise None.
    """
    settings = state.settings
    if not settings.openai_api_key:
        return None

    try:
        from openai_agent import SimpleDirectorAgent
    except Exception:
        return None

    async def _tool_apply_random_look(kwargs: Dict[str, Any]) -> Any:
        from models.requests import FleetApplyRandomLookRequest

        req = FleetApplyRandomLookRequest(
            theme=kwargs.get("theme"),
            brightness=kwargs.get("brightness"),
            seed=kwargs.get("seed"),
            include_self=True,
        )
        return await fleet_service.fleet_apply_random_look(
            req, request=None, state=state
        )

    async def _tool_start_ddp_pattern(kwargs: Dict[str, Any]) -> Any:
        return await a2a_service.actions()["start_ddp_pattern"](
            state, dict(kwargs or {})
        )

    async def _tool_stop_ddp(kwargs: Dict[str, Any]) -> Any:
        return await a2a_service.actions()["stop_ddp"](state, dict(kwargs or {}))

    async def _tool_stop_all(kwargs: Dict[str, Any]) -> Any:
        return await a2a_service.actions()["stop_all"](state, dict(kwargs or {}))

    async def _tool_generate_looks_pack(kwargs: Dict[str, Any]) -> Any:
        looks = getattr(state, "looks", None)
        if looks is None:
            return {"ok": False, "error": "Look service not initialized"}
        total = int(kwargs.get("total_looks", 800))
        themes = kwargs.get("themes") or [
            "classic",
            "candy_cane",
            "icy",
            "warm_white",
            "rainbow",
        ]
        bri = int(kwargs.get("brightness", settings.wled_max_bri))
        seed = int(kwargs.get("seed", 1337))
        summary = await looks.generate_pack(
            total_looks=total,
            themes=themes,
            brightness=bri,
            seed=seed,
            write_files=True,
            include_multi_segment=True,
        )
        return summary.__dict__

    async def _tool_fleet_start_sequence(kwargs: Dict[str, Any]) -> Any:
        svc = getattr(state, "fleet_sequences", None)
        if svc is None:
            return {"ok": False, "error": "Fleet sequences are not available."}
        file = str(kwargs.get("file") or kwargs.get("sequence_file") or "").strip()
        if not file:
            return {"ok": False, "error": "Missing 'file'."}
        targets = kwargs.get("targets")
        if targets is not None and not isinstance(targets, list):
            targets = None
        include_self = bool(kwargs.get("include_self", True))
        loop_flag = bool(kwargs.get("loop", False))
        timeout_s = kwargs.get("timeout_s")
        try:
            st2 = await svc.start(
                file=file,
                loop=loop_flag,
                targets=[str(x) for x in (targets or [])] if targets else None,
                include_self=include_self,
                timeout_s=float(timeout_s) if timeout_s is not None else None,
            )
            return {"ok": True, "status": st2.__dict__}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def _tool_fleet_stop_sequence(_: Dict[str, Any]) -> Any:
        svc = getattr(state, "fleet_sequences", None)
        if svc is None:
            return {"ok": False, "error": "Fleet sequences are not available."}
        try:
            st2 = await svc.stop()
            return {"ok": True, "status": st2.__dict__}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def _tool_fleet_start_sequence_staggered(kwargs: Dict[str, Any]) -> Any:
        from models.requests import FleetSequenceStaggeredStartRequest

        file = str(kwargs.get("file") or "").strip()
        if not file:
            return {"ok": False, "error": "Missing 'file'."}
        req = FleetSequenceStaggeredStartRequest(
            file=file,
            loop=bool(kwargs.get("loop", False)),
            targets=kwargs.get("targets"),
            include_self=bool(kwargs.get("include_self", True)),
            stagger_s=float(kwargs.get("stagger_s", 0.5)),
            start_delay_s=float(kwargs.get("start_delay_s", 0.0)),
            timeout_s=kwargs.get("timeout_s"),
        )
        return await fleet_sequences_service.fleet_sequences_start_staggered(
            req, state=state
        )

    async def _tool_orchestration_start(kwargs: Dict[str, Any]) -> Any:
        from models.requests import OrchestrationStartRequest

        svc = getattr(state, "orchestrator", None)
        if svc is None:
            return {"ok": False, "error": "Orchestration service not available."}
        try:
            req = OrchestrationStartRequest(
                name=kwargs.get("name"),
                steps=kwargs.get("steps") or [],
                loop=bool(kwargs.get("loop", False)),
            )
        except Exception as e:
            return {"ok": False, "error": str(e)}
        st2 = await svc.start(name=req.name, steps=list(req.steps), loop=req.loop)
        return {"ok": True, "status": st2.__dict__}

    async def _tool_orchestration_stop(_: Dict[str, Any]) -> Any:
        svc = getattr(state, "orchestrator", None)
        if svc is None:
            return {"ok": False, "error": "Orchestration service not available."}
        st2 = await svc.stop()
        return {"ok": True, "status": st2.__dict__}

    async def _tool_fleet_orchestration_start(kwargs: Dict[str, Any]) -> Any:
        from models.requests import FleetOrchestrationStartRequest

        svc = getattr(state, "fleet_orchestrator", None)
        if svc is None:
            return {
                "ok": False,
                "error": "Fleet orchestration service not available.",
            }
        try:
            req = FleetOrchestrationStartRequest(
                name=kwargs.get("name"),
                steps=kwargs.get("steps") or [],
                loop=bool(kwargs.get("loop", False)),
                targets=kwargs.get("targets"),
                include_self=bool(kwargs.get("include_self", True)),
                timeout_s=kwargs.get("timeout_s"),
            )
        except Exception as e:
            return {"ok": False, "error": str(e)}
        st2 = await svc.start(
            name=req.name,
            steps=list(req.steps),
            loop=req.loop,
            targets=req.targets,
            include_self=req.include_self,
            timeout_s=req.timeout_s,
        )
        return {"ok": True, "status": st2.__dict__}

    async def _tool_fleet_orchestration_stop(_: Dict[str, Any]) -> Any:
        svc = getattr(state, "fleet_orchestrator", None)
        if svc is None:
            return {
                "ok": False,
                "error": "Fleet orchestration service not available.",
            }
        st2 = await svc.stop()
        return {"ok": True, "status": st2.__dict__}

    async def _tool_fpp_start_playlist(kwargs: Dict[str, Any]) -> Any:
        if not settings.fpp_base_url:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        name = str(kwargs.get("name") or "").strip()
        if not name:
            return {"ok": False, "error": "Missing 'name'."}
        repeat = bool(kwargs.get("repeat", False))

        from fpp_client import AsyncFPPClient

        if state.peer_http is None:
            return {"ok": False, "error": "HTTP client not initialized"}
        fpp = AsyncFPPClient(
            base_url=settings.fpp_base_url,
            client=state.peer_http,
            timeout_s=float(settings.fpp_http_timeout_s),
            headers={k: v for (k, v) in settings.fpp_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        resp = await fpp.start_playlist(name, repeat=repeat)
        return {"ok": True, "fpp": resp.as_dict()}

    async def _tool_fpp_stop_playlist(_: Dict[str, Any]) -> Any:
        if not settings.fpp_base_url:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        from fpp_client import AsyncFPPClient

        if state.peer_http is None:
            return {"ok": False, "error": "HTTP client not initialized"}
        fpp = AsyncFPPClient(
            base_url=settings.fpp_base_url,
            client=state.peer_http,
            timeout_s=float(settings.fpp_http_timeout_s),
            headers={k: v for (k, v) in settings.fpp_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        resp = await fpp.stop_playlist()
        return {"ok": True, "fpp": resp.as_dict()}

    async def _tool_fpp_trigger_event(kwargs: Dict[str, Any]) -> Any:
        if not settings.fpp_base_url:
            return {"ok": False, "error": "FPP is not configured; set FPP_BASE_URL."}
        try:
            event_id = int(kwargs.get("event_id"))
        except Exception:
            return {"ok": False, "error": "event_id must be an integer > 0"}

        from fpp_client import AsyncFPPClient

        if state.peer_http is None:
            return {"ok": False, "error": "HTTP client not initialized"}
        fpp = AsyncFPPClient(
            base_url=settings.fpp_base_url,
            client=state.peer_http,
            timeout_s=float(settings.fpp_http_timeout_s),
            headers={k: v for (k, v) in settings.fpp_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        resp = await fpp.trigger_event(event_id)
        return {"ok": True, "fpp": resp.as_dict()}

    async def _resolve_ledfx_virtual_id(virtual_id: Any) -> str | None:
        vid = str(virtual_id or "").strip()
        if vid:
            return vid
        if not settings.ledfx_base_url:
            return None
        if state.peer_http is None:
            return None
        from ledfx_client import AsyncLedFxClient

        ledfx = AsyncLedFxClient(
            base_url=settings.ledfx_base_url,
            client=state.peer_http,
            timeout_s=float(settings.ledfx_http_timeout_s),
            headers={k: v for (k, v) in settings.ledfx_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        try:
            resp = await ledfx.virtuals()
        except Exception:
            return None
        body = resp.body
        raw = None
        if isinstance(body, dict):
            raw = body.get("virtuals")
            if raw is None:
                data = body.get("data")
                if isinstance(data, dict):
                    raw = data.get("virtuals")
        if raw is None:
            raw = body
        ids: list[str] = []
        if isinstance(raw, dict):
            ids = [str(k) for k in raw.keys()]
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict):
                    candidate = item.get("id") or item.get("name")
                    if candidate:
                        ids.append(str(candidate))
                elif item:
                    ids.append(str(item))
        ids = [x for x in ids if x.strip()]
        if len(ids) == 1:
            return ids[0]
        return None

    async def _tool_ledfx_activate_scene(kwargs: Dict[str, Any]) -> Any:
        if not settings.ledfx_base_url:
            return {"ok": False, "error": "LedFx is not configured; set LEDFX_BASE_URL."}
        scene_id = str(kwargs.get("scene_id") or kwargs.get("name") or "").strip()
        if not scene_id:
            return {"ok": False, "error": "Missing 'scene_id'."}
        from ledfx_client import AsyncLedFxClient

        if state.peer_http is None:
            return {"ok": False, "error": "HTTP client not initialized"}
        ledfx = AsyncLedFxClient(
            base_url=settings.ledfx_base_url,
            client=state.peer_http,
            timeout_s=float(settings.ledfx_http_timeout_s),
            headers={k: v for (k, v) in settings.ledfx_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        resp = await ledfx.activate_scene(scene_id)
        try:
            from services.ledfx_service import _record_last_applied

            await _record_last_applied(
                state,
                kind="ledfx_scene",
                name=scene_id,
                file=None,
                payload={"action": "activate", "scene_id": scene_id},
            )
        except Exception:
            pass
        return {"ok": True, "ledfx": resp.as_dict()}

    async def _tool_ledfx_deactivate_scene(kwargs: Dict[str, Any]) -> Any:
        if not settings.ledfx_base_url:
            return {"ok": False, "error": "LedFx is not configured; set LEDFX_BASE_URL."}
        scene_id = str(kwargs.get("scene_id") or kwargs.get("name") or "").strip()
        if not scene_id:
            return {"ok": False, "error": "Missing 'scene_id'."}
        from ledfx_client import AsyncLedFxClient

        if state.peer_http is None:
            return {"ok": False, "error": "HTTP client not initialized"}
        ledfx = AsyncLedFxClient(
            base_url=settings.ledfx_base_url,
            client=state.peer_http,
            timeout_s=float(settings.ledfx_http_timeout_s),
            headers={k: v for (k, v) in settings.ledfx_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        resp = await ledfx.deactivate_scene(scene_id)
        try:
            from services.ledfx_service import _record_last_applied

            await _record_last_applied(
                state,
                kind="ledfx_scene",
                name=scene_id,
                file=None,
                payload={"action": "deactivate", "scene_id": scene_id},
            )
        except Exception:
            pass
        return {"ok": True, "ledfx": resp.as_dict()}

    async def _tool_ledfx_set_virtual_effect(kwargs: Dict[str, Any]) -> Any:
        if not settings.ledfx_base_url:
            return {"ok": False, "error": "LedFx is not configured; set LEDFX_BASE_URL."}
        effect = str(kwargs.get("effect") or "").strip()
        if not effect:
            return {"ok": False, "error": "Missing 'effect'."}
        vid = await _resolve_ledfx_virtual_id(kwargs.get("virtual_id"))
        if not vid:
            return {"ok": False, "error": "Missing 'virtual_id'."}
        config = kwargs.get("config")
        cfg = dict(config) if isinstance(config, dict) else {}
        from ledfx_client import AsyncLedFxClient

        if state.peer_http is None:
            return {"ok": False, "error": "HTTP client not initialized"}
        ledfx = AsyncLedFxClient(
            base_url=settings.ledfx_base_url,
            client=state.peer_http,
            timeout_s=float(settings.ledfx_http_timeout_s),
            headers={k: v for (k, v) in settings.ledfx_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        resp = await ledfx.set_virtual_effect(
            virtual_id=vid,
            effect=effect,
            config=cfg,
        )
        try:
            from services.ledfx_service import _record_last_applied

            await _record_last_applied(
                state,
                kind="ledfx_effect",
                name=effect,
                file=vid,
                payload={"virtual_id": vid, "effect": effect, "config": cfg},
            )
        except Exception:
            pass
        return {"ok": True, "ledfx": resp.as_dict()}

    async def _tool_ledfx_set_virtual_brightness(kwargs: Dict[str, Any]) -> Any:
        if not settings.ledfx_base_url:
            return {"ok": False, "error": "LedFx is not configured; set LEDFX_BASE_URL."}
        try:
            brightness = float(kwargs.get("brightness"))
        except Exception:
            return {"ok": False, "error": "brightness must be a number"}
        vid = await _resolve_ledfx_virtual_id(kwargs.get("virtual_id"))
        if not vid:
            return {"ok": False, "error": "Missing 'virtual_id'."}
        primary = max(0.0, brightness)
        fallback: float | None = None
        if primary > 1.0:
            raw = min(255.0, primary)
            primary = max(0.0, min(1.0, raw / 255.0))
            fallback = raw
        from ledfx_client import AsyncLedFxClient

        if state.peer_http is None:
            return {"ok": False, "error": "HTTP client not initialized"}
        ledfx = AsyncLedFxClient(
            base_url=settings.ledfx_base_url,
            client=state.peer_http,
            timeout_s=float(settings.ledfx_http_timeout_s),
            headers={k: v for (k, v) in settings.ledfx_headers},
            retry=retry_policy_from_settings(state.settings),
        )
        resp = await ledfx.set_virtual_brightness(
            virtual_id=vid,
            brightness=primary,
            fallback_brightness=fallback,
        )
        try:
            from services.ledfx_service import _record_last_applied

            await _record_last_applied(
                state,
                kind="ledfx_brightness",
                name=str(brightness),
                file=vid,
                payload={"virtual_id": vid, "brightness": float(brightness)},
            )
        except Exception:
            pass
        return {"ok": True, "ledfx": resp.as_dict()}

    return SimpleDirectorAgent(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        tools={
            "apply_random_look": _tool_apply_random_look,
            "start_ddp_pattern": _tool_start_ddp_pattern,
            "stop_ddp": _tool_stop_ddp,
            "stop_all": _tool_stop_all,
            "generate_looks_pack": _tool_generate_looks_pack,
            "fleet_start_sequence": _tool_fleet_start_sequence,
            "fleet_stop_sequence": _tool_fleet_stop_sequence,
            "fleet_start_sequence_staggered": _tool_fleet_start_sequence_staggered,
            "orchestration_start": _tool_orchestration_start,
            "orchestration_stop": _tool_orchestration_stop,
            "fleet_orchestration_start": _tool_fleet_orchestration_start,
            "fleet_orchestration_stop": _tool_fleet_orchestration_stop,
            "fpp_start_playlist": _tool_fpp_start_playlist,
            "fpp_stop_playlist": _tool_fpp_stop_playlist,
            "fpp_trigger_event": _tool_fpp_trigger_event,
            "ledfx_activate_scene": _tool_ledfx_activate_scene,
            "ledfx_deactivate_scene": _tool_ledfx_deactivate_scene,
            "ledfx_set_virtual_effect": _tool_ledfx_set_virtual_effect,
            "ledfx_set_virtual_brightness": _tool_ledfx_set_virtual_brightness,
        },
    )

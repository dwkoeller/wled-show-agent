from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import Depends, HTTPException, Request

from ddp_control import prepare_ddp_params
from models.requests import DDPStartRequest
from orientation import infer_orientation, OrientationInfo
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _require_ddp(state: AppState):
    ddp = getattr(state, "ddp", None)
    if ddp is None:
        raise HTTPException(status_code=503, detail="DDP streamer not initialized")
    return ddp


async def _get_orientation(
    state: AppState, *, refresh: bool
) -> Optional[OrientationInfo]:
    settings = state.settings
    ordered = list(state.segment_ids or [])
    if not ordered:
        ordered = list(getattr(settings, "wled_segment_ids", []) or [])
    try:
        from segment_layout import fetch_segment_layout_async

        layout = await fetch_segment_layout_async(
            state.wled,
            segment_ids=list(state.segment_ids or []),
            refresh=bool(refresh),
        )
        if layout and getattr(layout, "segments", None):
            ordered = list(layout.ordered_ids())
    except Exception:
        ordered = list(state.segment_ids or [])

    if not ordered:
        return None

    try:
        return infer_orientation(
            ordered_segment_ids=[int(x) for x in ordered],
            right_segment_id=int(settings.quad_right_segment_id),
            order_direction_from_street=str(settings.quad_order_from_street),
        )
    except Exception:
        return None


async def ddp_patterns(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    from patterns import PatternFactory

    try:
        ddp = _require_ddp(state)
        info = await state.wled.device_info()
        layout = None
        try:
            from segment_layout import fetch_segment_layout_async

            layout = await fetch_segment_layout_async(
                state.wled,
                segment_ids=list(state.segment_ids or []),
                refresh=False,
            )
        except Exception:
            layout = None

        factory = PatternFactory(
            led_count=int(info.led_count),
            geometry=ddp.geometry,
            segment_layout=layout,
        )
        res = {
            "ok": True,
            "patterns": factory.available(),
            "geometry_enabled": bool(ddp.geometry.enabled_for(int(info.led_count))),
        }
        await log_event(
            state,
            action="ddp.patterns",
            ok=True,
            payload={"count": len(res["patterns"])},
            request=request,
        )
        return res
    except HTTPException:
        raise
    except Exception as e:
        await log_event(
            state, action="ddp.patterns", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=500, detail=str(e))


async def ddp_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    ddp = _require_ddp(state)
    try:
        st = await ddp.status()
        await log_event(state, action="ddp.status", ok=True, request=request)
        return {"ok": True, "status": st.__dict__}
    except HTTPException as e:
        await log_event(
            state,
            action="ddp.status",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="ddp.status", ok=False, error=str(e), request=request
        )
        raise


async def ddp_start(
    req: DDPStartRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    ddp = _require_ddp(state)
    try:
        # Merge top-level friendly controls into params for convenience
        params = dict(req.params or {})
        if req.direction and "direction" not in params:
            params["direction"] = req.direction
        if req.start_pos and "start_pos" not in params:
            params["start_pos"] = req.start_pos

        ori = await _get_orientation(state, refresh=False)
        params = prepare_ddp_params(
            pattern=req.pattern,
            params=params,
            orientation=ori,
            default_start_pos=str(state.settings.quad_default_start_pos),
        )

        st = await ddp.start(
            pattern=req.pattern,
            params=params,
            duration_s=req.duration_s,
            brightness=min(state.settings.wled_max_bri, req.brightness),
            fps=req.fps,
        )

        # Best-effort runtime state + DB metadata.
        try:
            from services.runtime_state_service import persist_runtime_state

            await persist_runtime_state(state, "ddp_start", {"pattern": req.pattern})
        except Exception:
            pass

        if state.db is not None:
            try:
                await state.db.set_last_applied(
                    kind="ddp",
                    name=str(req.pattern),
                    file=None,
                    payload={"pattern": req.pattern, "params": dict(params or {})},
                )
                try:
                    from services.events_service import emit_event

                    await emit_event(
                        state,
                        event_type="meta",
                        data={
                            "event": "last_applied",
                            "kind": "ddp",
                            "name": str(req.pattern),
                            "pattern": str(req.pattern),
                        },
                    )
                except Exception:
                    pass
            except Exception:
                pass

        await log_event(
            state,
            action="ddp.start",
            ok=True,
            resource=str(req.pattern),
            payload={"duration_s": req.duration_s},
            request=request,
        )
        return {"ok": True, "status": st.__dict__}
    except HTTPException as e:
        await log_event(
            state,
            action="ddp.start",
            ok=False,
            resource=str(req.pattern),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="ddp.start",
            ok=False,
            resource=str(req.pattern),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def ddp_stop(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    ddp = _require_ddp(state)
    try:
        st = await ddp.stop()
        try:
            from services.runtime_state_service import persist_runtime_state

            await persist_runtime_state(state, "ddp_stop")
        except Exception:
            pass
        await log_event(state, action="ddp.stop", ok=True, request=request)
        return {"ok": True, "status": st.__dict__}
    except HTTPException as e:
        await log_event(
            state,
            action="ddp.stop",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="ddp.stop", ok=False, error=str(e), request=request
        )
        raise

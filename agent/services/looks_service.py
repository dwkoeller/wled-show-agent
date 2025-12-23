from __future__ import annotations

from typing import Any, Dict

from fastapi import Depends, HTTPException, Request

from models.requests import ApplyRandomLookRequest, GenerateLooksRequest
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _require_looks(state: AppState):
    svc = getattr(state, "looks", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="Look service not initialized")
    return svc


async def looks_generate(
    req: GenerateLooksRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_looks(state)
        summary = await svc.generate_pack(
            total_looks=req.total_looks,
            themes=req.themes,
            brightness=min(state.settings.wled_max_bri, req.brightness),
            seed=req.seed,
            write_files=req.write_files,
            include_multi_segment=req.include_multi_segment,
        )
        await log_event(
            state,
            action="looks.generate",
            ok=True,
            resource=str(summary.file),
            payload={"total_looks": req.total_looks},
            request=request,
        )
        return {"ok": True, "summary": summary.__dict__}
    except HTTPException as e:
        await log_event(
            state,
            action="looks.generate",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="looks.generate", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=400, detail=str(e))


async def looks_packs(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_looks(state)
        packs = await svc.list_packs()
        latest = await svc.latest_pack()
        await log_event(
            state,
            action="looks.packs",
            ok=True,
            payload={"count": len(packs), "latest": latest},
            request=request,
        )
        return {"ok": True, "packs": packs, "latest": latest}
    except HTTPException as e:
        await log_event(
            state,
            action="looks.packs",
            ok=False,
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state, action="looks.packs", ok=False, error=str(e), request=request
        )
        raise


async def looks_apply_random(
    req: ApplyRandomLookRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_looks(state)
        cd = getattr(state, "wled_cooldown", None)
        if cd is not None:
            await cd.wait()
        pack, row = await svc.choose_random(
            theme=req.theme, pack_file=req.pack_file, seed=req.seed
        )
        out = await svc.apply_look(row, brightness_override=req.brightness)

        if state.db is not None:
            try:
                await state.db.set_last_applied(
                    kind="look",
                    name=str(out.get("name") or "") or None,
                    file=str(pack) if pack else None,
                    payload={
                        "look": dict(row or {}),
                        "result": dict(out or {}),
                        "pack_file": str(pack) if pack else None,
                    },
                )
                try:
                    from services.events_service import emit_event

                    await emit_event(
                        state,
                        event_type="meta",
                        data={
                            "event": "last_applied",
                            "kind": "look",
                            "name": str(out.get("name") or "") or None,
                            "file": str(pack) if pack else None,
                        },
                    )
                except Exception:
                    pass
            except Exception:
                pass

        await log_event(
            state,
            action="looks.apply_random",
            ok=True,
            resource=str(pack) if pack else None,
            payload={"theme": req.theme, "seed": req.seed},
            request=request,
        )
        return {"ok": True, "result": out}
    except HTTPException as e:
        await log_event(
            state,
            action="looks.apply_random",
            ok=False,
            error=str(getattr(e, "detail", e)),
            payload={"theme": req.theme},
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="looks.apply_random",
            ok=False,
            error=str(e),
            payload={"theme": req.theme},
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))

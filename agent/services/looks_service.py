from __future__ import annotations

import asyncio
from typing import Any, Dict

from fastapi import Depends, HTTPException

from models.requests import ApplyRandomLookRequest, GenerateLooksRequest
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _require_looks(state: AppState):
    svc = getattr(state, "looks", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="Look service not initialized")
    return svc


async def looks_generate(
    req: GenerateLooksRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_looks(state)
        summary = await asyncio.to_thread(
            svc.generate_pack,
            total_looks=req.total_looks,
            themes=req.themes,
            brightness=min(state.settings.wled_max_bri, req.brightness),
            seed=req.seed,
            write_files=req.write_files,
            include_multi_segment=req.include_multi_segment,
        )
        return {"ok": True, "summary": summary.__dict__}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def looks_packs(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    svc = _require_looks(state)
    packs = await asyncio.to_thread(svc.list_packs)
    latest = await asyncio.to_thread(svc.latest_pack)
    return {"ok": True, "packs": packs, "latest": latest}


async def looks_apply_random(
    req: ApplyRandomLookRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        svc = _require_looks(state)
        cd = getattr(state, "wled_cooldown", None)
        if cd is not None:
            await cd.wait()
        pack, row = await asyncio.to_thread(
            svc.choose_random,
            theme=req.theme,
            pack_file=req.pack_file,
            seed=req.seed,
        )
        out = await asyncio.to_thread(
            svc.apply_look, row, brightness_override=req.brightness
        )

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
            except Exception:
                pass

        return {"ok": True, "result": out}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException

from models.requests import (
    ShowConfigLoadRequest,
    XlightsImportNetworksRequest,
    XlightsImportProjectRequest,
    XlightsImportSequenceRequest,
)
from pack_io import write_json
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from show_config import load_show_config, write_show_config
from xlights_import import (
    import_xlights_models_file,
    import_xlights_networks_file,
    show_config_from_xlights_networks,
    show_config_from_xlights_project,
)
from xlights_sequence_import import (
    XlightsSequenceImportError,
    import_xlights_xsq_timing_file,
)


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


async def show_config_load(
    req: ShowConfigLoadRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        cfg = await asyncio.to_thread(
            load_show_config, data_dir=state.settings.data_dir, rel_path=req.file
        )
        return {"ok": True, "config": cfg.as_dict()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def xlights_import_networks(
    req: XlightsImportNetworksRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        networks_path = _resolve_data_path(state, req.networks_file)
        controllers = await asyncio.to_thread(
            import_xlights_networks_file, str(networks_path)
        )
        cfg = await asyncio.to_thread(
            show_config_from_xlights_networks,
            networks=controllers,
            show_name=req.show_name,
            subnet=req.subnet,
            coordinator_base_url=req.coordinator_base_url,
            fpp_base_url=req.fpp_base_url,
        )
        out_path = await asyncio.to_thread(
            write_show_config,
            data_dir=state.settings.data_dir,
            rel_path=req.out_file,
            config=cfg,
        )
        return {
            "ok": True,
            "controllers": [
                {
                    "name": c.name,
                    "host": c.host,
                    "protocol": c.protocol,
                    "universe_start": c.universe_start,
                    "pixel_count": c.pixel_count,
                }
                for c in controllers
            ],
            "show_config_file": out_path,
            "show_config": cfg.as_dict(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def xlights_import_project(
    req: XlightsImportProjectRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        proj = _resolve_data_path(state, req.project_dir)
        if not proj.is_dir():
            raise HTTPException(
                status_code=400, detail="project_dir must be a directory under DATA_DIR"
            )

        proj_root = proj.resolve()

        def _pick(name: Optional[str], defaults: List[str]) -> Path:
            candidates = [name] if name else []
            candidates.extend([d for d in defaults if d not in candidates])
            for n in candidates:
                if not n:
                    continue
                p = (proj_root / n).resolve()
                if proj_root not in p.parents:
                    continue
                if p.is_file():
                    return p
            raise HTTPException(
                status_code=400,
                detail=f"Missing required xLights file (tried: {', '.join(candidates)})",
            )

        networks_path = _pick(req.networks_file, ["xlights_networks.xml"])
        models_path = _pick(
            req.models_file,
            ["xlights_rgbeffects.xml", "xlights_models.xml", "xlights_layout.xml"],
        )

        controllers = await asyncio.to_thread(
            import_xlights_networks_file, str(networks_path)
        )
        models = await asyncio.to_thread(import_xlights_models_file, str(models_path))

        cfg = await asyncio.to_thread(
            show_config_from_xlights_project,
            networks=controllers,
            models=models,
            show_name=req.show_name,
            subnet=req.subnet,
            coordinator_base_url=req.coordinator_base_url,
            fpp_base_url=req.fpp_base_url,
            include_controllers=bool(req.include_controllers),
            include_models=bool(req.include_models),
        )

        out_path = await asyncio.to_thread(
            write_show_config,
            data_dir=state.settings.data_dir,
            rel_path=req.out_file,
            config=cfg,
        )
        return {
            "ok": True,
            "project_dir": str(proj_root),
            "networks_file": networks_path.name,
            "models_file": models_path.name,
            "controllers": [
                {
                    "name": c.name,
                    "host": c.host,
                    "protocol": c.protocol,
                    "universe_start": c.universe_start,
                    "pixel_count": c.pixel_count,
                }
                for c in controllers
            ],
            "models": [
                {
                    "name": m.name,
                    "start_channel": m.start_channel,
                    "channel_count": m.channel_count,
                    "pixel_count": m.pixel_count,
                }
                for m in models
            ],
            "show_config_file": out_path,
            "show_config": cfg.as_dict(),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def xlights_import_sequence(
    req: XlightsImportSequenceRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Import a beat/timing grid from an xLights `.xsq` file.
    """
    try:
        xsq_path = _resolve_data_path(state, req.xsq_file)
        out_path = _resolve_data_path(state, req.out_file)

        analysis = await asyncio.to_thread(
            import_xlights_xsq_timing_file,
            xsq_path=str(xsq_path),
            timing_track=req.timing_track,
        )
        await asyncio.to_thread(write_json, str(out_path), analysis)

        base = Path(state.settings.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )
        return {"ok": True, "analysis": analysis, "out_file": rel_out}
    except HTTPException:
        raise
    except XlightsSequenceImportError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

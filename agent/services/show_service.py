from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from aiofiles import os as aio_os
from fastapi import Depends, HTTPException, Request

from models.requests import (
    ShowConfigLoadRequest,
    XlightsImportNetworksRequest,
    XlightsImportProjectRequest,
    XlightsImportSequenceRequest,
)
from pack_io import write_json_async
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from show_config import ShowConfig, load_show_config_async, write_show_config_async
from utils.blocking import run_cpu_blocking_state
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


def _rel_show_config_path(state: AppState, path_s: str) -> str:
    base = Path(state.settings.data_dir).resolve()
    show_root = (base / "show").resolve()
    p = Path(path_s).resolve()
    try:
        return str(p.relative_to(show_root))
    except Exception:
        try:
            return str(p.relative_to(base))
        except Exception:
            return str(p)


def _show_config_payload(cfg: ShowConfig) -> Dict[str, Any]:
    props_by_kind: Dict[str, int] = {}
    for prop in cfg.props:
        k = str(getattr(prop, "kind", "") or "").strip().lower() or "unknown"
        props_by_kind[k] = props_by_kind.get(k, 0) + 1
    return {
        "subnet": cfg.subnet,
        "channels_per_universe": cfg.channels_per_universe,
        "props_by_kind": props_by_kind,
    }


async def _maybe_upsert_show_config(
    state: AppState, *, cfg: ShowConfig, path_s: str
) -> None:
    db = getattr(state, "db", None)
    if db is None:
        return
    try:
        await db.upsert_show_config(
            file=_rel_show_config_path(state, path_s),
            name=str(cfg.name or ""),
            props_total=len(cfg.props),
            groups_total=len(cfg.groups or {}),
            coordinator_base_url=str(cfg.coordinator.base_url or "").strip() or None,
            fpp_base_url=str(cfg.fpp.base_url or "").strip() or None,
            payload=_show_config_payload(cfg),
        )
    except Exception:
        pass


async def show_config_load(
    req: ShowConfigLoadRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        cfg = await load_show_config_async(
            data_dir=state.settings.data_dir, rel_path=req.file
        )
        await _maybe_upsert_show_config(
            state, cfg=cfg, path_s=str(_resolve_data_path(state, req.file))
        )
        await log_event(
            state,
            action="show.config.load",
            ok=True,
            resource=str(req.file),
            request=request,
        )
        return {"ok": True, "config": cfg.as_dict()}
    except Exception as e:
        await log_event(
            state,
            action="show.config.load",
            ok=False,
            resource=str(req.file),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def xlights_import_networks(
    req: XlightsImportNetworksRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        networks_path = _resolve_data_path(state, req.networks_file)
        controllers = await run_cpu_blocking_state(
            state, import_xlights_networks_file, str(networks_path)
        )
        cfg = show_config_from_xlights_networks(
            networks=controllers,
            show_name=req.show_name,
            subnet=req.subnet,
            coordinator_base_url=req.coordinator_base_url,
            fpp_base_url=req.fpp_base_url,
        )
        out_path = await write_show_config_async(
            data_dir=state.settings.data_dir,
            rel_path=req.out_file,
            config=cfg,
        )
        await _maybe_upsert_show_config(state, cfg=cfg, path_s=str(out_path))
        await log_event(
            state,
            action="xlights.import.networks",
            ok=True,
            resource=str(req.networks_file),
            payload={"out_file": req.out_file},
            request=request,
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
    except HTTPException as e:
        await log_event(
            state,
            action="xlights.import.networks",
            ok=False,
            resource=str(req.networks_file),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="xlights.import.networks",
            ok=False,
            resource=str(req.networks_file),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def xlights_import_project(
    req: XlightsImportProjectRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        proj = _resolve_data_path(state, req.project_dir)
        if not await aio_os.path.isdir(str(proj)):
            raise HTTPException(
                status_code=400, detail="project_dir must be a directory under DATA_DIR"
            )

        proj_root = proj.resolve()

        async def _pick(name: Optional[str], defaults: List[str]) -> Path:
            candidates = [name] if name else []
            candidates.extend([d for d in defaults if d not in candidates])
            for n in candidates:
                if not n:
                    continue
                p = (proj_root / n).resolve()
                if proj_root not in p.parents:
                    continue
                if await aio_os.path.isfile(str(p)):
                    return p
            raise HTTPException(
                status_code=400,
                detail=f"Missing required xLights file (tried: {', '.join(candidates)})",
            )

        networks_path = await _pick(req.networks_file, ["xlights_networks.xml"])
        models_path = await _pick(
            req.models_file,
            ["xlights_rgbeffects.xml", "xlights_models.xml", "xlights_layout.xml"],
        )

        controllers = await run_cpu_blocking_state(
            state, import_xlights_networks_file, str(networks_path)
        )
        models = await run_cpu_blocking_state(
            state, import_xlights_models_file, str(models_path)
        )

        cfg = show_config_from_xlights_project(
            networks=controllers,
            models=models,
            show_name=req.show_name,
            subnet=req.subnet,
            coordinator_base_url=req.coordinator_base_url,
            fpp_base_url=req.fpp_base_url,
            include_controllers=bool(req.include_controllers),
            include_models=bool(req.include_models),
        )

        out_path = await write_show_config_async(
            data_dir=state.settings.data_dir,
            rel_path=req.out_file,
            config=cfg,
        )
        await _maybe_upsert_show_config(state, cfg=cfg, path_s=str(out_path))
        await log_event(
            state,
            action="xlights.import.project",
            ok=True,
            resource=str(req.project_dir),
            payload={"out_file": req.out_file},
            request=request,
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
    except HTTPException as e:
        await log_event(
            state,
            action="xlights.import.project",
            ok=False,
            resource=str(req.project_dir),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="xlights.import.project",
            ok=False,
            resource=str(req.project_dir),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def xlights_import_sequence(
    req: XlightsImportSequenceRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Import a beat/timing grid from an xLights `.xsq` file.
    """
    try:
        xsq_path = _resolve_data_path(state, req.xsq_file)
        out_path = _resolve_data_path(state, req.out_file)

        analysis = await run_cpu_blocking_state(
            state,
            import_xlights_xsq_timing_file,
            xsq_path=str(xsq_path),
            timing_track=req.timing_track,
        )
        await write_json_async(str(out_path), analysis)

        base = Path(state.settings.data_dir).resolve()
        rel_out = (
            str(out_path.resolve().relative_to(base))
            if base in out_path.resolve().parents
            else str(out_path)
        )
        await log_event(
            state,
            action="xlights.import.sequence",
            ok=True,
            resource=str(req.xsq_file),
            payload={"out_file": rel_out, "timing_track": req.timing_track},
            request=request,
        )
        return {"ok": True, "analysis": analysis, "out_file": rel_out}
    except HTTPException as e:
        await log_event(
            state,
            action="xlights.import.sequence",
            ok=False,
            resource=str(req.xsq_file),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except XlightsSequenceImportError as e:
        await log_event(
            state,
            action="xlights.import.sequence",
            ok=False,
            resource=str(req.xsq_file),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        await log_event(
            state,
            action="xlights.import.sequence",
            ok=False,
            resource=str(req.xsq_file),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=str(e))

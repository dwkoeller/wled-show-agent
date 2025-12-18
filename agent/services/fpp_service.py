from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Dict

from fastapi import Depends, HTTPException

from fpp_client import AsyncFPPClient, FPPError
from fpp_export import render_http_post_script, write_script
from models.requests import (
    FPPExportFleetSequenceScriptRequest,
    FPPExportFleetStopAllScriptRequest,
    FPPProxyRequest,
    FPPStartPlaylistRequest,
    FPPTriggerEventRequest,
    FPPUploadFileRequest,
)
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from show_config import load_show_config


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def _client(state: AppState) -> AsyncFPPClient:
    base_url = str(state.settings.fpp_base_url or "").strip()
    if not base_url:
        raise HTTPException(
            status_code=400, detail="FPP integration not configured; set FPP_BASE_URL."
        )
    http = state.peer_http
    if http is None:
        raise HTTPException(status_code=503, detail="HTTP client not initialized")
    return AsyncFPPClient(
        base_url=base_url,
        client=http,
        timeout_s=float(state.settings.fpp_http_timeout_s),
        headers={k: v for (k, v) in state.settings.fpp_headers},
    )


async def fpp_status(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        return {"ok": True, "fpp": (await _client(state).status()).as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_discover(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        return {"ok": True, "discover": await _client(state).discover()}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_playlists(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        return {"ok": True, "fpp": (await _client(state).playlists()).as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_start_playlist(
    req: FPPStartPlaylistRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).start_playlist(req.name, repeat=req.repeat)
        return {"ok": True, "fpp": resp.as_dict()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def fpp_stop_playlist(
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        return {"ok": True, "fpp": (await _client(state).stop_playlist()).as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_trigger_event(
    req: FPPTriggerEventRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        return {
            "ok": True,
            "fpp": (await _client(state).trigger_event(req.event_id)).as_dict(),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def fpp_proxy(
    req: FPPProxyRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    method = (req.method or "GET").strip().upper()
    if method not in ("GET", "POST", "PUT", "DELETE", "PATCH"):
        raise HTTPException(
            status_code=400, detail="Unsupported method; use GET/POST/PUT/DELETE/PATCH."
        )
    try:
        resp = await _client(state).request(
            method, req.path, params=dict(req.params or {}), json_body=req.json_body
        )
        return {"ok": True, "fpp": resp.as_dict()}
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_upload_file(
    req: FPPUploadFileRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        local_path = _resolve_data_path(state, req.local_file)
        if not local_path.is_file():
            raise HTTPException(status_code=400, detail="local_file does not exist")
        dest = (req.dest_filename or "").strip() or local_path.name
        content = await asyncio.to_thread(local_path.read_bytes)
        resp = await _client(state).upload_file(
            dir=req.dir, subdir=req.subdir, filename=dest, content=content
        )
        return {
            "ok": True,
            "local": str(local_path),
            "dest": {"dir": req.dir, "subdir": req.subdir, "filename": dest},
            "fpp": resp.as_dict(),
        }
    except HTTPException:
        raise
    except FPPError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


async def export_fleet_sequence_start_script(
    req: FPPExportFleetSequenceScriptRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    coord = (req.coordinator_base_url or "").strip()
    if (not coord) and req.show_config_file:
        cfg = await asyncio.to_thread(
            load_show_config,
            data_dir=state.settings.data_dir,
            rel_path=req.show_config_file,
        )
        coord = (cfg.coordinator.base_url or "").strip()
    if not coord:
        raise HTTPException(
            status_code=400,
            detail="Provide coordinator_base_url or show_config_file with coordinator.base_url.",
        )

    payload: Dict[str, Any] = {
        "file": req.sequence_file,
        "loop": bool(req.loop),
        "targets": req.targets,
        "include_self": bool(req.include_self),
    }
    script = render_http_post_script(
        coordinator_base_url=coord,
        path="/v1/fleet/sequences/start",
        payload=payload,
        a2a_api_key=state.settings.a2a_api_key if req.include_a2a_key else None,
    )

    out_dir = str(_resolve_data_path(state, "fpp/scripts"))
    res = await asyncio.to_thread(
        write_script,
        out_dir=out_dir,
        filename=req.out_filename,
        script_text=script,
    )
    return {
        "ok": True,
        "script": {
            "file": res.filename,
            "path": res.rel_path,
            "bytes": res.bytes_written,
        },
    }


async def export_fleet_stop_all_script(
    req: FPPExportFleetStopAllScriptRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    coord = (req.coordinator_base_url or "").strip()
    if (not coord) and req.show_config_file:
        cfg = await asyncio.to_thread(
            load_show_config,
            data_dir=state.settings.data_dir,
            rel_path=req.show_config_file,
        )
        coord = (cfg.coordinator.base_url or "").strip()
    if not coord:
        raise HTTPException(
            status_code=400,
            detail="Provide coordinator_base_url or show_config_file with coordinator.base_url.",
        )

    payload: Dict[str, Any] = {
        "targets": req.targets,
        "include_self": bool(req.include_self),
    }
    script = render_http_post_script(
        coordinator_base_url=coord,
        path="/v1/fleet/stop_all",
        payload=payload,
        a2a_api_key=state.settings.a2a_api_key if req.include_a2a_key else None,
    )

    out_dir = str(_resolve_data_path(state, "fpp/scripts"))
    res = await asyncio.to_thread(
        write_script,
        out_dir=out_dir,
        filename=req.out_filename,
        script_text=script,
    )
    return {
        "ok": True,
        "script": {
            "file": res.filename,
            "path": res.rel_path,
            "bytes": res.bytes_written,
        },
    }

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import aiofiles
from aiofiles import os as aio_os
from fastapi import Depends, HTTPException, Request

from fpp_client import AsyncFPPClient, FPPError
from utils.outbound_http import retry_policy_from_settings
from fpp_export import render_http_post_script, write_script_async
from models.requests import (
    FPPExportFleetSequenceScriptRequest,
    FPPExportFleetStopAllScriptRequest,
    FPPExportEventScriptRequest,
    FPPPlaylistImportRequest,
    FPPPlaylistSyncRequest,
    FPPProxyRequest,
    FPPStartPlaylistRequest,
    FPPTriggerEventRequest,
    FPPUploadFileRequest,
)
from services.audit_logger import log_event
from services.auth_service import require_a2a_auth
from services.state import AppState, get_state
from show_config import load_show_config_async


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def _playlist_filename(name: str) -> str:
    base = Path(str(name or "").strip()).name
    if not base:
        raise ValueError("playlist name is required")
    return base if base.endswith(".json") else f"{base}.json"


def _playlist_local_path(state: AppState, name: str) -> Path:
    filename = _playlist_filename(name)
    return _resolve_data_path(state, f"fpp/playlists/{filename}")


def _normalize_playlist_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = payload.get("playlist") or payload.get("mainPlaylist") or payload.get("items")
    if isinstance(items, list):
        return [x for x in items if isinstance(x, dict)]
    return []


def _extract_playlist_names(body: Any) -> List[str]:
    names: List[str] = []
    if isinstance(body, list):
        for item in body:
            if isinstance(item, str) and item.strip():
                names.append(item.strip())
            elif isinstance(item, dict) and item.get("name"):
                names.append(str(item.get("name")).strip())
    elif isinstance(body, dict):
        candidates = body.get("playlists") or body.get("playlist") or body.get("entries")
        if isinstance(candidates, list):
            for item in candidates:
                if isinstance(item, str) and item.strip():
                    names.append(item.strip())
                elif isinstance(item, dict) and item.get("name"):
                    names.append(str(item.get("name")).strip())
    # de-dup and sort
    uniq = sorted({n for n in names if n})
    return uniq


def _build_playlist_payload(
    *, name: str, items: List[Dict[str, Any]], repeat: bool, description: str | None
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": str(name),
        "version": 1,
        "repeat": 1 if repeat else 0,
        "playlist": items,
        "mainPlaylist": items,
    }
    if description:
        payload["description"] = str(description)
    return payload


async def _fetch_playlist_from_fpp(state: AppState, name: str) -> Dict[str, Any]:
    client = _client(state)
    errors: List[str] = []
    paths = [f"/api/playlist/{name}", f"/api/playlists/{name}"]
    for path in paths:
        try:
            resp = await client.request("GET", path)
            body = resp.body
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except Exception:
                    body = None
            if isinstance(body, dict):
                return body
        except Exception as e:
            errors.append(str(e))
            continue

    # Try fetching the playlist file directly.
    try:
        resp = await client.request("GET", f"/api/file/playlists/{_playlist_filename(name)}")
        body = resp.body
        if isinstance(body, str):
            body = json.loads(body)
        if isinstance(body, dict):
            return body
    except Exception as e:
        errors.append(str(e))

    detail = errors[-1] if errors else "Failed to fetch playlist from FPP"
    raise HTTPException(status_code=502, detail=detail)


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
        retry=retry_policy_from_settings(state.settings),
    )


async def fpp_status(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        res = {"ok": True, "fpp": (await _client(state).status()).as_dict()}
        await log_event(state, action="fpp.status", ok=True, request=request)
        return res
    except FPPError as e:
        await log_event(
            state, action="fpp.status", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_discover(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        res = {"ok": True, "discover": await _client(state).discover()}
        await log_event(state, action="fpp.discover", ok=True, request=request)
        return res
    except Exception as e:
        await log_event(
            state, action="fpp.discover", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_playlists(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).playlists()
        body = resp.body
        playlists = _extract_playlist_names(body)
        res = {"ok": True, "playlists": playlists, "fpp": resp.as_dict()}
        await log_event(
            state,
            action="fpp.playlists",
            ok=True,
            payload={"count": len(playlists)},
            request=request,
        )
        return res
    except FPPError as e:
        await log_event(
            state, action="fpp.playlists", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_start_playlist(
    req: FPPStartPlaylistRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        resp = await _client(state).start_playlist(req.name, repeat=req.repeat)
        await log_event(
            state,
            action="fpp.playlist.start",
            ok=True,
            resource=str(req.name),
            payload={"repeat": bool(req.repeat)},
            request=request,
        )
        return {"ok": True, "fpp": resp.as_dict()}
    except Exception as e:
        await log_event(
            state,
            action="fpp.playlist.start",
            ok=False,
            resource=str(req.name),
            error=str(e),
            payload={"repeat": bool(req.repeat)},
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def fpp_stop_playlist(
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        res = {"ok": True, "fpp": (await _client(state).stop_playlist()).as_dict()}
        await log_event(state, action="fpp.playlist.stop", ok=True, request=request)
        return res
    except FPPError as e:
        await log_event(
            state, action="fpp.playlist.stop", ok=False, error=str(e), request=request
        )
        raise HTTPException(status_code=502, detail=str(e))


async def fpp_trigger_event(
    req: FPPTriggerEventRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        res = {
            "ok": True,
            "fpp": (await _client(state).trigger_event(req.event_id)).as_dict(),
        }
        await log_event(
            state,
            action="fpp.event.trigger",
            ok=True,
            resource=str(req.event_id),
            request=request,
        )
        return res
    except Exception as e:
        await log_event(
            state,
            action="fpp.event.trigger",
            ok=False,
            resource=str(req.event_id),
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def fpp_proxy(
    req: FPPProxyRequest,
    request: Request,
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
        await log_event(
            state,
            action="fpp.proxy",
            ok=True,
            resource=str(req.path or ""),
            payload={"method": method},
            request=request,
        )
        return {"ok": True, "fpp": resp.as_dict()}
    except FPPError as e:
        await log_event(
            state,
            action="fpp.proxy",
            ok=False,
            resource=str(req.path or ""),
            error=str(e),
            payload={"method": method},
            request=request,
        )
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        await log_event(
            state,
            action="fpp.proxy",
            ok=False,
            resource=str(req.path or ""),
            error=str(e),
            payload={"method": method},
            request=request,
        )
        raise


async def fpp_upload_file(
    req: FPPUploadFileRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        local_path = _resolve_data_path(state, req.local_file)
        if not await aio_os.path.isfile(str(local_path)):
            raise HTTPException(status_code=400, detail="local_file does not exist")
        dest = (req.dest_filename or "").strip() or local_path.name
        async with aiofiles.open(local_path, "rb") as f:
            content = await f.read()
        resp = await _client(state).upload_file(
            dir=req.dir, subdir=req.subdir, filename=dest, content=content
        )
        await log_event(
            state,
            action="fpp.upload",
            ok=True,
            resource=str(req.local_file),
            payload={"dir": req.dir, "subdir": req.subdir, "dest": dest},
            request=request,
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
        await log_event(
            state,
            action="fpp.upload",
            ok=False,
            resource=str(req.local_file),
            error=str(e),
            payload={"dir": req.dir, "subdir": req.subdir},
            request=request,
        )
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        await log_event(
            state,
            action="fpp.upload",
            ok=False,
            resource=str(req.local_file),
            error=str(e),
            payload={"dir": req.dir, "subdir": req.subdir},
            request=request,
        )
        raise HTTPException(status_code=400, detail=str(e))


async def fpp_playlists_sync(
    req: FPPPlaylistSyncRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        entries: List[Dict[str, Any]] = []
        for ent in req.entries or []:
            kind = str(ent.kind or "sequence").strip().lower()
            if kind == "sequence":
                if not ent.file:
                    raise HTTPException(
                        status_code=400, detail="Sequence entry requires file"
                    )
                item: Dict[str, Any] = {
                    "type": "sequence",
                    "sequenceName": str(ent.file),
                }
                if ent.duration_s is not None:
                    item["duration"] = float(ent.duration_s)
                if ent.repeat is not None:
                    item["repeat"] = int(ent.repeat)
                entries.append(item)
            elif kind == "pause":
                if ent.duration_s is None:
                    raise HTTPException(
                        status_code=400, detail="Pause entry requires duration_s"
                    )
                entries.append({"type": "pause", "duration": float(ent.duration_s)})
            elif kind == "event":
                if ent.event_id is None:
                    raise HTTPException(
                        status_code=400, detail="Event entry requires event_id"
                    )
                entries.append(
                    {
                        "type": "event",
                        "eventID": int(ent.event_id),
                        "eventId": int(ent.event_id),
                    }
                )
            else:
                raise HTTPException(
                    status_code=400, detail=f"Unsupported entry kind: {kind}"
                )

        for seq in req.sequence_files or []:
            if str(seq).strip():
                entries.append({"type": "sequence", "sequenceName": str(seq).strip()})

        if not entries:
            raise HTTPException(status_code=400, detail="No entries provided")

        payload = _build_playlist_payload(
            name=req.name,
            items=entries,
            repeat=bool(req.repeat),
            description=req.description,
        )
        data = json.dumps(payload, indent=2, ensure_ascii=True).encode("utf-8")

        local_path = None
        if req.write_local:
            try:
                path = _playlist_local_path(state, req.name)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))
            if path.exists() and not req.overwrite:
                raise HTTPException(
                    status_code=400, detail="Local playlist file already exists"
                )
            path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(path, "wb") as f:
                await f.write(data)
            local_path = str(path)

        fpp_resp = None
        if req.upload:
            try:
                filename = _playlist_filename(req.name)
                fpp_resp = (
                    await _client(state).upload_file(
                        dir="playlists", filename=filename, content=data
                    )
                ).as_dict()
            except FPPError as e:
                raise HTTPException(status_code=502, detail=str(e))

        res = {
            "ok": True,
            "playlist": payload,
            "local_file": local_path,
            "fpp": fpp_resp,
        }
        await log_event(
            state,
            action="fpp.playlists.sync",
            ok=True,
            resource=str(req.name),
            payload={
                "entries": len(entries),
                "write_local": bool(req.write_local),
                "upload": bool(req.upload),
            },
            request=request,
        )
        return res
    except HTTPException as e:
        await log_event(
            state,
            action="fpp.playlists.sync",
            ok=False,
            resource=str(req.name),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="fpp.playlists.sync",
            ok=False,
            resource=str(req.name),
            error=str(e),
            request=request,
        )
        raise


async def fpp_playlists_import(
    req: FPPPlaylistImportRequest,
    request: Request,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        payload: Dict[str, Any]
        if req.from_fpp:
            payload = await _fetch_playlist_from_fpp(state, req.name)
        else:
            if not req.file:
                raise HTTPException(
                    status_code=400, detail="file is required when from_fpp=false"
                )
            path = _resolve_data_path(state, req.file)
            if not await aio_os.path.isfile(str(path)):
                raise HTTPException(status_code=400, detail="file does not exist")
            async with aiofiles.open(path, "r") as f:
                raw = await f.read()
            payload = json.loads(raw)

        if req.write_local and req.from_fpp:
            try:
                out_path = _playlist_local_path(state, req.name)
                await aio_os.makedirs(str(out_path.parent), exist_ok=True)
                async with aiofiles.open(out_path, "w") as f:
                    await f.write(json.dumps(payload, indent=2, ensure_ascii=True))
            except Exception:
                pass

        items = _normalize_playlist_items(payload)
        parsed: List[Dict[str, Any]] = []
        for item in items:
            kind = str(item.get("type") or item.get("kind") or "").strip().lower()
            if kind == "sequence":
                parsed.append(
                    {
                        "kind": "sequence",
                        "file": item.get("sequenceName")
                        or item.get("file")
                        or item.get("name"),
                        "duration_s": item.get("duration"),
                    }
                )
            elif kind == "pause":
                parsed.append({"kind": "pause", "duration_s": item.get("duration")})
            elif kind == "event":
                parsed.append(
                    {
                        "kind": "event",
                        "event_id": item.get("eventID") or item.get("eventId"),
                    }
                )

        await log_event(
            state,
            action="fpp.playlists.import",
            ok=True,
            resource=str(req.name),
            payload={"from_fpp": bool(req.from_fpp)},
            request=request,
        )
        return {"ok": True, "playlist": payload, "entries": parsed}
    except HTTPException as e:
        await log_event(
            state,
            action="fpp.playlists.import",
            ok=False,
            resource=str(req.name),
            error=str(getattr(e, "detail", e)),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="fpp.playlists.import",
            ok=False,
            resource=str(req.name),
            error=str(e),
            request=request,
        )
        raise


async def export_fleet_sequence_start_script(
    req: FPPExportFleetSequenceScriptRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    coord = (req.coordinator_base_url or "").strip()
    if (not coord) and req.show_config_file:
        cfg = await load_show_config_async(
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
    res = await write_script_async(
        out_dir=out_dir,
        filename=req.out_filename,
        script_text=script,
    )
    if state.db is not None:
        try:
            base = Path(state.settings.data_dir).resolve()
            scripts_root = (base / "fpp" / "scripts").resolve()
            try:
                rel_file = str(Path(res.rel_path).resolve().relative_to(scripts_root))
            except Exception:
                rel_file = str(Path(res.rel_path).resolve().relative_to(base))
            await state.db.upsert_fpp_script(
                file=rel_file,
                kind="fleet_sequence_start",
                bytes_written=int(res.bytes_written),
                payload={
                    "coordinator_base_url": coord,
                    "path": "/v1/fleet/sequences/start",
                    "targets": req.targets,
                    "include_self": bool(req.include_self),
                },
            )
        except Exception:
            pass
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
        cfg = await load_show_config_async(
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
    res = await write_script_async(
        out_dir=out_dir,
        filename=req.out_filename,
        script_text=script,
    )
    if state.db is not None:
        try:
            base = Path(state.settings.data_dir).resolve()
            scripts_root = (base / "fpp" / "scripts").resolve()
            try:
                rel_file = str(Path(res.rel_path).resolve().relative_to(scripts_root))
            except Exception:
                rel_file = str(Path(res.rel_path).resolve().relative_to(base))
            await state.db.upsert_fpp_script(
                file=rel_file,
                kind="fleet_stop_all",
                bytes_written=int(res.bytes_written),
                payload={
                    "coordinator_base_url": coord,
                    "path": "/v1/fleet/stop_all",
                    "targets": req.targets,
                    "include_self": bool(req.include_self),
                },
            )
        except Exception:
            pass
    return {
        "ok": True,
        "script": {
            "file": res.filename,
            "path": res.rel_path,
            "bytes": res.bytes_written,
        },
    }


async def export_event_script(
    req: FPPExportEventScriptRequest,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    coord = (req.coordinator_base_url or "").strip()
    if (not coord) and req.show_config_file:
        cfg = await load_show_config_async(
            data_dir=state.settings.data_dir,
            rel_path=req.show_config_file,
        )
        coord = (cfg.coordinator.base_url or "").strip()
    if not coord:
        raise HTTPException(
            status_code=400,
            detail="Provide coordinator_base_url or show_config_file with coordinator.base_url.",
        )

    path = str(req.path or "").strip()
    if not path:
        raise HTTPException(status_code=400, detail="path is required")
    if not path.startswith("/"):
        path = "/" + path

    payload: Dict[str, Any] = dict(req.payload or {})
    script = render_http_post_script(
        coordinator_base_url=coord,
        path=path,
        payload=payload,
        a2a_api_key=state.settings.a2a_api_key if req.include_a2a_key else None,
    )

    out_dir = str(_resolve_data_path(state, "fpp/scripts"))
    default_name = f"event-{int(req.event_id)}.sh"
    filename = (req.out_filename or "").strip() or default_name
    res = await write_script_async(
        out_dir=out_dir,
        filename=filename,
        script_text=script,
    )
    if state.db is not None:
        try:
            base = Path(state.settings.data_dir).resolve()
            scripts_root = (base / "fpp" / "scripts").resolve()
            try:
                rel_file = str(Path(res.rel_path).resolve().relative_to(scripts_root))
            except Exception:
                rel_file = str(Path(res.rel_path).resolve().relative_to(base))
            await state.db.upsert_fpp_script(
                file=rel_file,
                kind="event_script",
                bytes_written=int(res.bytes_written),
                payload={
                    "event_id": int(req.event_id),
                    "coordinator_base_url": coord,
                    "path": path,
                    "payload": payload,
                },
            )
        except Exception:
            pass
    return {
        "ok": True,
        "script": {
            "file": res.filename,
            "path": res.rel_path,
            "bytes": res.bytes_written,
        },
    }

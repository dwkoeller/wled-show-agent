from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict

from fastapi import Depends, HTTPException, Request
from fastapi.responses import FileResponse

from services.auth_service import require_a2a_auth
from services.state import AppState, get_state


def _resolve_data_path(state: AppState, rel_path: str) -> Path:
    base = Path(state.settings.data_dir).resolve()
    p = (base / (rel_path or "")).resolve()
    if p == base:
        return p
    if base not in p.parents:
        raise HTTPException(status_code=400, detail="Path must be within DATA_DIR.")
    return p


def files_list(
    dir: str = "",
    glob: str = "*",
    recursive: bool = False,
    limit: int = 500,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    try:
        base = _resolve_data_path(state, dir)
        if not base.exists():
            return {"ok": True, "files": []}
        if not base.is_dir():
            raise HTTPException(
                status_code=400, detail="dir must be a directory under DATA_DIR"
            )

        pattern = (glob or "*").strip() or "*"
        it = base.rglob(pattern) if bool(recursive) else base.glob(pattern)

        out: list[str] = []
        root = Path(state.settings.data_dir).resolve()
        lim = max(1, min(5000, int(limit)))
        for p in it:
            if len(out) >= lim:
                break
            try:
                rp = p.resolve()
            except Exception:
                continue
            if root not in rp.parents and rp != root:
                continue
            if rp.is_file():
                try:
                    out.append(str(rp.relative_to(root)))
                except Exception:
                    out.append(str(rp))

        out.sort()
        return {"ok": True, "files": out}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def files_download(
    path: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> FileResponse:
    p = _resolve_data_path(state, path)
    if not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(p), filename=p.name)


async def files_upload(
    path: str,
    request: Request,
    overwrite: bool = False,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    """
    Upload a file to DATA_DIR.

    This endpoint accepts raw bytes (e.g. Content-Type: application/octet-stream) to avoid
    requiring multipart parsing dependencies.
    """
    root = Path(state.settings.data_dir).resolve()
    dest = _resolve_data_path(state, path)
    if dest == root or dest.is_dir():
        raise HTTPException(
            status_code=400, detail="path must be a file under DATA_DIR"
        )

    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create parent dir: {e}")

    if dest.exists() and not overwrite:
        raise HTTPException(
            status_code=409, detail="File already exists (set overwrite=true)"
        )

    tmp = dest.with_name(f".{dest.name}.uploading-{uuid.uuid4().hex}")
    total = 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total += len(chunk)
                f.write(chunk)
        os.replace(tmp, dest)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass

    try:
        rel = str(dest.resolve().relative_to(root))
    except Exception:
        rel = str(dest)
    return {"ok": True, "path": rel, "bytes": total}


def files_delete(
    path: str,
    _: None = Depends(require_a2a_auth),
    state: AppState = Depends(get_state),
) -> Dict[str, Any]:
    p = _resolve_data_path(state, path)
    if not p.exists():
        return {"ok": True, "deleted": False}
    if not p.is_file():
        raise HTTPException(
            status_code=400, detail="path must be a file under DATA_DIR"
        )
    try:
        p.unlink()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {e}")
    return {"ok": True, "deleted": True}

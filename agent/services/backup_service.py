from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
import zipfile
import fnmatch
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from fastapi import Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from sqlalchemy import delete
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from services.auth_service import require_admin
from services.audit_logger import log_event
from services.blocking_service import BlockingQueueFull
from services.state import AppState, get_state
from sql_store import (
    AgentHeartbeatHistoryRecord,
    AgentHeartbeatHistoryTagRecord,
    AgentHeartbeatRecord,
    AgentOverrideRecord,
    AuditLogRecord,
    AuthApiKeyRecord,
    AuthLoginAttemptRecord,
    AuthPasswordResetRecord,
    AuthSessionRecord,
    AuthUserRecord,
    AudioAnalysisRecord,
    FppScriptRecord,
    FseqExportRecord,
    GlobalKVRecord,
    JobRecord,
    KVRecord,
    LastAppliedRecord,
    LeaseRecord,
    EventLogRecord,
    OrchestrationPeerResultRecord,
    OrchestrationPresetRecord,
    OrchestrationRunRecord,
    OrchestrationStepRecord,
    PackIngestRecord,
    ReconcileRunRecord,
    SchedulerEventRecord,
    SchemaVersion,
    SequenceMetaRecord,
    ShowConfigRecord,
)


@dataclass(frozen=True)
class TableSpec:
    name: str
    model: type[SQLModel]
    category: str


BACKUP_FORMAT_VERSION = 1
SUPPORTED_FORMAT_VERSIONS = (1,)


@dataclass(frozen=True)
class DataEntry:
    info: zipfile.ZipInfo
    rel: str


AUTH_TABLES = {
    "auth_users",
    "auth_sessions",
    "auth_login_attempts",
    "auth_api_keys",
    "auth_password_resets",
}

TABLES: Tuple[TableSpec, ...] = (
    TableSpec("schema_version", SchemaVersion, "core"),
    TableSpec("jobs", JobRecord, "core"),
    TableSpec("kv", KVRecord, "core"),
    TableSpec("kv_global", GlobalKVRecord, "core"),
    TableSpec("pack_ingests", PackIngestRecord, "meta"),
    TableSpec("sequence_meta", SequenceMetaRecord, "meta"),
    TableSpec("show_configs", ShowConfigRecord, "meta"),
    TableSpec("fseq_exports", FseqExportRecord, "meta"),
    TableSpec("fpp_scripts", FppScriptRecord, "meta"),
    TableSpec("audio_analyses", AudioAnalysisRecord, "meta"),
    TableSpec("last_applied", LastAppliedRecord, "meta"),
    TableSpec("agent_heartbeats", AgentHeartbeatRecord, "fleet"),
    TableSpec("agent_overrides", AgentOverrideRecord, "fleet"),
    TableSpec("agent_heartbeat_history", AgentHeartbeatHistoryRecord, "fleet"),
    TableSpec(
        "agent_heartbeat_history_tags", AgentHeartbeatHistoryTagRecord, "fleet"
    ),
    TableSpec("leases", LeaseRecord, "scheduler"),
    TableSpec("scheduler_events", SchedulerEventRecord, "scheduler"),
    TableSpec("audit_logs", AuditLogRecord, "audit"),
    TableSpec("event_logs", EventLogRecord, "audit"),
    TableSpec("auth_users", AuthUserRecord, "auth"),
    TableSpec("auth_sessions", AuthSessionRecord, "auth"),
    TableSpec("auth_login_attempts", AuthLoginAttemptRecord, "auth"),
    TableSpec("auth_api_keys", AuthApiKeyRecord, "auth"),
    TableSpec("auth_password_resets", AuthPasswordResetRecord, "auth"),
    TableSpec("orchestration_presets", OrchestrationPresetRecord, "orchestration"),
    TableSpec("reconcile_runs", ReconcileRunRecord, "meta"),
    TableSpec("orchestration_runs", OrchestrationRunRecord, "orchestration"),
    TableSpec("orchestration_steps", OrchestrationStepRecord, "orchestration"),
    TableSpec("orchestration_peers", OrchestrationPeerResultRecord, "orchestration"),
)


def _mb_to_bytes(mb: float) -> int:
    return max(0, int(float(mb) * 1024 * 1024))


def _normalize_rel(rel: str) -> str:
    return str(rel or "").replace("\\", "/").lstrip("/")


def _matches_exclude(rel: str, patterns: Iterable[str]) -> bool:
    norm = _normalize_rel(rel)
    for pat in patterns:
        p = str(pat or "").strip()
        if not p:
            continue
        if fnmatch.fnmatch(norm, p):
            return True
    return False


async def _run_blocking(state: AppState, func, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
    blocking = getattr(state, "blocking", None)
    if blocking is not None and hasattr(blocking, "run"):
        try:
            return await blocking.run(func, *args, **kwargs)
        except BlockingQueueFull as e:
            raise HTTPException(
                status_code=503, detail="Backup worker queue is full"
            ) from e
    return await asyncio.to_thread(func, *args, **kwargs)


async def _get_schema_version(state: AppState) -> int | None:
    try:
        async with AsyncSession(state.db.engine) as session:
            res = await session.exec(select(SchemaVersion))
            row = res.first()
            if row is None:
                return None
            return int(row.version)
    except Exception:
        return None


def _load_manifest(
    zf: zipfile.ZipFile, *, require_manifest: bool
) -> tuple[Dict[str, Any] | None, list[str]]:
    warnings: list[str] = []
    try:
        raw = zf.read("manifest.json")
    except KeyError:
        if require_manifest:
            raise HTTPException(status_code=400, detail="manifest.json is required")
        warnings.append("manifest.json missing")
        return None, warnings
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid manifest: {e}") from e

    try:
        manifest = json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid manifest JSON: {e}") from e
    if not isinstance(manifest, dict):
        raise HTTPException(status_code=400, detail="manifest.json must be an object")

    fv = manifest.get("format_version")
    if fv is not None:
        try:
            fv_int = int(fv)
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid manifest format_version: {e}"
            ) from e
        if fv_int not in SUPPORTED_FORMAT_VERSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported backup format_version {fv_int}",
            )
    created_at = manifest.get("created_at")
    if created_at is not None and not isinstance(created_at, (int, float)):
        raise HTTPException(status_code=400, detail="manifest.created_at must be a number")

    return manifest, warnings


async def _read_upload_to_temp(
    upload: UploadFile, *, max_bytes: int, spool_max_bytes: int
) -> tempfile.SpooledTemporaryFile:
    tmp = tempfile.SpooledTemporaryFile(max_size=max(1, int(spool_max_bytes)))
    total = 0
    limit = int(max_bytes)
    if limit <= 0:
        limit = -1
    try:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if limit > 0 and total > limit:
                raise HTTPException(
                    status_code=413,
                    detail="Backup zip exceeds size limit",
                )
            await asyncio.to_thread(tmp.write, chunk)
    except Exception:
        try:
            tmp.close()
        except Exception:
            pass
        raise
    await asyncio.to_thread(tmp.seek, 0)
    return tmp


def _copy_with_limit(
    src,
    dst,
    *,
    max_total_bytes: int,
    max_file_bytes: int,
    total: int,
) -> int:
    total_written = int(total)
    file_written = 0
    while True:
        chunk = src.read(1024 * 1024)
        if not chunk:
            break
        total_written += len(chunk)
        file_written += len(chunk)
        if max_total_bytes and total_written > max_total_bytes:
            raise HTTPException(
                status_code=413,
                detail="Backup exceeds unpacked size limit",
            )
        if max_file_bytes and file_written > max_file_bytes:
            raise HTTPException(
                status_code=413,
                detail="Backup file exceeds per-file size limit",
            )
        dst.write(chunk)
    return total_written


def _rollback_data(applied: Iterable[tuple[Path, Path | None]]) -> None:
    for target, backup_path in reversed(list(applied)):
        try:
            if backup_path and backup_path.exists():
                os.replace(backup_path, target)
            else:
                if target.exists():
                    target.unlink()
        except Exception:
            pass


def _iter_data_files(data_dir: str) -> Iterable[Tuple[str, str]]:
    base = Path(data_dir).resolve()
    for root, _, files in os.walk(base):
        for fname in files:
            path = Path(root) / fname
            try:
                rel = path.relative_to(base).as_posix()
            except Exception:
                continue
            yield str(path), rel


def _safe_data_path(base: Path, rel_path: str) -> Path:
    base_resolved = base.resolve()
    target = (base_resolved / rel_path).resolve()
    try:
        target.relative_to(base_resolved)
    except ValueError as exc:
        raise ValueError(f"Invalid path: {rel_path}") from exc
    return target


def _build_backup_zip(
    *,
    data_dir: str,
    include_db: bool,
    include_data: bool,
    db_data: Dict[str, List[Dict[str, Any]]],
    manifest: Dict[str, Any],
    exclude_patterns: Iterable[str],
    max_data_bytes: int,
    max_file_bytes: int,
    max_files: int,
    spool_max_bytes: int,
) -> tuple[tempfile.SpooledTemporaryFile, Dict[str, Any]]:
    final_manifest = dict(manifest or {})
    buf = tempfile.SpooledTemporaryFile(max_size=max(1, int(spool_max_bytes)))
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if include_db:
            zf.writestr("db.json", json.dumps(db_data, separators=(",", ":")))
        if include_data:
            data_bytes = 0
            data_files = 0
            data_skipped = 0
            for path_s, rel in _iter_data_files(data_dir):
                try:
                    if _matches_exclude(rel, exclude_patterns):
                        data_skipped += 1
                        continue
                    size = int(os.path.getsize(path_s))
                    if max_file_bytes and size > max_file_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail=f"File {rel} exceeds per-file backup limit",
                        )
                    data_bytes += size
                    if max_data_bytes and data_bytes > max_data_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail="Backup data exceeds size limit",
                        )
                    data_files += 1
                    if data_files > max_files:
                        raise HTTPException(
                            status_code=413,
                            detail="Backup data exceeds file count limit",
                        )
                    arcname = f"data/{rel}"
                    zf.write(path_s, arcname=arcname)
                except HTTPException:
                    raise
                except Exception as e:
                    final_manifest.setdefault("data_errors", []).append(
                        {"path": rel, "error": str(e)}
                    )
            final_manifest["data_files"] = data_files
            final_manifest["data_bytes"] = data_bytes
            final_manifest["data_skipped"] = data_skipped
        zf.writestr("manifest.json", json.dumps(final_manifest, indent=2, sort_keys=True))
    return buf, final_manifest


def _inspect_backup_zip(
    tmp: tempfile.SpooledTemporaryFile,
    *,
    restore_db: bool,
    require_manifest: bool,
    max_file_bytes: int,
) -> tuple[Dict[str, Any] | None, list[str], Dict[str, Any]]:
    tmp.seek(0)
    with zipfile.ZipFile(tmp, "r") as zf:
        manifest, warnings = _load_manifest(zf, require_manifest=require_manifest)
        db_payload: Dict[str, Any] = {}
        if restore_db:
            try:
                info = zf.getinfo("db.json")
                if max_file_bytes and info.file_size > max_file_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail="db.json exceeds per-file size limit",
                    )
                db_raw = zf.read("db.json")
                db_payload = json.loads(db_raw.decode("utf-8") or "{}")
            except KeyError:
                db_payload = {}
                warnings.append("db.json missing")
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Invalid db.json: {e}"
                ) from e
            if not isinstance(db_payload, dict):
                raise HTTPException(status_code=400, detail="db.json must be an object")
    return manifest, warnings, db_payload


def _cleanup_backup_root(path: Path | None) -> None:
    if path is None:
        return
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def _restore_backup_data(
    tmp: tempfile.SpooledTemporaryFile,
    *,
    data_root: str,
    exclude_patterns: Iterable[str],
    max_data_bytes: int,
    max_file_bytes: int,
    max_files: int,
    overwrite_data: bool,
) -> tuple[Dict[str, Any], list[tuple[Path, Path | None]], Path | None]:
    data_root_path = Path(data_root).resolve()
    if not data_root_path.is_dir():
        raise HTTPException(status_code=400, detail="DATA_DIR is not accessible")

    tmp.seek(0)
    entries: list[DataEntry] = []
    total_bytes = 0
    skipped_excluded = 0
    with zipfile.ZipFile(tmp, "r") as zf:
        for info in zf.infolist():
            if not info.filename.startswith("data/"):
                continue
            if info.is_dir():
                continue
            rel = _normalize_rel(info.filename[len("data/") :])
            if not rel:
                continue
            if _matches_exclude(rel, exclude_patterns):
                skipped_excluded += 1
                continue
            if max_file_bytes and info.file_size > max_file_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=f"Data file {rel} exceeds per-file size limit",
                )
            total_bytes += int(info.file_size or 0)
            if max_data_bytes and total_bytes > max_data_bytes:
                raise HTTPException(
                    status_code=413,
                    detail="Backup data exceeds size limit",
                )
            if len(entries) >= max_files:
                raise HTTPException(
                    status_code=413,
                    detail="Backup data exceeds file count limit",
                )
            entries.append(DataEntry(info=info, rel=rel))

        restored = 0
        skipped_existing = 0
        data_applied: list[tuple[Path, Path | None]] = []
        backup_root: Path | None = None
        try:
            with tempfile.TemporaryDirectory(dir=data_root_path) as staging_dir:
                staging_root = Path(staging_dir)
                total_written = 0
                for entry in entries:
                    rel = _normalize_rel(entry.rel)
                    if not rel:
                        continue
                    dest = _safe_data_path(staging_root, rel)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(entry.info, "r") as src, open(dest, "wb") as dst:
                        total_written = _copy_with_limit(
                            src,
                            dst,
                            max_total_bytes=max_data_bytes,
                            max_file_bytes=max_file_bytes,
                            total=total_written,
                        )

                for entry in entries:
                    rel = _normalize_rel(entry.rel)
                    if not rel:
                        continue
                    target = _safe_data_path(data_root_path, rel)
                    if target.exists():
                        if not overwrite_data:
                            skipped_existing += 1
                            continue
                        if backup_root is None:
                            backup_root = (
                                data_root_path
                                / ".backup_restore"
                                / f"{int(time.time())}_{os.getpid()}"
                            )
                        backup_path = backup_root / rel
                        backup_path.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(target, backup_path)
                    else:
                        backup_path = None
                    target.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(staging_root / rel, target)
                    data_applied.append((target, backup_path))
                    restored += 1
        except Exception as e:
            _rollback_data(data_applied)
            _cleanup_backup_root(backup_root)
            raise HTTPException(status_code=500, detail=f"Data restore failed: {e}") from e

    summary = {
        "restored": restored,
        "skipped_existing": skipped_existing,
        "skipped_excluded": skipped_excluded,
        "bytes": total_bytes,
    }
    return summary, data_applied, backup_root


async def _export_db(
    state: AppState,
    *,
    include_auth: bool,
    tables: Iterable[TableSpec],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str]]:
    data: Dict[str, List[Dict[str, Any]]] = {}
    errors: Dict[str, str] = {}
    async with AsyncSession(state.db.engine) as session:
        for spec in tables:
            if spec.name in AUTH_TABLES and not include_auth:
                continue
            try:
                rows = (await session.exec(select(spec.model))).all()
                data[spec.name] = [r.model_dump() for r in rows]
            except Exception as e:
                errors[spec.name] = str(e)
    return data, errors


async def _restore_db(
    state: AppState,
    *,
    payload: Dict[str, Any],
    include_auth: bool,
    mode: str,
    tables: Iterable[TableSpec],
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"tables": {}}
    async with AsyncSession(state.db.engine) as session:
        try:
            async with session.begin():
                for spec in tables:
                    if spec.name in AUTH_TABLES and not include_auth:
                        continue
                    rows = payload.get(spec.name)
                    if not isinstance(rows, list):
                        continue
                    if mode == "replace":
                        await session.exec(delete(spec.model))
                    count = 0
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        rec = spec.model(**row)
                        if mode == "merge":
                            await session.merge(rec)
                        else:
                            session.add(rec)
                        count += 1
                    summary["tables"][spec.name] = count
        except Exception as e:
            try:
                await session.rollback()
            except Exception:
                pass
            raise HTTPException(
                status_code=400, detail=f"DB restore failed: {e}"
            ) from e
    return summary


async def backup_export(
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
    include_db: bool = True,
    include_data: bool = True,
    include_auth: bool = False,
    exclude_globs: List[str] | None = None,
) -> StreamingResponse:
    if getattr(state, "db", None) is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    settings = state.settings
    data_dir = str(state.settings.data_dir)
    if include_data and not os.path.isdir(data_dir):
        raise HTTPException(status_code=400, detail="DATA_DIR is not accessible")

    max_data_bytes = _mb_to_bytes(getattr(settings, "backup_max_unpacked_mb", 0))
    max_file_bytes = _mb_to_bytes(getattr(settings, "backup_max_file_mb", 0))
    max_files = max(1, int(getattr(settings, "backup_max_files", 10000)))
    exclude_patterns = list(getattr(settings, "backup_exclude_globs", ()) or ())
    exclude_patterns.extend(list(exclude_globs or []))
    spool_max_bytes = _mb_to_bytes(getattr(settings, "backup_spool_max_mb", 50))

    manifest: Dict[str, Any] = {
        "format_version": BACKUP_FORMAT_VERSION,
        "created_at": time.time(),
        "agent_id": state.settings.agent_id,
        "schema_version": await _get_schema_version(state),
        "include_db": bool(include_db),
        "include_data": bool(include_data),
        "include_auth": bool(include_auth),
        "tables": [t.name for t in TABLES if include_auth or t.name not in AUTH_TABLES],
        "exclude_globs": exclude_patterns,
        "data_files": 0,
        "data_bytes": 0,
        "data_skipped": 0,
        "data_errors": [],
        "db_errors": {},
    }

    db_data: Dict[str, List[Dict[str, Any]]] = {}
    try:
        if include_db:
            db_data, db_errors = await _export_db(
                state, include_auth=include_auth, tables=TABLES
            )
            manifest["db_errors"] = db_errors
        buf, manifest = await _run_blocking(
            state,
            _build_backup_zip,
            data_dir=data_dir,
            include_db=include_db,
            include_data=include_data,
            db_data=db_data,
            manifest=manifest,
            exclude_patterns=exclude_patterns,
            max_data_bytes=max_data_bytes,
            max_file_bytes=max_file_bytes,
            max_files=max_files,
            spool_max_bytes=spool_max_bytes,
        )
    except HTTPException as e:
        await log_event(
            state,
            action="backup.export",
            ok=False,
            error=str(e.detail),
            request=request,
        )
        raise
    except Exception as e:
        await log_event(
            state,
            action="backup.export",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=f"Backup failed: {e}") from e

    buf.seek(0)
    filename = f"wsa_backup_{int(time.time())}.zip"
    await log_event(
        state,
        action="backup.export",
        ok=True,
        payload={
            "include_db": include_db,
            "include_data": include_data,
            "include_auth": include_auth,
        },
        request=request,
    )
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
        background=BackgroundTask(buf.close),
    )


async def backup_import(
    request: Request,
    _: Dict[str, Any] = Depends(require_admin),
    state: AppState = Depends(get_state),
    file: UploadFile = File(...),
    restore_db: bool = True,
    restore_data: bool = True,
    restore_auth: bool = False,
    db_mode: str = "merge",
    overwrite_data: bool = False,
    exclude_globs: List[str] | None = None,
    require_manifest: bool = False,
    require_schema_match: bool = False,
) -> Dict[str, Any]:
    if getattr(state, "db", None) is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    if not restore_db and not restore_data:
        raise HTTPException(status_code=400, detail="Nothing to restore")
    db_mode = (db_mode or "merge").strip().lower()
    if db_mode not in ("merge", "replace"):
        raise HTTPException(status_code=400, detail="db_mode must be merge or replace")

    settings = state.settings
    max_zip_bytes = _mb_to_bytes(getattr(settings, "backup_max_zip_mb", 0))
    max_data_bytes = _mb_to_bytes(getattr(settings, "backup_max_unpacked_mb", 0))
    max_file_bytes = _mb_to_bytes(getattr(settings, "backup_max_file_mb", 0))
    max_files = max(1, int(getattr(settings, "backup_max_files", 10000)))
    spool_max_bytes = _mb_to_bytes(getattr(settings, "backup_spool_max_mb", 50))
    exclude_patterns = list(getattr(settings, "backup_exclude_globs", ()) or ())
    exclude_patterns.extend(list(exclude_globs or []))

    summary: Dict[str, Any] = {
        "ok": True,
        "db": None,
        "data": None,
        "warnings": [],
    }
    tmp: tempfile.SpooledTemporaryFile | None = None
    data_applied: list[tuple[Path, Path | None]] = []
    backup_root: Path | None = None
    db_payload: Dict[str, Any] = {}
    try:
        tmp = await _read_upload_to_temp(
            file, max_bytes=max_zip_bytes, spool_max_bytes=spool_max_bytes
        )
        manifest, warnings, db_payload = await _run_blocking(
            state,
            _inspect_backup_zip,
            tmp,
            restore_db=restore_db,
            require_manifest=require_manifest,
            max_file_bytes=max_file_bytes,
        )
        summary["warnings"] = warnings
        if manifest:
            if manifest.get("include_db") is False and restore_db:
                summary["warnings"].append(
                    "Manifest indicates backup omitted DB data"
                )
            if manifest.get("include_data") is False and restore_data:
                summary["warnings"].append(
                    "Manifest indicates backup omitted data files"
                )
            schema_version = manifest.get("schema_version")
            if schema_version is not None:
                current_schema = await _get_schema_version(state)
                try:
                    schema_int = int(schema_version)
                except Exception:
                    schema_int = None
                if (
                    schema_int is not None
                    and current_schema is not None
                    and schema_int != int(current_schema)
                ):
                    msg = (
                        f"Backup schema_version {schema_int} "
                        f"does not match current {current_schema}"
                    )
                    if require_schema_match:
                        raise HTTPException(status_code=409, detail=msg)
                    summary["warnings"].append(msg)

        if restore_data:
            data_summary, data_applied, backup_root = await _run_blocking(
                state,
                _restore_backup_data,
                tmp,
                data_root=str(state.settings.data_dir),
                exclude_patterns=exclude_patterns,
                max_data_bytes=max_data_bytes,
                max_file_bytes=max_file_bytes,
                max_files=max_files,
                overwrite_data=overwrite_data,
            )
            summary["data"] = data_summary

        if restore_db:
            if not isinstance(db_payload, dict):
                raise HTTPException(
                    status_code=400, detail="db.json must be an object"
                )
            known_tables = {spec.name for spec in TABLES}
            unknown = sorted(set(db_payload.keys()) - known_tables)
            if unknown:
                summary["warnings"].append(
                    f"db.json contains unknown tables: {', '.join(unknown)}"
                )
            try:
                summary["db"] = await _restore_db(
                    state,
                    payload=db_payload,
                    include_auth=restore_auth,
                    mode=db_mode,
                    tables=TABLES,
                )
            except HTTPException:
                if data_applied:
                    await _run_blocking(state, _rollback_data, data_applied)
                await _run_blocking(state, _cleanup_backup_root, backup_root)
                raise
            await _run_blocking(state, _cleanup_backup_root, backup_root)
        elif backup_root is not None:
            await _run_blocking(state, _cleanup_backup_root, backup_root)
    except HTTPException as e:
        await log_event(
            state,
            action="backup.import",
            ok=False,
            error=str(e.detail),
            request=request,
        )
        raise
    except zipfile.BadZipFile as e:
        await log_event(
            state,
            action="backup.import",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=400, detail="Invalid backup zip") from e
    except Exception as e:
        await log_event(
            state,
            action="backup.import",
            ok=False,
            error=str(e),
            request=request,
        )
        raise HTTPException(status_code=500, detail=f"Restore failed: {e}") from e
    finally:
        if tmp is not None:
            try:
                tmp.close()
            except Exception:
                pass

    await log_event(
        state,
        action="backup.import",
        ok=True,
        payload={
            "restore_db": restore_db,
            "restore_data": restore_data,
            "db_mode": db_mode,
            "overwrite_data": overwrite_data,
            "restore_auth": restore_auth,
        },
        request=request,
    )
    return summary

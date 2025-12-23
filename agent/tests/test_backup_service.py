from __future__ import annotations

import io
import zipfile

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from services import backup_service


def _zip_with_manifest(content: bytes | None) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if content is not None:
            zf.writestr("manifest.json", content)
    buf.seek(0)
    return zipfile.ZipFile(buf, "r")


def test_load_manifest_missing_optional() -> None:
    zf = _zip_with_manifest(None)
    manifest, warnings = backup_service._load_manifest(zf, require_manifest=False)
    assert manifest is None
    assert any("manifest" in w for w in warnings)


def test_load_manifest_missing_required() -> None:
    zf = _zip_with_manifest(None)
    with pytest.raises(HTTPException):
        backup_service._load_manifest(zf, require_manifest=True)


def test_load_manifest_invalid_json() -> None:
    zf = _zip_with_manifest(b"{not json")
    with pytest.raises(HTTPException):
        backup_service._load_manifest(zf, require_manifest=True)


def test_load_manifest_unsupported_version() -> None:
    content = b'{"format_version": 999, "created_at": 1}'
    zf = _zip_with_manifest(content)
    with pytest.raises(HTTPException):
        backup_service._load_manifest(zf, require_manifest=True)


def test_safe_data_path_rejects_escape(tmp_path) -> None:
    base = tmp_path / "data"
    base.mkdir()
    with pytest.raises(ValueError):
        backup_service._safe_data_path(base, "../escape.txt")


def test_copy_with_limit_total_exceeded() -> None:
    src = io.BytesIO(b"hello world")
    dst = io.BytesIO()
    with pytest.raises(HTTPException):
        backup_service._copy_with_limit(
            src,
            dst,
            max_total_bytes=5,
            max_file_bytes=0,
            total=0,
        )


def test_copy_with_limit_file_exceeded() -> None:
    src = io.BytesIO(b"hello world")
    dst = io.BytesIO()
    with pytest.raises(HTTPException):
        backup_service._copy_with_limit(
            src,
            dst,
            max_total_bytes=0,
            max_file_bytes=5,
            total=0,
        )


@pytest.mark.asyncio
async def test_read_upload_to_temp_enforces_limit() -> None:
    file_obj = io.BytesIO(b"x" * 20)
    upload = UploadFile(file_obj, filename="backup.zip")
    with pytest.raises(HTTPException):
        await backup_service._read_upload_to_temp(
            upload, max_bytes=10, spool_max_bytes=1
        )

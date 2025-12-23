from __future__ import annotations

import pytest

from services.db_service import DatabaseService, normalize_database_url_async


def test_normalize_database_url_async_mysql() -> None:
    assert (
        normalize_database_url_async("mysql://user:pass@host:3306/db")
        == "mysql+aiomysql://user:pass@host:3306/db"
    )


@pytest.mark.anyio
async def test_db_job_persistence_roundtrip(tmp_path) -> None:
    db = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db.init()

    await db.upsert_job(
        {"id": "j1", "kind": "k", "status": "queued", "created_at": 1.0}
    )
    await db.upsert_job(
        {"id": "j2", "kind": "k", "status": "succeeded", "created_at": 2.0}
    )

    j1 = await db.get_job("j1")
    assert j1 is not None
    assert j1["status"] == "queued"

    rows = await db.list_jobs(limit=10)
    assert [j["id"] for j in rows] == ["j2", "j1"]

    updated = await db.mark_in_flight_failed(reason="restart")
    assert updated == 1
    j1 = await db.get_job("j1")
    assert j1 is not None
    assert j1["status"] == "failed"
    assert j1["error"] == "restart"
    assert j1["finished_at"] is not None

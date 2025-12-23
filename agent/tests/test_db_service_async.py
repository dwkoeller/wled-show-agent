from __future__ import annotations

import asyncio

import pytest

from services.db_service import DatabaseService


@pytest.mark.anyio
async def test_db_service_agent_heartbeat_roundtrip(tmp_path) -> None:
    db = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db.init()

    await db.upsert_agent_heartbeat(
        agent_id="agent1",
        started_at=123.0,
        name="Agent 1",
        role="tree",
        controller_kind="wled",
        version="3.4.0",
        payload={"capabilities": ["status"]},
    )

    rows = await db.list_agent_heartbeats(limit=10)
    assert rows
    assert rows[0]["agent_id"] == "agent1"
    assert rows[0]["name"] == "Agent 1"
    assert rows[0]["payload"]["capabilities"] == ["status"]


@pytest.mark.anyio
async def test_db_service_lease_acquire_and_expire(tmp_path) -> None:
    db = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db.init()

    assert await db.try_acquire_lease(key="k1", owner_id="a1", ttl_s=1.0) is True
    assert await db.try_acquire_lease(key="k1", owner_id="a2", ttl_s=1.0) is False

    await asyncio.sleep(1.1)
    assert await db.try_acquire_lease(key="k1", owner_id="a2", ttl_s=1.0) is True


@pytest.mark.anyio
async def test_db_service_scheduler_events(tmp_path) -> None:
    db = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db.init()

    await db.add_scheduler_event(
        agent_id="agent1",
        action="apply_random_look",
        scope="fleet",
        reason="interval",
        ok=True,
        duration_s=0.25,
        payload={"theme": "candy_cane"},
    )

    rows = await db.list_scheduler_events(limit=10, agent_id="agent1")
    assert rows
    assert rows[0]["action"] == "apply_random_look"
    assert rows[0]["ok"] is True


@pytest.mark.anyio
async def test_db_service_scheduler_events_retention(tmp_path) -> None:
    db = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db.init()

    for i in range(3):
        await db.add_scheduler_event(
            agent_id="agent1",
            action="apply_random_look",
            scope="fleet",
            reason=f"test{i}",
            ok=True,
            duration_s=0.01,
            payload={"i": i},
        )

    res = await db.enforce_scheduler_events_retention(max_rows=1, max_days=None)
    assert res["ok"] is True

    rows = await db.list_scheduler_events(limit=10, agent_id="agent1")
    assert len(rows) == 1


@pytest.mark.anyio
async def test_db_service_global_kv_roundtrip(tmp_path) -> None:
    db1 = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db1.init()
    await db1.global_kv_set_json("scheduler_config", {"enabled": True, "mode": "looks"})

    db2 = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent2",
    )
    await db2.init()
    got = await db2.global_kv_get_json("scheduler_config")
    assert got is not None
    assert got["enabled"] is True
    assert got["mode"] == "looks"


@pytest.mark.anyio
async def test_db_service_event_logs_after_id(tmp_path) -> None:
    db = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db.init()

    id1 = await db.add_event_log(
        event_type="jobs",
        event="created",
        payload={"event": "created", "job_id": "1"},
    )
    id2 = await db.add_event_log(
        event_type="jobs",
        event="updated",
        payload={"event": "updated", "job_id": "1"},
    )
    assert id1 is not None
    assert id2 is not None

    rows = await db.list_event_logs_after_id(
        last_id=int(id1), limit=10, agent_id="agent1"
    )
    assert len(rows) == 1
    assert rows[0]["id"] == id2
    assert rows[0]["event_type"] == "jobs"

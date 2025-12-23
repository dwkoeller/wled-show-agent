from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from services import events_service
from services.db_service import DatabaseService


def test_format_event_payload() -> None:
    msg = events_service.EventMessage(id=42, type="jobs", data={"id": "1"}, ts=123.4)
    raw = events_service._format_event(msg)
    lines = raw.strip().split("\n")
    assert lines[0] == "id: 42"
    assert lines[1].startswith("data: ")
    payload = json.loads(lines[1][len("data: ") :].strip())
    assert payload["id"] == 42
    assert payload["type"] == "jobs"
    assert payload["data"] == {"id": "1"}
    assert payload["ts"] == 123.4


def test_event_type_from_action() -> None:
    assert events_service._event_type_from_action("scheduler.start") == "scheduler"
    assert events_service._event_type_from_action("files.upload") == "files"
    assert events_service._event_type_from_action("orchestration.run") == "orchestration"


def test_merge_history_dedup_and_order() -> None:
    db_history = [
        events_service.EventMessage(id=6, type="jobs", data={"id": 1}, ts=1.0),
        events_service.EventMessage(id=8, type="jobs", data={"id": 2}, ts=2.0),
    ]
    bus_history = [
        events_service.EventMessage(id=7, type="jobs", data={"id": 3}, ts=1.5),
        events_service.EventMessage(id=8, type="jobs", data={"id": 4}, ts=3.0),
    ]
    merged = events_service._merge_history(
        5, db_history=db_history, bus_history=bus_history
    )
    ids = [m.id for m in merged]
    assert ids == [6, 7, 8]


@pytest.mark.anyio
async def test_events_history_export_ndjson_after_id(tmp_path) -> None:
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
    id3 = await db.add_event_log(
        event_type="jobs",
        event="finished",
        payload={"event": "finished", "job_id": "1"},
    )
    assert id1 is not None and id2 is not None and id3 is not None
    state = SimpleNamespace(db=db, settings=SimpleNamespace(agent_id="agent1"))

    resp = await events_service.events_history_export(
        limit=10,
        after_id=int(id1),
        format="ndjson",
        state=state,
        _=None,
    )
    chunks: list[bytes] = []
    async for chunk in resp.body_iterator:
        if isinstance(chunk, bytes):
            chunks.append(chunk)
        else:
            chunks.append(str(chunk).encode("utf-8"))
    body = b"".join(chunks).decode("utf-8").strip()
    lines = [line for line in body.splitlines() if line.strip()]
    rows = [json.loads(line) for line in lines]
    assert [row.get("id") for row in rows] == [id2, id3]


@pytest.mark.anyio
async def test_events_history_after_id_with_since_until(tmp_path) -> None:
    db = DatabaseService(
        database_url=f"sqlite:///{tmp_path / 'test.db'}",
        agent_id="agent1",
    )
    await db.init()
    id1 = await db.add_event_log(
        event_type="jobs",
        event="created",
        payload={"event": "created", "job_id": "1"},
        created_at=10.0,
    )
    id2 = await db.add_event_log(
        event_type="jobs",
        event="updated",
        payload={"event": "updated", "job_id": "1"},
        created_at=20.0,
    )
    id3 = await db.add_event_log(
        event_type="jobs",
        event="finished",
        payload={"event": "finished", "job_id": "1"},
        created_at=30.0,
    )
    assert id1 is not None and id2 is not None and id3 is not None
    state = SimpleNamespace(db=db, settings=SimpleNamespace(agent_id="agent1"))

    res = await events_service.events_history(
        limit=10,
        after_id=int(id1),
        since=15.0,
        until=35.0,
        state=state,
        _=None,
    )
    ids = [row.get("id") for row in res.get("events", [])]
    assert ids == [id2, id3]

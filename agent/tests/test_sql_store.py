from __future__ import annotations

import time

from jobs import JobManager
from sql_store import (
    SQLJobStore,
    SQLKVStore,
    create_db_engine,
    init_db,
    normalize_database_url,
)


def test_normalize_database_url_mysql() -> None:
    assert (
        normalize_database_url("mysql://user:pass@host:3306/db")
        == "mysql+pymysql://user:pass@host:3306/db"
    )


def test_sql_job_store_upsert_list_get_and_mark_failed(tmp_path) -> None:
    engine = create_db_engine(f"sqlite:///{tmp_path / 'test.db'}")
    init_db(engine)
    store = SQLJobStore(engine=engine, agent_id="agent1")

    store.upsert_job(
        {"id": "j1", "kind": "k", "status": "queued", "created_at": 1.0, "logs": []}
    )
    store.upsert_job(
        {"id": "j2", "kind": "k", "status": "succeeded", "created_at": 2.0, "logs": []}
    )

    assert store.get_job("j1")["status"] == "queued"  # type: ignore[index]
    assert [j["id"] for j in store.list_jobs(limit=10)] == ["j2", "j1"]

    updated = store.mark_in_flight_failed(reason="restart")
    assert updated == 1
    j1 = store.get_job("j1")
    assert j1 is not None
    assert j1["status"] == "failed"
    assert j1["error"] == "restart"
    assert j1["finished_at"] is not None


def test_sql_kv_store_set_get(tmp_path) -> None:
    engine = create_db_engine(f"sqlite:///{tmp_path / 'test.db'}")
    init_db(engine)
    kv = SQLKVStore(engine=engine, agent_id="agent1")

    assert kv.get_json("missing") is None
    kv.set_json("scheduler_config", {"enabled": False, "autostart": True})
    assert kv.get_json("scheduler_config") == {"enabled": False, "autostart": True}


def test_job_manager_persists_to_sql_store(tmp_path) -> None:
    engine = create_db_engine(f"sqlite:///{tmp_path / 'test.db'}")
    init_db(engine)
    store = SQLJobStore(engine=engine, agent_id="agent1")

    mgr = JobManager(
        persist_path=str(tmp_path / "jobs.json"),
        store=store,
        max_jobs=50,
    )

    job = mgr.create(kind="test", runner=lambda _: {"ok": True})

    deadline = time.time() + 5.0
    while time.time() < deadline:
        j = mgr.get(job.id)
        if j and j.status in ("succeeded", "failed", "canceled"):
            break
        time.sleep(0.05)

    j = mgr.get(job.id)
    assert j is not None
    assert j.status == "succeeded"

    row = store.get_job(job.id)
    assert row is not None
    assert row["status"] == "succeeded"
    assert row["result"] == {"ok": True}

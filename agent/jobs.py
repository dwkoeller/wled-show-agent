from __future__ import annotations

import json
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Protocol


JobStatus = Literal["queued", "running", "succeeded", "failed", "canceled"]


class JobCanceled(RuntimeError):
    pass


@dataclass
class JobProgress:
    current: Optional[float] = None
    total: Optional[float] = None
    message: Optional[str] = None

    @staticmethod
    def from_dict(d: Any) -> "JobProgress":
        if not isinstance(d, dict):
            return JobProgress()
        return JobProgress(
            current=float(d["current"]) if d.get("current") is not None else None,
            total=float(d["total"]) if d.get("total") is not None else None,
            message=str(d["message"]) if d.get("message") is not None else None,
        )


@dataclass
class Job:
    id: str
    kind: str
    status: JobStatus
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    progress: JobProgress = field(default_factory=JobProgress)
    result: Any = None
    error: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    cancel_requested: bool = False

    @staticmethod
    def from_dict(d: Any) -> "Job":
        if not isinstance(d, dict):
            raise ValueError("Job must be an object")
        return Job(
            id=str(d.get("id") or ""),
            kind=str(d.get("kind") or ""),
            status=str(d.get("status") or "failed"),  # type: ignore[assignment]
            created_at=float(d.get("created_at") or 0.0),
            started_at=(
                float(d["started_at"]) if d.get("started_at") is not None else None
            ),
            finished_at=(
                float(d["finished_at"]) if d.get("finished_at") is not None else None
            ),
            progress=JobProgress.from_dict(d.get("progress")),
            result=d.get("result"),
            error=str(d["error"]) if d.get("error") is not None else None,
            logs=[str(x) for x in (d.get("logs") or []) if x is not None],
            cancel_requested=bool(d.get("cancel_requested")),
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "kind": str(self.kind),
            "status": str(self.status),
            "created_at": float(self.created_at),
            "started_at": (
                float(self.started_at) if self.started_at is not None else None
            ),
            "finished_at": (
                float(self.finished_at) if self.finished_at is not None else None
            ),
            "progress": {
                "current": (
                    float(self.progress.current)
                    if self.progress.current is not None
                    else None
                ),
                "total": (
                    float(self.progress.total)
                    if self.progress.total is not None
                    else None
                ),
                "message": (
                    str(self.progress.message)
                    if self.progress.message is not None
                    else None
                ),
            },
            "result": self.result,
            "error": str(self.error) if self.error is not None else None,
            "logs": list(self.logs),
            "cancel_requested": bool(self.cancel_requested),
        }


@dataclass(frozen=True)
class JobUpdateEvent:
    """
    JSON-serializable job update event for SSE.
    """

    type: str
    job: Dict[str, Any]

    def to_sse_data(self) -> str:
        return json.dumps({"type": self.type, "job": self.job}, ensure_ascii=False)


class JobContext:
    def __init__(self, manager: "JobManager", job_id: str) -> None:
        self._manager = manager
        self.job_id = job_id

    def log(self, message: str) -> None:
        self._manager.log(self.job_id, message)

    def set_progress(
        self,
        *,
        current: Optional[float] = None,
        total: Optional[float] = None,
        message: Optional[str] = None,
    ) -> None:
        self._manager.set_progress(
            self.job_id, current=current, total=total, message=message
        )

    def check_cancelled(self) -> None:
        if self._manager.is_cancel_requested(self.job_id):
            raise JobCanceled("Job canceled")


class JobStore(Protocol):
    def list_jobs(self, *, limit: int) -> List[Dict[str, Any]]: ...

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]: ...

    def upsert_job(self, job: Dict[str, Any]) -> None: ...

    def mark_in_flight_failed(self, *, reason: str = "Server restarted") -> int: ...


class JobManager:
    def __init__(
        self,
        *,
        max_jobs: int = 200,
        subscriber_queue_size: int = 200,
        persist_path: Optional[str] = None,
        store: Optional[JobStore] = None,
    ) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, Job] = {}
        self._max_jobs = max(10, int(max_jobs))

        self._subs_lock = threading.Lock()
        self._subs: List[queue.Queue[str]] = []
        self._subscriber_queue_size = max(10, int(subscriber_queue_size))

        self._store = store
        self._persist_path = str(persist_path).strip() if persist_path else None
        if self._store is not None:
            try:
                self._store.mark_in_flight_failed(reason="Server restarted")
            except Exception:
                pass
            # Best-effort migrate any existing file-based history into the DB.
            if self._persist_path:
                try:
                    for j in self._load_jobs_file(self._persist_path).values():
                        try:
                            self._store.upsert_job(j.as_dict())
                        except Exception:
                            continue
                except Exception:
                    pass
            self._load_from_store()
        elif self._persist_path:
            self._load_from_disk()

    def list_jobs(self, *, limit: int = 50) -> List[Job]:
        lim = max(1, int(limit))
        store = self._store
        if store is not None:
            try:
                persisted = store.list_jobs(limit=lim)
                jobs = [Job.from_dict(d) for d in persisted]
            except Exception:
                jobs = []
            # Overlay any in-memory jobs (active / newest) to reflect freshest progress/logs.
            with self._lock:
                for j in self._jobs.values():
                    if not j.id:
                        continue
                    if all(pj.id != j.id for pj in jobs):
                        jobs.append(j)
                    else:
                        # Replace the persisted copy with the in-memory copy.
                        jobs = [j if pj.id == j.id else pj for pj in jobs]
            jobs = sorted(jobs, key=lambda j: j.created_at, reverse=True)
            return jobs[:lim]

        with self._lock:
            jobs_mem = sorted(
                self._jobs.values(), key=lambda j: j.created_at, reverse=True
            )
            return jobs_mem[:lim]

    def status_counts(self) -> Dict[str, int]:
        """
        Return counts by status for the in-memory job set (fast, no DB access).
        """
        out: Dict[str, int] = {}
        with self._lock:
            for j in self._jobs.values():
                st = str(j.status)
                out[st] = out.get(st, 0) + 1
        return out

    def get(self, job_id: str) -> Optional[Job]:
        jid = str(job_id)
        with self._lock:
            j = self._jobs.get(jid)
        if j is not None:
            return j
        store = self._store
        if store is None:
            return None
        try:
            row = store.get_job(jid)
        except Exception:
            return None
        if not row:
            return None
        try:
            return Job.from_dict(row)
        except Exception:
            return None

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            j = self._jobs.get(str(job_id))
            return bool(j.cancel_requested) if j else False

    def cancel(self, job_id: str) -> Optional[Job]:
        jid = str(job_id)
        with self._lock:
            j = self._jobs.get(jid)
            if not j:
                return None
            j.cancel_requested = True
            # If the job hasn't started, mark it canceled immediately.
            if j.status == "queued":
                j.status = "canceled"
                j.finished_at = time.time()
            job_copy = j.as_dict()
        self._broadcast(JobUpdateEvent(type="job_update", job=job_copy).to_sse_data())
        self._persist_job(jid)
        return self.get(jid)

    def log(self, job_id: str, message: str) -> None:
        jid = str(job_id)
        msg = str(message)
        with self._lock:
            j = self._jobs.get(jid)
            if not j:
                return
            j.logs.append(msg)
            if len(j.logs) > 200:
                j.logs[:] = j.logs[-200:]
            job_copy = j.as_dict()
        self._broadcast(JobUpdateEvent(type="job_update", job=job_copy).to_sse_data())

    def set_progress(
        self,
        job_id: str,
        *,
        current: Optional[float],
        total: Optional[float],
        message: Optional[str],
    ) -> None:
        jid = str(job_id)
        with self._lock:
            j = self._jobs.get(jid)
            if not j:
                return
            if current is not None:
                j.progress.current = float(current)
            if total is not None:
                j.progress.total = float(total)
            if message is not None:
                j.progress.message = str(message)
            job_copy = j.as_dict()
        self._broadcast(JobUpdateEvent(type="job_update", job=job_copy).to_sse_data())

    def create(self, *, kind: str, runner: Callable[[JobContext], Any]) -> Job:
        jid = uuid.uuid4().hex
        now = time.time()
        job = Job(id=jid, kind=str(kind), status="queued", created_at=now)
        with self._lock:
            self._jobs[jid] = job
            self._trim_locked()
            job_copy = job.as_dict()
        self._broadcast(JobUpdateEvent(type="job_update", job=job_copy).to_sse_data())
        self._persist_job(jid)

        th = threading.Thread(
            target=self._run_job,
            args=(jid, runner),
            name=f"job_{kind}_{jid[:8]}",
            daemon=True,
        )
        th.start()
        return job

    def subscribe(self) -> queue.Queue[str]:
        q: queue.Queue[str] = queue.Queue(maxsize=self._subscriber_queue_size)
        with self._subs_lock:
            self._subs.append(q)
        return q

    def unsubscribe(self, q: queue.Queue[str]) -> None:
        with self._subs_lock:
            try:
                self._subs.remove(q)
            except ValueError:
                pass

    def _broadcast(self, payload_json: str) -> None:
        with self._subs_lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(payload_json)
            except queue.Full:
                # Best effort: drop updates if the client can't keep up.
                continue

    def _trim_locked(self) -> None:
        if len(self._jobs) <= self._max_jobs:
            return
        jobs = sorted(self._jobs.values(), key=lambda j: j.created_at)
        to_remove = jobs[: max(0, len(jobs) - self._max_jobs)]
        for j in to_remove:
            self._jobs.pop(j.id, None)

    def _run_job(self, job_id: str, runner: Callable[[JobContext], Any]) -> None:
        jid = str(job_id)
        ctx = JobContext(self, jid)

        with self._lock:
            job = self._jobs.get(jid)
            if not job:
                return
            if job.cancel_requested or job.status == "canceled":
                # Canceled before starting.
                job.status = "canceled"
                job.finished_at = job.finished_at or time.time()
                job_copy = job.as_dict()
            else:
                job.status = "running"
                job.started_at = time.time()
                job_copy = job.as_dict()
        self._broadcast(JobUpdateEvent(type="job_update", job=job_copy).to_sse_data())
        self._persist_job(jid)

        # If canceled before start, do not run.
        if job_copy.get("status") == "canceled":
            self._persist_job(jid)
            return

        try:
            ctx.check_cancelled()
            ctx.set_progress(message="Running…")
            result = runner(ctx)
            ctx.check_cancelled()
            with self._lock:
                job = self._jobs.get(jid)
                if job:
                    job.status = "succeeded"
                    job.finished_at = time.time()
                    job.result = result
                    job.error = None
                    job_copy = job.as_dict()
            self._broadcast(
                JobUpdateEvent(type="job_update", job=job_copy).to_sse_data()
            )
            self._persist_job(jid)
        except JobCanceled as e:
            with self._lock:
                job = self._jobs.get(jid)
                if job:
                    job.status = "canceled"
                    job.finished_at = time.time()
                    job.error = str(e)
                    job_copy = job.as_dict()
            self._broadcast(
                JobUpdateEvent(type="job_update", job=job_copy).to_sse_data()
            )
            self._persist_job(jid)
        except Exception as e:
            with self._lock:
                job = self._jobs.get(jid)
                if job:
                    job.status = "failed"
                    job.finished_at = time.time()
                    job.error = str(e)
                    job_copy = job.as_dict()
            self._broadcast(
                JobUpdateEvent(type="job_update", job=job_copy).to_sse_data()
            )
            self._persist_job(jid)

    def _load_from_store(self) -> None:
        store = self._store
        if store is None:
            return
        try:
            rows = store.list_jobs(limit=self._max_jobs)
        except Exception:
            return

        now = time.time()
        loaded: Dict[str, Job] = {}
        for row in rows:
            try:
                j = Job.from_dict(row)
            except Exception:
                continue
            if not j.id:
                continue
            # Jobs can't survive a restart; mark in-flight ones failed.
            if j.status in ("queued", "running"):
                j.status = "failed"
                j.finished_at = j.finished_at or now
                j.error = j.error or "Server restarted"
            loaded[j.id] = j

        with self._lock:
            self._jobs = loaded
            self._trim_locked()

    def _load_jobs_file(self, path_s: str) -> Dict[str, Job]:
        p = Path(path_s)
        if not p.is_file():
            return {}
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}

        rows: List[Dict[str, Any]] = []
        if isinstance(raw, dict) and isinstance(raw.get("jobs"), list):
            rows = [x for x in raw["jobs"] if isinstance(x, dict)]
        elif isinstance(raw, list):
            rows = [x for x in raw if isinstance(x, dict)]
        else:
            return {}

        now = time.time()
        loaded: Dict[str, Job] = {}
        for row in rows:
            try:
                j = Job.from_dict(row)
            except Exception:
                continue
            if not j.id:
                continue
            # Jobs can't survive a restart; mark in-flight ones failed.
            if j.status in ("queued", "running"):
                j.status = "failed"
                j.finished_at = j.finished_at or now
                j.error = j.error or "Server restarted"
            loaded[j.id] = j
        return loaded

    def _load_from_disk(self) -> None:
        path_s = self._persist_path
        if not path_s:
            return
        loaded = self._load_jobs_file(path_s)
        with self._lock:
            self._jobs = loaded
            self._trim_locked()

    def _persist_job(self, job_id: str) -> None:
        store = self._store
        if store is not None:
            with self._lock:
                j = self._jobs.get(str(job_id))
                job_dict = j.as_dict() if j else None
            if job_dict is None:
                return
            try:
                store.upsert_job(job_dict)
            except Exception:
                # Best-effort: fall back to filesystem persistence.
                pass
        self._persist()

    def _persist(self) -> None:
        path_s = self._persist_path
        if not path_s:
            return

        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
            jobs = jobs[: self._max_jobs]
            payload = {"version": 1, "jobs": [j.as_dict() for j in jobs]}

        p = Path(path_s)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.replace(tmp, p)
        except Exception:
            # Best-effort: persistence should never break jobs.
            return


def sse_format_event(*, event: str, data: str) -> str:
    # SSE requires each line to be prefixed; split to preserve newlines.
    out = [f"event: {event}"]
    for line in str(data).splitlines() or [""]:
        out.append(f"data: {line}")
    return "\n".join(out) + "\n\n"


def jobs_snapshot_payload(jobs: Iterable[Job]) -> str:
    return json.dumps(
        {"type": "snapshot", "jobs": [j.as_dict() for j in jobs]}, ensure_ascii=False
    )

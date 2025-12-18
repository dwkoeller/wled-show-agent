from __future__ import annotations

import json
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Literal, Optional


JobStatus = Literal["queued", "running", "succeeded", "failed", "canceled"]


class JobCanceled(RuntimeError):
    pass


@dataclass
class JobProgress:
    current: Optional[float] = None
    total: Optional[float] = None
    message: Optional[str] = None


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


class JobManager:
    def __init__(
        self, *, max_jobs: int = 200, subscriber_queue_size: int = 200
    ) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, Job] = {}
        self._max_jobs = max(10, int(max_jobs))

        self._subs_lock = threading.Lock()
        self._subs: List[queue.Queue[str]] = []
        self._subscriber_queue_size = max(10, int(subscriber_queue_size))

    def list_jobs(self, *, limit: int = 50) -> List[Job]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
            return jobs[: max(1, int(limit))]

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(str(job_id))

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

        # If canceled before start, do not run.
        if job_copy.get("status") == "canceled":
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

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
)



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


# ----------------------------
# Async Job Manager
# ----------------------------


AsyncJobRunner = Callable[["AsyncJobContext"], Awaitable[Any]]
JobEventCallback = Callable[[str, Dict[str, Any]], Awaitable[None]]


class AsyncJobContext:
    def __init__(self, manager: "AsyncJobManager", job_id: str) -> None:
        self._manager = manager
        self.job_id = str(job_id)

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


class AsyncJobManager:
    """
    Async job runner with event callbacks and optional DB persistence.

    Notes:
    - Job runners may call ctx.set_progress/log from worker threads (e.g., via the blocking
      worker pool). These methods are thread-safe and schedule event callbacks onto the loop.
    - Persistence is best-effort and intentionally *not* performed on every progress update.
    """

    def __init__(
        self,
        *,
        loop: "asyncio.AbstractEventLoop",
        max_jobs: int = 200,
        queue_size: int = 50,
        worker_count: int = 2,
        db: Any = None,
        event_cb: JobEventCallback | None = None,
    ) -> None:
        import asyncio  # local import to avoid module-level event-loop use

        self._loop = loop
        self._lock = threading.Lock()
        self._jobs: Dict[str, Job] = {}
        self._max_jobs = max(10, int(max_jobs))

        self._queue: "asyncio.Queue[str]" = asyncio.Queue(
            maxsize=max(1, int(queue_size))
        )
        self._worker_count = max(1, int(worker_count))
        self._workers: List["asyncio.Task[None]"] = []
        self._job_tasks: Dict[str, "asyncio.Task[None]"] = {}
        self._runners: Dict[str, AsyncJobRunner] = {}
        self._stop = asyncio.Event()

        self._db = db
        self._event_cb: JobEventCallback | None = event_cb

    def set_event_callback(self, cb: JobEventCallback | None) -> None:
        self._event_cb = cb

    async def init(self) -> None:
        """
        Load persisted jobs (DB preferred, else file) and start worker tasks.
        """
        import asyncio

        db = self._db
        if db is not None:
            try:
                await db.mark_in_flight_failed(reason="Server restarted")
            except Exception:
                pass
            await self._load_from_db()

        self._start_workers()

    async def shutdown(self, *, reason: str = "Server shutting down") -> None:
        import asyncio

        self._stop.set()

        # Cancel workers.
        for w in list(self._workers):
            try:
                w.cancel()
            except Exception:
                continue
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

        # Mark any queued/running jobs as failed (best-effort).
        to_fail: List[str] = []
        with self._lock:
            for jid, j in self._jobs.items():
                if j.status in ("queued", "running"):
                    to_fail.append(jid)
        for jid in to_fail:
            self._set_terminal_status(
                jid,
                status="failed",
                error=str(reason),
            )
        for jid in to_fail:
            await self._persist_job(jid)

        # Cancel any running job tasks after status update (best-effort).
        for t in list(self._job_tasks.values()):
            try:
                t.cancel()
            except Exception:
                continue
        await asyncio.gather(*self._job_tasks.values(), return_exceptions=True)
        self._job_tasks = {}

    # ---- Queries ----

    async def list_jobs(self, *, limit: int = 50) -> List[Job]:
        lim = max(1, int(limit))
        db = self._db
        if db is not None:
            jobs: List[Job] = []
            try:
                persisted = await db.list_jobs(limit=lim)
                jobs = [Job.from_dict(d) for d in (persisted or [])]
            except Exception:
                jobs = []
            with self._lock:
                for j in self._jobs.values():
                    if not j.id:
                        continue
                    if all(pj.id != j.id for pj in jobs):
                        jobs.append(j)
                    else:
                        jobs = [j if pj.id == j.id else pj for pj in jobs]
            jobs = sorted(jobs, key=lambda j: j.created_at, reverse=True)
            return jobs[:lim]

        with self._lock:
            jobs_mem = sorted(
                self._jobs.values(), key=lambda j: j.created_at, reverse=True
            )
            return jobs_mem[:lim]

    async def get(self, job_id: str) -> Optional[Job]:
        jid = str(job_id)
        with self._lock:
            j = self._jobs.get(jid)
        if j is not None:
            return j
        db = self._db
        if db is None:
            return None
        try:
            row = await db.get_job(jid)
        except Exception:
            return None
        if not row:
            return None
        try:
            return Job.from_dict(row)
        except Exception:
            return None

    def status_counts(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        with self._lock:
            for j in self._jobs.values():
                st = str(j.status)
                out[st] = out.get(st, 0) + 1
        return out

    def queue_full(self) -> bool:
        try:
            return bool(self._queue.full())
        except Exception:
            return False

    def queue_stats(self) -> Dict[str, int]:
        try:
            size = int(self._queue.qsize())
            maxsize = int(getattr(self._queue, "maxsize", 0) or 0)
        except Exception:
            size = 0
            maxsize = 0
        return {
            "size": size,
            "max": maxsize,
            "workers": int(self._worker_count),
        }

    # ---- Mutation (thread-safe) ----

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            j = self._jobs.get(str(job_id))
            return bool(j.cancel_requested) if j else False

    async def cancel(self, job_id: str) -> Optional[Job]:
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

        self._emit_event("jobs", {"event": "cancel_requested", "job": job_copy})
        if job_copy.get("status") == "canceled":
            self._emit_event("jobs", {"event": "canceled", "job": job_copy})
        await self._persist_job(jid)

        # Cancel running task if present.
        task = self._job_tasks.get(jid)
        if task is not None:
            try:
                task.cancel()
            except Exception:
                pass

        return await self.get(jid)

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
        self._emit_event("jobs", {"event": "log", "job": job_copy})

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
        self._emit_event("jobs", {"event": "progress", "job": job_copy})

    # ---- Creation / execution ----

    async def create(self, *, kind: str, runner: AsyncJobRunner) -> Job:
        if self._queue.full():
            raise RuntimeError("Job queue is full; try again later.")

        jid = uuid.uuid4().hex
        now = time.time()
        job = Job(id=jid, kind=str(kind), status="queued", created_at=now)

        with self._lock:
            self._jobs[jid] = job
            self._runners[jid] = runner
            self._trim_locked()
            job_copy = job.as_dict()

        self._emit_event("jobs", {"event": "created", "job": job_copy})
        await self._persist_job(jid)

        try:
            self._queue.put_nowait(jid)
        except Exception:
            # If we somehow couldn't enqueue, fail the job immediately.
            self._set_terminal_status(
                jid,
                status="failed",
                error="Job queue enqueue failed",
            )
            await self._persist_job(jid)
        return job

    # ---- Internals ----

    def _start_workers(self) -> None:
        import asyncio

        if self._workers:
            return
        for i in range(self._worker_count):
            self._workers.append(
                asyncio.create_task(self._worker_loop(), name=f"jobs_worker_{i}")
            )

    async def _worker_loop(self) -> None:
        import asyncio

        while not self._stop.is_set():
            try:
                jid = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                await self._run_queued_job(jid)
            finally:
                try:
                    self._queue.task_done()
                except Exception:
                    pass

    async def _run_queued_job(self, job_id: str) -> None:
        jid = str(job_id)
        runner = self._runners.get(jid)
        if runner is None:
            return

        with self._lock:
            job = self._jobs.get(jid)
            if not job:
                return
            if job.cancel_requested or job.status == "canceled":
                job.status = "canceled"
                job.finished_at = job.finished_at or time.time()
                job_copy = job.as_dict()
            else:
                job.status = "running"
                job.started_at = time.time()
                job_copy = job.as_dict()

        self._emit_event(
            "jobs",
            {
                "event": "canceled"
                if job_copy.get("status") == "canceled"
                else "started",
                "job": job_copy,
            },
        )
        await self._persist_job(jid)

        # If canceled before start, do not run.
        if job_copy.get("status") == "canceled":
            return

        task = self._loop.create_task(self._execute_job(jid, runner))
        self._job_tasks[jid] = task
        try:
            await task
        finally:
            self._job_tasks.pop(jid, None)
            self._runners.pop(jid, None)

    async def _execute_job(self, job_id: str, runner: AsyncJobRunner) -> None:
        jid = str(job_id)
        ctx = AsyncJobContext(self, jid)
        try:
            ctx.check_cancelled()
            ctx.set_progress(message="Running…")
            result = await runner(ctx)
            ctx.check_cancelled()

            with self._lock:
                job = self._jobs.get(jid)
                if job:
                    job.status = "succeeded"
                    job.finished_at = time.time()
                    job.result = result
                    job.error = None
                    job_copy = job.as_dict()
                else:
                    job_copy = {"id": jid, "status": "succeeded"}

            self._emit_event("jobs", {"event": "succeeded", "job": job_copy})
            await self._persist_job(jid)
        except JobCanceled as e:
            self._set_terminal_status(jid, status="canceled", error=str(e))
            await self._persist_job(jid)
        except asyncio.CancelledError:
            # Treat task cancellation as user cancel/shutdown.
            self._set_terminal_status(jid, status="canceled", error="Job canceled")
            await self._persist_job(jid)
        except Exception as e:
            self._set_terminal_status(jid, status="failed", error=str(e))
            await self._persist_job(jid)

    def _set_terminal_status(
        self, job_id: str, *, status: JobStatus, error: str | None
    ) -> None:
        jid = str(job_id)
        with self._lock:
            job = self._jobs.get(jid)
            if not job:
                return
            job.status = status
            job.finished_at = job.finished_at or time.time()
            job.error = str(error) if error is not None else None
            job_copy = job.as_dict()
        self._emit_event("jobs", {"event": str(status), "job": job_copy})

    def _emit_event(self, event_type: str, data: Dict[str, Any]) -> None:
        cb = self._event_cb
        if cb is None:
            return

        async def _runner() -> None:
            try:
                await cb(event_type, dict(data))
            except Exception:
                return

        def _schedule() -> None:
            try:
                self._loop.create_task(_runner())
            except Exception:
                return

        try:
            running = asyncio.get_running_loop()
        except Exception:
            running = None
        if running is self._loop:
            _schedule()
        else:
            try:
                self._loop.call_soon_threadsafe(_schedule)
            except Exception:
                return

    def _trim_locked(self) -> None:
        if len(self._jobs) <= self._max_jobs:
            return
        jobs = sorted(self._jobs.values(), key=lambda j: j.created_at)
        to_remove = jobs[: max(0, len(jobs) - self._max_jobs)]
        for j in to_remove:
            self._jobs.pop(j.id, None)
            self._runners.pop(j.id, None)
            self._job_tasks.pop(j.id, None)

    async def _load_from_db(self) -> None:
        db = self._db
        if db is None:
            return
        try:
            rows = await db.list_jobs(limit=self._max_jobs)
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
            if j.status in ("queued", "running"):
                j.status = "failed"
                j.finished_at = j.finished_at or now
                j.error = j.error or "Server restarted"
            loaded[j.id] = j

        with self._lock:
            self._jobs = loaded
            self._trim_locked()

    async def _persist_job(self, job_id: str) -> None:
        jid = str(job_id)
        with self._lock:
            j = self._jobs.get(jid)
            job_dict = j.as_dict() if j else None
        if job_dict is None:
            return

        db = self._db
        if db is not None:
            try:
                await db.upsert_job(job_dict)
            except Exception:
                pass
            return

"""
Job manager: thread-safe ETL job tracking with concurrency protection.

Only ONE ETL execution is allowed at a time (enforced by a threading.Lock).
Jobs are tracked in memory. For production persistence, replace _jobs dict
with Redis or a database table.
"""
import logging
import threading
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional

from models.etl import (
    ETLRunRequest,
    JobStatus,
    JobStatusResponse,
    ObjectResult,
    ObjectError,
)

logger = logging.getLogger(__name__)

MAX_JOB_HISTORY = 100


class JobManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._jobs: OrderedDict[str, JobStatusResponse] = OrderedDict()
        self._running = False

    def submit(self, request: ETLRunRequest) -> JobStatusResponse:
        """
        Submit an ETL job. Returns immediately with QUEUED status.
        Raises RuntimeError if another ETL is already running.
        """
        if self._running:
            raise RuntimeError(
                "An ETL job is already running. "
                "Wait for it to finish or check /api/v1/etl/jobs for status."
            )

        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        job = JobStatusResponse(
            job_id=job_id,
            status=JobStatus.QUEUED,
            object_types=request.object_types,
            created_at=now,
        )
        self._jobs[job_id] = job
        self._trim_history()

        thread = threading.Thread(
            target=self._execute_job,
            args=(job_id, request),
            daemon=True,
        )
        thread.start()

        return job

    def get_job(self, job_id: str) -> Optional[JobStatusResponse]:
        return self._jobs.get(job_id)

    def list_jobs(self, limit: int = 20) -> list[JobStatusResponse]:
        jobs = list(self._jobs.values())
        jobs.reverse()
        return jobs[:limit]

    @property
    def is_running(self) -> bool:
        return self._running

    def _execute_job(self, job_id: str, request: ETLRunRequest):
        """Background execution of the ETL job."""
        from etl.config import ETLConfig
        from service.etl_runner import run_etl

        job = self._jobs[job_id]

        if not self._lock.acquire(blocking=False):
            job.status = JobStatus.FAILED
            job.errors.append(ObjectError(
                object_type="*",
                error="CONCURRENCY",
                detail="Another ETL acquired the lock first",
            ))
            job.finished_at = datetime.now(timezone.utc).isoformat()
            return

        self._running = True
        try:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now(timezone.utc).isoformat()

            object_types = request.object_types
            for i, obj_type in enumerate(object_types):
                job.progress = f"{i}/{len(object_types)} objects completed"
                try:
                    config = ETLConfig.from_env()
                    config.object_type = obj_type
                    config.log_level = request.log_level
                    config.force_full_load = request.force_full_load
                    config.validate()

                    summary = run_etl(config)
                    job.results.append(ObjectResult(
                        object_type=summary.get("object_type", obj_type),
                        status=summary.get("status", "unknown"),
                        duration_seconds=summary.get("duration_seconds", 0),
                        records_fetched=summary.get("records_fetched", 0),
                        records_processed_ok=summary.get("records_processed_ok", 0),
                        records_failed=summary.get("records_failed", 0),
                        db_upserts=summary.get("db_upserts", 0),
                        records_deleted=summary.get("records_deleted", 0),
                        sync_mode=summary.get("sync_mode"),
                    ))
                except Exception as e:
                    logger.error("Error processing '%s': %s", obj_type, e, exc_info=True)
                    job.errors.append(ObjectError(
                        object_type=obj_type,
                        error=type(e).__name__,
                        detail=str(e),
                    ))

            job.progress = f"{len(object_types)}/{len(object_types)} objects completed"
            job.status = JobStatus.COMPLETED
            job.finished_at = datetime.now(timezone.utc).isoformat()

        except Exception as e:
            logger.error("Fatal job error: %s", e, exc_info=True)
            job.status = JobStatus.FAILED
            job.finished_at = datetime.now(timezone.utc).isoformat()

        finally:
            self._running = False
            self._lock.release()

    def _trim_history(self):
        """Remove oldest jobs if history exceeds MAX_JOB_HISTORY."""
        while len(self._jobs) > MAX_JOB_HISTORY:
            self._jobs.popitem(last=False)


# Module-level singleton
job_manager = JobManager()

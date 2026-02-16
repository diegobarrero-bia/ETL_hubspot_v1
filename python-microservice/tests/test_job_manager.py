"""Tests para el JobManager: concurrencia, ciclo de vida de jobs."""
import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from models.etl import ETLRunRequest, JobStatus
from service.job_manager import JobManager


@pytest.fixture
def manager():
    """Fresh JobManager per test."""
    m = JobManager()
    yield m
    m._jobs.clear()
    m._running = False
    if m._lock.locked():
        m._lock.release()


def _make_request(object_types=None):
    return ETLRunRequest(
        object_types=object_types or ["contacts"],
        force_full_load=False,
    )


class TestSubmitJob:
    @patch("service.job_manager.threading.Thread")
    def test_submit_creates_queued_job(self, mock_thread_cls, manager):
        mock_thread_cls.return_value = MagicMock()

        request = _make_request(["services"])
        job = manager.submit(request)

        assert job.status == JobStatus.QUEUED
        assert job.object_types == ["services"]
        assert job.job_id is not None
        assert job.created_at is not None

    @patch("service.job_manager.threading.Thread")
    def test_submit_while_running_raises(self, mock_thread_cls, manager):
        mock_thread_cls.return_value = MagicMock()
        manager._running = True

        with pytest.raises(RuntimeError, match="already running"):
            manager.submit(_make_request())

    @patch("service.job_manager.threading.Thread")
    def test_submit_starts_thread(self, mock_thread_cls, manager):
        mock_thread = MagicMock()
        mock_thread_cls.return_value = mock_thread

        manager.submit(_make_request())

        mock_thread_cls.assert_called_once()
        mock_thread.start.assert_called_once()


class TestGetJob:
    def test_get_existing_job(self, manager):
        from models.etl import JobStatusResponse
        job = JobStatusResponse(
            job_id="test-123",
            status=JobStatus.QUEUED,
            object_types=["contacts"],
        )
        manager._jobs["test-123"] = job

        result = manager.get_job("test-123")
        assert result.job_id == "test-123"

    def test_get_nonexistent_returns_none(self, manager):
        assert manager.get_job("nonexistent") is None


class TestListJobs:
    def test_list_returns_most_recent_first(self, manager):
        from models.etl import JobStatusResponse
        for i in range(5):
            manager._jobs[f"job-{i}"] = JobStatusResponse(
                job_id=f"job-{i}",
                status=JobStatus.COMPLETED,
                object_types=["contacts"],
            )

        jobs = manager.list_jobs(limit=3)
        assert len(jobs) == 3
        assert jobs[0].job_id == "job-4"
        assert jobs[2].job_id == "job-2"

    def test_list_respects_limit(self, manager):
        from models.etl import JobStatusResponse
        for i in range(10):
            manager._jobs[f"job-{i}"] = JobStatusResponse(
                job_id=f"job-{i}",
                status=JobStatus.COMPLETED,
                object_types=["contacts"],
            )

        assert len(manager.list_jobs(limit=3)) == 3
        assert len(manager.list_jobs(limit=20)) == 10


class TestTrimHistory:
    def test_trims_when_exceeding_max(self, manager):
        from models.etl import JobStatusResponse

        for i in range(105):
            manager._jobs[f"job-{i}"] = JobStatusResponse(
                job_id=f"job-{i}",
                status=JobStatus.COMPLETED,
                object_types=["contacts"],
            )
        manager._trim_history()

        assert len(manager._jobs) == 100
        assert "job-0" not in manager._jobs
        assert "job-4" not in manager._jobs
        assert "job-5" in manager._jobs


class TestExecuteJob:
    @patch("service.etl_runner.run_etl")
    @patch("etl.config.ETLConfig.from_env")
    def test_successful_execution(self, mock_from_env, mock_run_etl, manager):
        mock_config = MagicMock()
        mock_config.validate = MagicMock()
        mock_from_env.return_value = mock_config

        mock_run_etl.return_value = {
            "object_type": "contacts",
            "status": "healthy",
            "duration_seconds": 10.5,
            "records_fetched": 100,
            "records_processed_ok": 100,
            "records_failed": 0,
            "db_upserts": 100,
            "records_deleted": 0,
            "sync_mode": "full",
        }

        request = _make_request(["contacts"])
        from models.etl import JobStatusResponse
        job = JobStatusResponse(
            job_id="test-job",
            status=JobStatus.QUEUED,
            object_types=["contacts"],
        )
        manager._jobs["test-job"] = job

        manager._execute_job("test-job", request)

        assert job.status == JobStatus.COMPLETED
        assert len(job.results) == 1
        assert job.results[0].object_type == "contacts"
        assert job.results[0].status == "healthy"
        assert job.results[0].records_processed_ok == 100
        assert job.finished_at is not None
        assert not manager._running

    @patch("service.etl_runner.run_etl")
    @patch("etl.config.ETLConfig.from_env")
    def test_error_isolation(self, mock_from_env, mock_run_etl, manager):
        """One object failing should not block others."""
        mock_config = MagicMock()
        mock_config.validate = MagicMock()
        mock_from_env.return_value = mock_config

        mock_run_etl.side_effect = [
            Exception("API Error"),
            {
                "object_type": "deals",
                "status": "healthy",
                "duration_seconds": 5.0,
                "records_fetched": 50,
                "records_processed_ok": 50,
                "records_failed": 0,
                "db_upserts": 50,
                "records_deleted": 0,
                "sync_mode": "incremental",
            },
        ]

        request = _make_request(["services", "deals"])
        from models.etl import JobStatusResponse
        job = JobStatusResponse(
            job_id="test-job",
            status=JobStatus.QUEUED,
            object_types=["services", "deals"],
        )
        manager._jobs["test-job"] = job

        manager._execute_job("test-job", request)

        assert job.status == JobStatus.COMPLETED
        assert len(job.errors) == 1
        assert job.errors[0].object_type == "services"
        assert len(job.results) == 1
        assert job.results[0].object_type == "deals"

    def test_lock_conflict(self, manager):
        """If lock is already held, job should fail."""
        from models.etl import JobStatusResponse

        manager._lock.acquire()  # Hold the lock

        job = JobStatusResponse(
            job_id="test-job",
            status=JobStatus.QUEUED,
            object_types=["contacts"],
        )
        manager._jobs["test-job"] = job

        manager._execute_job("test-job", _make_request())

        assert job.status == JobStatus.FAILED
        assert len(job.errors) == 1
        assert job.errors[0].error == "CONCURRENCY"

        manager._lock.release()

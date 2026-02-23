"""Tests para el scheduler de ETL."""
import os
from unittest.mock import patch, MagicMock

import pytest

from service import scheduler as scheduler_module


class TestStartScheduler:
    @patch.dict(os.environ, {}, clear=True)
    def test_disabled_by_default(self):
        """Scheduler should not start when SCHEDULER_ENABLED is not set."""
        scheduler_module.scheduler = None
        scheduler_module.start_scheduler()
        assert scheduler_module.scheduler is None

    @patch.dict(os.environ, {"SCHEDULER_ENABLED": "false"})
    def test_disabled_when_false(self):
        scheduler_module.scheduler = None
        scheduler_module.start_scheduler()
        assert scheduler_module.scheduler is None

    @patch("service.scheduler.BackgroundScheduler")
    @patch.dict(os.environ, {
        "SCHEDULER_ENABLED": "true",
        "SCHEDULER_CRON": "30 2 * * *",
    })
    def test_starts_when_enabled(self, mock_scheduler_cls):
        mock_sched = MagicMock()
        mock_scheduler_cls.return_value = mock_sched

        scheduler_module.start_scheduler()

        mock_scheduler_cls.assert_called_once()
        mock_sched.add_job.assert_called_once()
        mock_sched.start.assert_called_once()

        # Verify the job was registered with correct ID
        call_kwargs = mock_sched.add_job.call_args
        assert call_kwargs.kwargs["id"] == "etl_periodic"
        assert call_kwargs.kwargs["replace_existing"] is True

        # Cleanup
        scheduler_module.scheduler = None


class TestShutdownScheduler:
    def test_shutdown_when_no_scheduler(self):
        """Should not raise when scheduler is None."""
        scheduler_module.scheduler = None
        scheduler_module.shutdown_scheduler()

    def test_shutdown_calls_scheduler(self):
        mock_sched = MagicMock()
        scheduler_module.scheduler = mock_sched

        scheduler_module.shutdown_scheduler()

        mock_sched.shutdown.assert_called_once_with(wait=False)
        scheduler_module.scheduler = None


class TestScheduledETLRun:
    @patch("service.job_manager.job_manager")
    @patch.dict(os.environ, {
        "SCHEDULER_OBJECT_TYPES": "services,p50445259_meters",
        "SCHEDULER_FORCE_FULL_LOAD": "false",
    })
    def test_submits_job_with_correct_objects(self, mock_manager):
        mock_manager.submit.return_value = MagicMock(job_id="test-123")

        scheduler_module._scheduled_etl_run()

        mock_manager.submit.assert_called_once()
        request = mock_manager.submit.call_args[0][0]
        assert request.object_types == ["services", "p50445259_meters"]
        assert request.force_full_load is False

    @patch("service.job_manager.job_manager")
    @patch.dict(os.environ, {
        "SCHEDULER_OBJECT_TYPES": "contacts",
        "SCHEDULER_FORCE_FULL_LOAD": "true",
    })
    def test_force_full_load_from_env(self, mock_manager):
        mock_manager.submit.return_value = MagicMock(job_id="test-456")

        scheduler_module._scheduled_etl_run()

        request = mock_manager.submit.call_args[0][0]
        assert request.force_full_load is True

    @patch("service.job_manager.job_manager")
    @patch.dict(os.environ, {"SCHEDULER_OBJECT_TYPES": "contacts"})
    def test_handles_already_running_gracefully(self, mock_manager):
        """When ETL is running, scheduler should skip without error."""
        mock_manager.submit.side_effect = RuntimeError("already running")

        # Should not raise
        scheduler_module._scheduled_etl_run()

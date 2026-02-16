"""Tests para los endpoints HTTP del ETL."""
import time
from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from main import app
from models.etl import JobStatus, JobStatusResponse


@pytest.fixture
def client():
    return TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "etl-hubspot-postgres"


class TestTriggerETL:
    @patch("api.v1.etl.job_manager")
    def test_run_returns_202_with_job_id(self, mock_manager, client):
        mock_manager.submit.return_value = JobStatusResponse(
            job_id="test-uuid-123",
            status=JobStatus.QUEUED,
            object_types=["contacts"],
        )

        response = client.post(
            "/api/v1/etl/run",
            json={"object_types": ["contacts"]},
        )

        assert response.status_code == 202
        data = response.json()
        assert data["job_id"] == "test-uuid-123"
        assert data["status"] == "queued"
        assert data["message"] == "ETL job queued for processing"

    @patch("api.v1.etl.job_manager")
    def test_run_returns_409_when_already_running(self, mock_manager, client):
        mock_manager.submit.side_effect = RuntimeError("An ETL job is already running.")

        response = client.post(
            "/api/v1/etl/run",
            json={"object_types": ["contacts"]},
        )

        assert response.status_code == 409
        data = response.json()["detail"]
        assert data["error"] == "ETL_ALREADY_RUNNING"

    def test_run_validates_empty_object_types(self, client):
        response = client.post(
            "/api/v1/etl/run",
            json={"object_types": []},
        )
        assert response.status_code == 422

    def test_run_validates_missing_object_types(self, client):
        response = client.post(
            "/api/v1/etl/run",
            json={},
        )
        assert response.status_code == 422


class TestJobStatus:
    @patch("api.v1.etl.job_manager")
    def test_get_job_returns_200(self, mock_manager, client):
        mock_manager.get_job.return_value = JobStatusResponse(
            job_id="test-uuid-123",
            status=JobStatus.RUNNING,
            object_types=["contacts"],
            progress="0/1 objects completed",
        )

        response = client.get("/api/v1/etl/jobs/test-uuid-123")

        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == "test-uuid-123"
        assert data["status"] == "running"
        assert data["progress"] == "0/1 objects completed"

    @patch("api.v1.etl.job_manager")
    def test_get_job_returns_404_for_unknown_id(self, mock_manager, client):
        mock_manager.get_job.return_value = None

        response = client.get("/api/v1/etl/jobs/nonexistent")

        assert response.status_code == 404
        data = response.json()["detail"]
        assert data["error"] == "JOB_NOT_FOUND"


class TestJobList:
    @patch("api.v1.etl.job_manager")
    def test_list_jobs_returns_200(self, mock_manager, client):
        mock_manager.list_jobs.return_value = [
            JobStatusResponse(
                job_id="job-1",
                status=JobStatus.COMPLETED,
                object_types=["contacts"],
            ),
            JobStatusResponse(
                job_id="job-2",
                status=JobStatus.RUNNING,
                object_types=["deals"],
            ),
        ]

        response = client.get("/api/v1/etl/jobs")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["jobs"]) == 2
        assert data["jobs"][0]["job_id"] == "job-1"

    @patch("api.v1.etl.job_manager")
    def test_list_jobs_with_limit(self, mock_manager, client):
        mock_manager.list_jobs.return_value = []

        response = client.get("/api/v1/etl/jobs?limit=5")

        assert response.status_code == 200
        mock_manager.list_jobs.assert_called_once_with(limit=5)

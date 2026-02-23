"""API routes for ETL operations."""
import logging

from fastapi import APIRouter, HTTPException, Query

from models.etl import (
    ETLRunRequest,
    ETLRunResponse,
    JobStatusResponse,
    JobListResponse,
    ErrorResponse,
)
from service.job_manager import job_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/etl", tags=["etl"])


@router.post("/run", response_model=ETLRunResponse, status_code=202)
def trigger_etl(request: ETLRunRequest):
    """
    Trigger an ETL execution asynchronously.

    Returns 202 + job_id. Use GET /etl/jobs/{job_id} to poll status.
    Returns 409 if another ETL is already running.
    """
    try:
        job = job_manager.submit(request)
        return ETLRunResponse(
            job_id=job.job_id,
            status=job.status,
            message="ETL job queued for processing",
        )
    except RuntimeError as e:
        raise HTTPException(
            status_code=409,
            detail=ErrorResponse(
                error="ETL_ALREADY_RUNNING",
                message=str(e),
            ).model_dump(),
        )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str):
    """Get status of an ETL job."""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=ErrorResponse(
                error="JOB_NOT_FOUND",
                message=f"Job {job_id} not found",
            ).model_dump(),
        )
    return job


@router.get("/jobs", response_model=JobListResponse)
def list_jobs(limit: int = Query(default=20, ge=1, le=100)):
    """List recent ETL jobs."""
    jobs = job_manager.list_jobs(limit=limit)
    return JobListResponse(jobs=jobs, total=len(jobs))

"""Pydantic models for ETL API."""
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ETLRunRequest(BaseModel):
    """Request to trigger an ETL run."""
    object_types: list[str] = Field(
        ...,
        description="HubSpot object types to sync",
        min_length=1,
    )
    force_full_load: bool = Field(
        default=False,
        description="Force full load (ignore incremental sync)",
    )
    log_level: str = Field(
        default="INFO",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
    )


class ETLRunResponse(BaseModel):
    """Response when ETL job is accepted (202)."""
    job_id: str
    status: JobStatus
    message: Optional[str] = None


class ObjectResult(BaseModel):
    """Result for a single object type ETL run."""
    object_type: str
    status: str
    duration_seconds: float = 0.0
    records_fetched: int = 0
    records_processed_ok: int = 0
    records_failed: int = 0
    db_upserts: int = 0
    records_deleted: int = 0
    sync_mode: Optional[str] = None


class ObjectError(BaseModel):
    """Error for a single object type."""
    object_type: str
    error: str
    detail: str


class JobStatusResponse(BaseModel):
    """Full status of an ETL job."""
    job_id: str
    status: JobStatus
    progress: Optional[str] = None
    object_types: list[str] = []
    results: list[ObjectResult] = []
    errors: list[ObjectError] = []
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class JobListResponse(BaseModel):
    """List of jobs."""
    jobs: list[JobStatusResponse]
    total: int


class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str
    message: str
    details: Optional[dict] = None

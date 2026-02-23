"""
FastAPI microservice: ETL HubSpot -> PostgreSQL.

Replaces the Lambda handler with HTTP endpoints + internal scheduler.
"""
import logging
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from api.v1 import etl_router
from service.scheduler import start_scheduler, shutdown_scheduler

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle: start/stop scheduler."""
    start_scheduler()
    yield
    shutdown_scheduler()


app = FastAPI(
    title="ETL HubSpot -> PostgreSQL",
    description="Microservice for HubSpot CRM data synchronization to PostgreSQL",
    version="2.0.0",
    lifespan=lifespan,
)

app.include_router(etl_router, prefix="/api/v1")


@app.get("/health")
def health():
    """Health check endpoint for load balancers and Kubernetes probes."""
    return {"status": "healthy", "service": "etl-hubspot-postgres"}

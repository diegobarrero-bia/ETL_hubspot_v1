"""
Internal scheduler for periodic ETL execution.

Uses APScheduler to trigger ETL jobs on a configurable cron schedule.
Disabled by default; enable via SCHEDULER_ENABLED=true environment variable.
"""
import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

scheduler: BackgroundScheduler | None = None


def _scheduled_etl_run():
    """Callback for scheduled ETL execution."""
    from models.etl import ETLRunRequest
    from service.job_manager import job_manager

    object_types_str = os.getenv("SCHEDULER_OBJECT_TYPES", "contacts")
    object_types = [t.strip() for t in object_types_str.split(",") if t.strip()]
    force_full_load = os.getenv("SCHEDULER_FORCE_FULL_LOAD", "false").lower() == "true"

    request = ETLRunRequest(
        object_types=object_types,
        force_full_load=force_full_load,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )

    try:
        job = job_manager.submit(request)
        logger.info("Scheduled ETL job submitted: %s", job.job_id)
    except RuntimeError as e:
        logger.warning("Scheduled ETL skipped (already running): %s", e)


def start_scheduler():
    """Start the APScheduler if SCHEDULER_ENABLED=true."""
    global scheduler

    if os.getenv("SCHEDULER_ENABLED", "false").lower() != "true":
        logger.info("Scheduler disabled (set SCHEDULER_ENABLED=true to enable)")
        return

    cron_expression = os.getenv("SCHEDULER_CRON", "0 */6 * * *")
    parts = cron_expression.split()

    trigger = CronTrigger(
        minute=parts[0] if len(parts) > 0 else "0",
        hour=parts[1] if len(parts) > 1 else "*",
        day=parts[2] if len(parts) > 2 else "*",
        month=parts[3] if len(parts) > 3 else "*",
        day_of_week=parts[4] if len(parts) > 4 else "*",
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _scheduled_etl_run,
        trigger,
        id="etl_periodic",
        replace_existing=True,
    )
    scheduler.start()

    logger.info("Scheduler started with cron: %s", cron_expression)


def shutdown_scheduler():
    """Gracefully shut down the scheduler."""
    global scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down.")

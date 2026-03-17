"""APScheduler job definitions for automated pipeline execution."""

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from config.settings import get_settings

from spotify_dw.db.session import SessionFactory
from spotify_dw.pipelines.analytics import AnalyticsPipeline
from spotify_dw.pipelines.ingestion import IngestionPipeline

logger = logging.getLogger(__name__)


def run_ingestion_job() -> None:
    """Scheduled job: run the Spotify API ingestion pipeline."""
    logger.info("Scheduled ingestion job starting")
    factory = SessionFactory()
    with factory.session() as session:
        pipeline = IngestionPipeline(session)
        result = pipeline.execute()
        logger.info(
            "Ingestion job complete",
            extra={
                "status": result.status,
                "rows_extracted": result.rows_extracted,
                "rows_loaded": result.rows_loaded,
            },
        )


def run_analytics_job() -> None:
    """Scheduled job: run the analytics pipeline after ingestion."""
    logger.info("Scheduled analytics job starting")
    factory = SessionFactory()
    with factory.session() as session:
        pipeline = AnalyticsPipeline(session)
        result = pipeline.execute()
        logger.info(
            "Analytics job complete",
            extra={"status": result.status, "rows_loaded": result.rows_loaded},
        )


def run_full_cycle() -> None:
    """Run ingestion followed by analytics."""
    run_ingestion_job()
    run_analytics_job()


def create_scheduler() -> BlockingScheduler:
    """Create and configure the APScheduler with ingestion + analytics jobs."""
    settings = get_settings()
    scheduler = BlockingScheduler()

    scheduler.add_job(
        run_full_cycle,
        trigger="interval",
        hours=settings.scheduler_interval_hours,
        id="spotify_full_cycle",
        name="Spotify Ingestion + Analytics",
        max_instances=1,
        coalesce=True,
    )

    logger.info(f"Scheduler configured: full cycle every {settings.scheduler_interval_hours} hours")
    return scheduler

"""Abstract base class for pipelines and PipelineResult dataclass."""

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from spotify_dw.models.pipeline_run_log import PipelineRunLog


@dataclass
class PipelineResult:
    """Result of a pipeline run, logged to pipeline_run_log."""

    status: str = "success"  # success / failure / partial
    rows_extracted: int = 0
    rows_loaded: int = 0
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    endpoint_statuses: dict[str, str] = field(default_factory=dict)


class BasePipeline(ABC):
    """Base class for all ETL and analytics pipelines.

    Subclasses must implement run(). The base class provides lifecycle
    hooks (_setup, _teardown, _on_failure) and observability logging.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self) -> PipelineResult:
        """Execute the full pipeline and return the result."""

    def execute(self) -> PipelineResult:
        """Run the pipeline with lifecycle hooks and observability logging."""
        start = time.time()
        try:
            self._setup()
            result = self.run()
            result.duration_seconds = time.time() - start
            self._log_run(result)
            return result
        except Exception as e:
            duration = time.time() - start
            result = PipelineResult(
                status="failure",
                errors=[str(e)],
                duration_seconds=duration,
            )
            self._on_failure(e)
            self._log_run(result)
            raise
        finally:
            self._teardown()

    def _setup(self) -> None:
        """Pre-run initialization. Override in subclasses if needed."""
        self.logger.info("Pipeline starting", extra={"pipeline": self.__class__.__name__})

    def _teardown(self) -> None:
        """Post-run cleanup. Override in subclasses if needed."""
        self.logger.info("Pipeline finished", extra={"pipeline": self.__class__.__name__})

    def _on_failure(self, error: Exception) -> None:
        """Error handling hook. Override in subclasses for custom behavior."""
        self.logger.error(
            "Pipeline failed",
            extra={"pipeline": self.__class__.__name__, "error": str(error)},
        )

    def _log_run(self, result: PipelineResult) -> None:
        """Write the pipeline result to the pipeline_run_log table."""
        try:
            log_entry = PipelineRunLog(
                pipeline_name=self.__class__.__name__,
                status=result.status,
                rows_extracted=result.rows_extracted,
                rows_loaded=result.rows_loaded,
                errors_json=json.dumps(result.errors) if result.errors else None,
                duration_seconds=result.duration_seconds,
                endpoint_statuses_json=json.dumps(result.endpoint_statuses) if result.endpoint_statuses else None,
                started_at=result.timestamp,
                completed_at=datetime.now(timezone.utc),
            )
            self.session.add(log_entry)
            self.session.flush()
        except Exception as e:
            self.logger.warning(f"Failed to log pipeline run: {e}")

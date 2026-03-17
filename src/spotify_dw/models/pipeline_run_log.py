"""Observability table: Pipeline run history."""

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class PipelineRunLog(Base):
    __tablename__ = "pipeline_run_log"

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # success / failure / partial
    rows_extracted: Mapped[int] = mapped_column(Integer, default=0)
    rows_loaded: Mapped[int] = mapped_column(Integer, default=0)
    errors_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON array of error messages
    duration_seconds: Mapped[float] = mapped_column(Float, default=0.0)
    endpoint_statuses_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON: endpoint -> status
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def __repr__(self) -> str:
        return f"<PipelineRunLog(id={self.run_id}, pipeline='{self.pipeline_name}', status='{self.status}')>"

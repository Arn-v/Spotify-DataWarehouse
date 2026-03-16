"""Fact table: Track Popularity snapshots (append-only time series)."""

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class FactTrackPopularity(Base):
    __tablename__ = "fact_track_popularity"

    popularity_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    track_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_track.track_key"), nullable=False, index=True)
    date_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_date.date_key"), nullable=False, index=True)
    popularity: Mapped[int] = mapped_column(Integer, nullable=False)  # 0-100
    source: Mapped[str] = mapped_column(String(10), nullable=False)  # 'api' or 'kaggle'

    def __repr__(self) -> str:
        return f"<FactTrackPopularity(track={self.track_key}, pop={self.popularity}, source='{self.source}')>"

"""Aggregate table: Trending tracks analysis results."""

from datetime import date

from sqlalchemy import Date, Float, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class AggTrendingTrack(Base):
    __tablename__ = "agg_trending_tracks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    track_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_track.track_key"), nullable=False, index=True)
    window_start: Mapped[date] = mapped_column(Date, nullable=False)
    window_end: Mapped[date] = mapped_column(Date, nullable=False)
    popularity_delta: Mapped[int] = mapped_column(Integer, nullable=False)
    velocity: Mapped[float] = mapped_column(Float, nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)

    def __repr__(self) -> str:
        return f"<AggTrendingTrack(track={self.track_key}, rank={self.rank}, delta={self.popularity_delta})>"

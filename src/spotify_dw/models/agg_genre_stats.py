"""Aggregate table: Genre statistics."""

from datetime import date

from sqlalchemy import Date, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class AggGenreStats(Base):
    __tablename__ = "agg_genre_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    genre_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_genre.genre_key"), nullable=False, index=True)
    period: Mapped[str] = mapped_column(String(10), nullable=False)  # 'weekly' or 'monthly'
    period_date: Mapped[date] = mapped_column(Date, nullable=False)
    track_count: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_popularity: Mapped[float] = mapped_column(Float, nullable=False)
    avg_danceability: Mapped[float] = mapped_column(Float, nullable=False)
    avg_energy: Mapped[float] = mapped_column(Float, nullable=False)
    avg_tempo: Mapped[float] = mapped_column(Float, nullable=False)

    def __repr__(self) -> str:
        return f"<AggGenreStats(genre={self.genre_key}, period='{self.period}', date={self.period_date})>"

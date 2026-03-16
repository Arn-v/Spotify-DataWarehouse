"""Aggregate table: Audio mood profile clusters."""

from datetime import date

from sqlalchemy import Date, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class AggAudioProfile(Base):
    __tablename__ = "agg_audio_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[int] = mapped_column(Integer, nullable=False)
    cluster_label: Mapped[str] = mapped_column(String(50), nullable=False)
    centroid_tempo: Mapped[float] = mapped_column(Float, nullable=False)
    centroid_energy: Mapped[float] = mapped_column(Float, nullable=False)
    centroid_valence: Mapped[float] = mapped_column(Float, nullable=False)
    centroid_danceability: Mapped[float] = mapped_column(Float, nullable=False)
    track_count: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)

    def __repr__(self) -> str:
        return f"<AggAudioProfile(cluster='{self.cluster_label}', tracks={self.track_count})>"

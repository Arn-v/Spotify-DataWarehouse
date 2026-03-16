"""Fact table: Audio Features per track (slowly changing)."""

from sqlalchemy import Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class FactAudioFeatures(Base):
    __tablename__ = "fact_audio_features"

    audio_feature_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    track_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_track.track_key"), nullable=False, index=True)

    # Core audio features (0.0 - 1.0 unless noted)
    danceability: Mapped[float | None] = mapped_column(Float, nullable=True)
    energy: Mapped[float | None] = mapped_column(Float, nullable=True)
    loudness: Mapped[float | None] = mapped_column(Float, nullable=True)  # dB, not 0-1
    speechiness: Mapped[float | None] = mapped_column(Float, nullable=True)
    acousticness: Mapped[float | None] = mapped_column(Float, nullable=True)
    instrumentalness: Mapped[float | None] = mapped_column(Float, nullable=True)
    liveness: Mapped[float | None] = mapped_column(Float, nullable=True)
    valence: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Tempo and rhythm
    tempo: Mapped[float | None] = mapped_column(Float, nullable=True)  # BPM
    tempo_category: Mapped[str | None] = mapped_column(String(10), nullable=True)  # slow/mid/fast
    time_signature: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Key and mode
    key: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 0-11 pitch class
    mode: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 0=minor, 1=major

    # Snapshot tracking
    snapshot_date_key: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("dim_date.date_key"), nullable=True
    )

    def __repr__(self) -> str:
        return f"<FactAudioFeatures(key={self.audio_feature_key}, track={self.track_key})>"

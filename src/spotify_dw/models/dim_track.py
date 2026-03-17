"""Dimension table: Tracks."""

from sqlalchemy import Boolean, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base, TimestampMixin


class DimTrack(TimestampMixin, Base):
    __tablename__ = "dim_track"

    track_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    spotify_track_id: Mapped[str] = mapped_column(String(62), unique=True, nullable=False, index=True)
    track_name: Mapped[str] = mapped_column(String(1000), nullable=False)
    album_key: Mapped[int | None] = mapped_column(Integer, ForeignKey("dim_album.album_key"), nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    explicit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    isrc: Mapped[str | None] = mapped_column(String(20), nullable=True)

    def __repr__(self) -> str:
        return f"<DimTrack(track_key={self.track_key}, name='{self.track_name}')>"

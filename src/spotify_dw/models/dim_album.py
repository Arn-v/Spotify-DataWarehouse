"""Dimension table: Albums."""

from datetime import date

from sqlalchemy import Date, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base, TimestampMixin


class DimAlbum(TimestampMixin, Base):
    __tablename__ = "dim_album"

    album_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    spotify_album_id: Mapped[str] = mapped_column(String(62), unique=True, nullable=False, index=True)
    album_name: Mapped[str] = mapped_column(String(1000), nullable=False)
    album_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    release_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    total_tracks: Mapped[int | None] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        return f"<DimAlbum(album_key={self.album_key}, name='{self.album_name}')>"

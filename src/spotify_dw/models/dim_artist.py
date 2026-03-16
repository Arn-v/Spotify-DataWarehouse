"""Dimension table: Artists."""

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base, TimestampMixin


class DimArtist(TimestampMixin, Base):
    __tablename__ = "dim_artist"

    artist_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    spotify_artist_id: Mapped[str] = mapped_column(String(62), unique=True, nullable=False, index=True)
    artist_name: Mapped[str] = mapped_column(String(500), nullable=False)
    popularity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    followers: Mapped[int | None] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        return f"<DimArtist(artist_key={self.artist_key}, name='{self.artist_name}')>"

"""Bridge table: Artist ↔ Genre (many-to-many)."""

from sqlalchemy import ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class BridgeArtistGenre(Base):
    __tablename__ = "bridge_artist_genre"

    artist_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_artist.artist_key"), primary_key=True
    )
    genre_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_genre.genre_key"), primary_key=True
    )

    def __repr__(self) -> str:
        return f"<BridgeArtistGenre(artist={self.artist_key}, genre={self.genre_key})>"

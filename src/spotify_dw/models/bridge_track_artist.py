"""Bridge table: Track ↔ Artist (many-to-many)."""

from sqlalchemy import Boolean, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class BridgeTrackArtist(Base):
    __tablename__ = "bridge_track_artist"

    track_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_track.track_key"), primary_key=True)
    artist_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_artist.artist_key"), primary_key=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False)

    def __repr__(self) -> str:
        return f"<BridgeTrackArtist(track={self.track_key}, artist={self.artist_key})>"

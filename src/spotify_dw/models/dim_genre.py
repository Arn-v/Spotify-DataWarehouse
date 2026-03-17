"""Dimension table: Genres."""

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base, TimestampMixin


class DimGenre(TimestampMixin, Base):
    __tablename__ = "dim_genre"

    genre_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    genre_name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)

    def __repr__(self) -> str:
        return f"<DimGenre(genre_key={self.genre_key}, name='{self.genre_name}')>"

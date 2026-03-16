"""Dimension table: Date (pre-populated calendar)."""

from datetime import date

from sqlalchemy import Boolean, Date, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from spotify_dw.models.base import Base


class DimDate(Base):
    __tablename__ = "dim_date"

    date_key: Mapped[int] = mapped_column(Integer, primary_key=True)  # YYYYMMDD
    full_date: Mapped[date] = mapped_column(Date, unique=True, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    quarter: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    month_name: Mapped[str] = mapped_column(String(10), nullable=False)
    week_of_year: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)
    is_weekend: Mapped[bool] = mapped_column(Boolean, nullable=False)

    def __repr__(self) -> str:
        return f"<DimDate(date_key={self.date_key}, date='{self.full_date}')>"

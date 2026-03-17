"""Reusable sidebar filter components."""

from datetime import date, timedelta

import streamlit as st


def date_range_filter(default_days: int = 30) -> tuple[date, date]:
    """Sidebar date range picker."""
    st.sidebar.subheader("Date Range")
    end_date = st.sidebar.date_input("End date", value=date.today())
    start_date = st.sidebar.date_input("Start date", value=end_date - timedelta(days=default_days))
    return start_date, end_date


def popularity_range_filter() -> tuple[int, int]:
    """Sidebar popularity range slider."""
    st.sidebar.subheader("Popularity")
    return st.sidebar.slider("Range", 0, 100, (0, 100))


def genre_multi_select(genres: list[str]) -> list[str]:
    """Sidebar genre multi-select."""
    st.sidebar.subheader("Genres")
    return st.sidebar.multiselect("Select genres", genres, default=genres[:5] if len(genres) > 5 else genres)

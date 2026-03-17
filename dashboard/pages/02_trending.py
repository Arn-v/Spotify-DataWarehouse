"""Trending page — popularity trends and top movers."""

import pandas as pd
import streamlit as st
from dashboard.components.charts import popularity_line_chart, trending_bar_chart
from sqlalchemy import text

from spotify_dw.db.session import SessionFactory


@st.cache_data(ttl=300)
def _load_trending() -> pd.DataFrame:
    """Load trending tracks data with caching."""
    factory = SessionFactory()
    with factory.session() as session:
        return pd.read_sql(
            text("""
                SELECT att.rank, att.popularity_delta, att.velocity,
                       att.window_start, att.window_end,
                       dt.track_name, dt.spotify_track_id
                FROM agg_trending_tracks att
                JOIN dim_track dt ON att.track_key = dt.track_key
                ORDER BY att.rank
                LIMIT 50
            """),
            session.get_bind(),
        )


@st.cache_data(ttl=300)
def _load_track_list() -> pd.DataFrame:
    """Load track list for selection with caching."""
    factory = SessionFactory()
    with factory.session() as session:
        return pd.read_sql(
            text("SELECT spotify_track_id, track_name FROM dim_track LIMIT 100"),
            session.get_bind(),
        )


@st.cache_data(ttl=300)
def _load_popularity_history(track_ids: tuple[str, ...]) -> pd.DataFrame:
    """Load popularity history using parameterized query."""
    factory = SessionFactory()
    with factory.session() as session:
        query = text("""
            SELECT dt.track_name, dd.full_date as date, ftp.popularity
            FROM fact_track_popularity ftp
            JOIN dim_track dt ON ftp.track_key = dt.track_key
            JOIN dim_date dd ON ftp.date_key = dd.date_key
            WHERE dt.spotify_track_id = ANY(:ids)
            ORDER BY dd.full_date
        """)
        return pd.read_sql(query, session.get_bind(), params={"ids": list(track_ids)})


st.header("Trending Tracks")

try:
    trending_df = _load_trending()

    if trending_df.empty:
        st.info("No trending data available. Run the analytics pipeline first.")
    else:
        # Top risers
        st.subheader("Top Risers")
        risers = trending_df[trending_df["popularity_delta"] > 0].head(20)
        if not risers.empty:
            fig = trending_bar_chart(risers, "Fastest Rising Tracks")
            st.plotly_chart(fig, use_container_width=True)

        # Top fallers
        st.subheader("Top Fallers")
        fallers = trending_df[trending_df["popularity_delta"] < 0].tail(20)
        if not fallers.empty:
            fig = trending_bar_chart(fallers, "Biggest Drops")
            st.plotly_chart(fig, use_container_width=True)

        # Full trending table
        st.subheader("Full Trending Table")
        st.dataframe(trending_df, use_container_width=True)

    # Popularity time series for individual tracks
    st.markdown("---")
    st.subheader("Track Popularity Over Time")

    tracks = _load_track_list()

    if not tracks.empty:
        selected = st.multiselect(
            "Select tracks to compare",
            tracks["track_name"].tolist(),
            default=tracks["track_name"].head(3).tolist(),
        )

        if selected:
            selected_ids = tracks[tracks["track_name"].isin(selected)]["spotify_track_id"].tolist()
            pop_df = _load_popularity_history(tuple(selected_ids))

            if not pop_df.empty:
                fig = popularity_line_chart(pop_df)
                st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error loading trending data: {e}")

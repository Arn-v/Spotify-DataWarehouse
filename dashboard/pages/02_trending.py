"""Trending page — popularity trends and top movers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import streamlit as st
from sqlalchemy import text

from dashboard.components.charts import popularity_line_chart, trending_bar_chart
from spotify_dw.db.session import SessionFactory

st.header("Trending Tracks")

try:
    factory = SessionFactory()
    engine = factory.engine

    with engine.connect() as conn:
        # Get trending data with track names
        trending_df = pd.read_sql(
            text("""
                SELECT att.rank, att.popularity_delta, att.velocity,
                       att.window_start, att.window_end,
                       dt.track_name, dt.spotify_track_id
                FROM agg_trending_tracks att
                JOIN dim_track dt ON att.track_key = dt.track_key
                ORDER BY att.rank
                LIMIT 50
            """),
            conn,
        )

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

    with engine.connect() as conn:
        tracks = pd.read_sql(
            text("SELECT spotify_track_id, track_name FROM dim_track LIMIT 100"),
            conn,
        )

    if not tracks.empty:
        selected = st.multiselect(
            "Select tracks to compare",
            tracks["track_name"].tolist(),
            default=tracks["track_name"].head(3).tolist(),
        )

        if selected:
            selected_ids = tracks[tracks["track_name"].isin(selected)]["spotify_track_id"].tolist()
            placeholders = ",".join(f"'{sid}'" for sid in selected_ids)

            with engine.connect() as conn:
                pop_df = pd.read_sql(
                    text(f"""
                        SELECT dt.track_name, dd.full_date as date, ftp.popularity
                        FROM fact_track_popularity ftp
                        JOIN dim_track dt ON ftp.track_key = dt.track_key
                        JOIN dim_date dd ON ftp.date_key = dd.date_key
                        WHERE dt.spotify_track_id IN ({placeholders})
                        ORDER BY dd.full_date
                    """),
                    conn,
                )

            if not pop_df.empty:
                fig = popularity_line_chart(pop_df)
                st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error loading trending data: {e}")

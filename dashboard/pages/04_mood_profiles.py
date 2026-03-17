"""Mood Profiles page — audio feature clusters and radar charts."""

import pandas as pd
import streamlit as st
from dashboard.components.charts import cluster_scatter, radar_chart
from sqlalchemy import text

from spotify_dw.db.session import SessionFactory


@st.cache_data(ttl=300)
def _load_profiles() -> pd.DataFrame:
    """Load audio profiles with caching."""
    factory = SessionFactory()
    with factory.session() as session:
        return pd.read_sql(
            text("""
                SELECT cluster_id, cluster_label, centroid_tempo, centroid_energy,
                       centroid_valence, centroid_danceability, track_count, snapshot_date
                FROM agg_audio_profiles
                ORDER BY track_count DESC
            """),
            session.get_bind(),
        )


st.header("Mood Profiles")
st.markdown("Audio feature clusters reveal marketing-relevant mood segments.")

try:
    profiles = _load_profiles()

    if profiles.empty:
        st.info("No mood profiles available. Run the analytics pipeline first.")
    else:
        # Cluster overview scatter
        st.subheader("Cluster Overview")
        fig = cluster_scatter(profiles)
        st.plotly_chart(fig, use_container_width=True)

        # Individual cluster radar charts
        st.subheader("Cluster Profiles")
        cols = st.columns(min(len(profiles), 3))

        for i, (_, row) in enumerate(profiles.iterrows()):
            col_idx = i % len(cols)
            with cols[col_idx]:
                st.markdown(f"**{row['cluster_label']}**")
                st.markdown(f"_{row['track_count']:,} tracks_")
                fig = radar_chart(row, title=row["cluster_label"])
                st.plotly_chart(fig, use_container_width=True)

        # Detail table
        st.subheader("Cluster Details")
        display_df = profiles[
            [
                "cluster_label",
                "track_count",
                "centroid_danceability",
                "centroid_energy",
                "centroid_valence",
                "centroid_tempo",
            ]
        ].copy()
        display_df.columns = ["Mood Segment", "Tracks", "Danceability", "Energy", "Valence", "Tempo (BPM)"]
        st.dataframe(display_df, use_container_width=True)

except Exception as e:
    st.error(f"Error loading mood profiles: {e}")

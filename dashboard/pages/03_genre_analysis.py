"""Genre Analysis page — market share, stats, and comparisons."""

import pandas as pd
import streamlit as st
from dashboard.components.charts import genre_bar_chart, genre_treemap
from dashboard.components.filters import genre_multi_select
from sqlalchemy import text

from spotify_dw.db.session import SessionFactory


@st.cache_data(ttl=300)
def _load_genre_stats() -> pd.DataFrame:
    """Load genre statistics with caching."""
    factory = SessionFactory()
    with factory.session() as session:
        return pd.read_sql(
            text("""
                SELECT ags.*, dg.genre_name
                FROM agg_genre_stats ags
                JOIN dim_genre dg ON ags.genre_key = dg.genre_key
                ORDER BY ags.track_count DESC
            """),
            session.get_bind(),
        )


st.header("Genre Analysis")

try:
    genre_stats = _load_genre_stats()

    if genre_stats.empty:
        st.info("No genre data available. Run the analytics pipeline first.")
    else:
        # Genre filter
        all_genres = genre_stats["genre_name"].unique().tolist()
        selected_genres = genre_multi_select(all_genres)

        if selected_genres:
            filtered = genre_stats[genre_stats["genre_name"].isin(selected_genres)]
        else:
            filtered = genre_stats

        # Market share treemap
        st.subheader("Genre Market Share")
        fig = genre_treemap(filtered)
        st.plotly_chart(fig, use_container_width=True)

        # Comparison metrics
        st.subheader("Genre Comparison")
        metric = st.selectbox(
            "Compare by",
            ["avg_popularity", "avg_danceability", "avg_energy", "avg_tempo", "track_count"],
        )
        fig = genre_bar_chart(filtered, metric)
        st.plotly_chart(fig, use_container_width=True)

        # Detail table
        st.subheader("Genre Statistics")
        st.dataframe(
            filtered[["genre_name", "track_count", "avg_popularity", "avg_danceability", "avg_energy", "avg_tempo"]],
            use_container_width=True,
        )

except Exception as e:
    st.error(f"Error loading genre data: {e}")

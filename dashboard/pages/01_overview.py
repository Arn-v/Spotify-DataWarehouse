"""Overview page — KPIs and pipeline health."""

import pandas as pd
import streamlit as st
from sqlalchemy import text

from spotify_dw.db.session import SessionFactory


@st.cache_data(ttl=300)
def _load_kpis() -> dict[str, int]:
    """Load KPI counts with caching."""
    factory = SessionFactory()
    with factory.session() as session:
        conn = session.get_bind()
        with conn.connect() as raw_conn:
            total_tracks = raw_conn.execute(text("SELECT COUNT(*) FROM dim_track")).scalar() or 0
            total_artists = raw_conn.execute(text("SELECT COUNT(*) FROM dim_artist")).scalar() or 0
            total_genres = raw_conn.execute(text("SELECT COUNT(*) FROM dim_genre")).scalar() or 0
            total_albums = raw_conn.execute(text("SELECT COUNT(*) FROM dim_album")).scalar() or 0
    return {
        "tracks": total_tracks,
        "artists": total_artists,
        "genres": total_genres,
        "albums": total_albums,
    }


@st.cache_data(ttl=60)
def _load_pipeline_runs() -> pd.DataFrame:
    """Load recent pipeline runs with caching."""
    factory = SessionFactory()
    with factory.session() as session:
        return pd.read_sql(
            text("""
                SELECT pipeline_name, status, rows_extracted, rows_loaded,
                       duration_seconds, started_at, completed_at
                FROM pipeline_run_log
                ORDER BY started_at DESC
                LIMIT 20
            """),
            session.get_bind(),
        )


st.header("Overview")

try:
    kpis = _load_kpis()

    # Display KPIs
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tracks", f"{kpis['tracks']:,}")
    col2.metric("Total Artists", f"{kpis['artists']:,}")
    col3.metric("Total Genres", f"{kpis['genres']:,}")
    col4.metric("Total Albums", f"{kpis['albums']:,}")

    st.markdown("---")

    # Pipeline run history
    st.subheader("Pipeline Run History")
    runs_df = _load_pipeline_runs()

    if not runs_df.empty:
        st.dataframe(runs_df, use_container_width=True)
    else:
        st.info("No pipeline runs recorded yet. Run a pipeline to see history here.")

except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.info(
        "Make sure PostgreSQL is running (`docker-compose up -d`) and migrations are applied (`alembic upgrade head`)."
    )

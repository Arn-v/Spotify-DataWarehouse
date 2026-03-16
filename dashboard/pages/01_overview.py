"""Overview page — KPIs and pipeline health."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
import streamlit as st
from sqlalchemy import text

from config.settings import get_settings
from spotify_dw.db.session import SessionFactory


st.header("Overview")

try:
    factory = SessionFactory()
    engine = factory.engine

    # KPI queries
    with engine.connect() as conn:
        total_tracks = conn.execute(text("SELECT COUNT(*) FROM dim_track")).scalar() or 0
        total_artists = conn.execute(text("SELECT COUNT(*) FROM dim_artist")).scalar() or 0
        total_genres = conn.execute(text("SELECT COUNT(*) FROM dim_genre")).scalar() or 0
        total_albums = conn.execute(text("SELECT COUNT(*) FROM dim_album")).scalar() or 0

    # Display KPIs
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tracks", f"{total_tracks:,}")
    col2.metric("Total Artists", f"{total_artists:,}")
    col3.metric("Total Genres", f"{total_genres:,}")
    col4.metric("Total Albums", f"{total_albums:,}")

    st.markdown("---")

    # Pipeline run history
    st.subheader("Pipeline Run History")
    with engine.connect() as conn:
        runs_df = pd.read_sql(
            text("""
                SELECT pipeline_name, status, rows_extracted, rows_loaded,
                       duration_seconds, started_at, completed_at
                FROM pipeline_run_log
                ORDER BY started_at DESC
                LIMIT 20
            """),
            conn,
        )

    if not runs_df.empty:
        st.dataframe(runs_df, use_container_width=True)
    else:
        st.info("No pipeline runs recorded yet. Run a pipeline to see history here.")

except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.info("Make sure PostgreSQL is running (`docker-compose up -d`) and migrations are applied (`alembic upgrade head`).")

"""Streamlit dashboard entry point — Spotify Data Warehouse Analytics."""

import sys
from pathlib import Path

# Add project root for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

st.set_page_config(
    page_title="Spotify Data Warehouse",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply dark theme
st.markdown("""
<style>
    .stMetric .metric-container { background-color: #1a1a2e; border-radius: 10px; padding: 15px; }
    .block-container { padding-top: 2rem; }
    h1 { color: #1DB954; }
</style>
""", unsafe_allow_html=True)

st.title("Spotify Data Warehouse")
st.markdown("**Marketing Analytics Dashboard** — Trending tracks, genre insights, and mood profiles.")
st.markdown("---")
st.markdown("Use the sidebar to navigate between pages.")
st.markdown("""
### Pages
- **Overview** — KPI cards, pipeline health
- **Trending** — Popularity trends, top risers/fallers
- **Genre Analysis** — Genre market share, stats
- **Mood Profiles** — Audio feature clusters
""")

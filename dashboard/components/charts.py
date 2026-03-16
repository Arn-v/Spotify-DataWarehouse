"""Reusable Plotly chart builders for the dashboard."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dashboard.components.theme import PLOTLY_COLORS, PLOTLY_LAYOUT


def popularity_line_chart(df: pd.DataFrame, title: str = "Popularity Over Time") -> go.Figure:
    """Line chart showing track popularity trends over time."""
    fig = px.line(
        df,
        x="date",
        y="popularity",
        color="track_name",
        title=title,
        labels={"popularity": "Popularity", "date": "Date"},
    )
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def trending_bar_chart(df: pd.DataFrame, title: str = "Top Trending Tracks") -> go.Figure:
    """Horizontal bar chart of trending tracks by velocity."""
    fig = px.bar(
        df.head(20),
        x="velocity",
        y="track_name",
        orientation="h",
        title=title,
        color="popularity_delta",
        color_continuous_scale=["#E8115B", "#1DB954"],
        labels={"velocity": "Velocity (pop/day)", "track_name": "Track"},
    )
    fig.update_layout(**PLOTLY_LAYOUT, yaxis={"categoryorder": "total ascending"})
    return fig


def genre_treemap(df: pd.DataFrame, title: str = "Genre Market Share") -> go.Figure:
    """Treemap showing genre distribution by track count."""
    fig = px.treemap(
        df,
        path=["genre_name"],
        values="track_count",
        color="avg_popularity",
        color_continuous_scale="Greens",
        title=title,
    )
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def genre_bar_chart(df: pd.DataFrame, metric: str = "avg_popularity") -> go.Figure:
    """Bar chart comparing genres by a metric."""
    fig = px.bar(
        df.sort_values(metric, ascending=False).head(15),
        x="genre_name",
        y=metric,
        title=f"Top Genres by {metric.replace('_', ' ').title()}",
        color=metric,
        color_continuous_scale="Greens",
    )
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def cluster_scatter(df: pd.DataFrame) -> go.Figure:
    """Scatter plot of audio feature clusters."""
    fig = px.scatter(
        df,
        x="centroid_energy",
        y="centroid_danceability",
        size="track_count",
        color="cluster_label",
        title="Audio Mood Clusters",
        hover_data=["centroid_tempo", "centroid_valence", "track_count"],
        labels={
            "centroid_energy": "Energy",
            "centroid_danceability": "Danceability",
        },
    )
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def radar_chart(row: pd.Series, title: str = "Cluster Profile") -> go.Figure:
    """Radar chart for a single cluster's centroid values."""
    categories = ["Danceability", "Energy", "Valence", "Tempo (norm)"]
    # Normalize tempo to 0-1 scale for radar (divide by 200 as rough max)
    values = [
        row["centroid_danceability"],
        row["centroid_energy"],
        row["centroid_valence"],
        min(row["centroid_tempo"] / 200, 1.0),
    ]
    values.append(values[0])  # Close the radar

    fig = go.Figure(
        go.Scatterpolar(
            r=values,
            theta=categories + [categories[0]],
            fill="toself",
            fillcolor="rgba(29, 185, 84, 0.3)",
            line_color=PLOTLY_COLORS[0],
        )
    )
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=title,
        polar={"bgcolor": "rgba(0,0,0,0)"},
        showlegend=False,
    )
    return fig


def kpi_metric(label: str, value, delta=None) -> dict:
    """Helper to create KPI card data."""
    return {"label": label, "value": value, "delta": delta}

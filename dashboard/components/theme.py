"""Streamlit theme and styling constants."""

# Spotify brand colors
SPOTIFY_GREEN = "#1DB954"
SPOTIFY_BLACK = "#191414"
SPOTIFY_DARK_GRAY = "#1a1a2e"
SPOTIFY_LIGHT_GRAY = "#b3b3b3"
SPOTIFY_WHITE = "#FFFFFF"

# Plotly color scale for charts
PLOTLY_COLORS = [
    "#1DB954",  # Spotify green
    "#1ED760",  # Lighter green
    "#535353",  # Gray
    "#B49BC8",  # Purple
    "#E8115B",  # Red/pink
    "#F573A0",  # Light pink
    "#FFC864",  # Gold
    "#509BF5",  # Blue
]

# Plotly layout template for dark theme
PLOTLY_LAYOUT = {
    "template": "plotly_dark",
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {"color": SPOTIFY_WHITE, "family": "Arial"},
    "colorway": PLOTLY_COLORS,
}

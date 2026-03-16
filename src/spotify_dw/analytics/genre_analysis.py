"""Genre analyzer — market share, stats, and emerging genres."""

import pandas as pd

from spotify_dw.analytics.base import BaseAnalyzer


class GenreAnalyzer(BaseAnalyzer):
    """Analyzes genre distribution, popularity, and audio characteristics.

    Computes:
        - Genre market share (track count per genre)
        - Average popularity, danceability, energy, tempo per genre
        - Genre ranking by track count and popularity
    """

    REQUIRED_COLUMNS = ["genre_key", "track_count"]

    def analyze(self, df: pd.DataFrame, period: str = "monthly") -> pd.DataFrame:
        """Compute genre statistics.

        Args:
            df: DataFrame with genre_key and audio feature aggregates.
                Expected columns: genre_key, track_count, avg_popularity,
                avg_danceability, avg_energy, avg_tempo.
            period: 'weekly' or 'monthly' label for the output.

        Returns:
            DataFrame matching agg_genre_stats schema.
        """
        if not self._validate_input(df, ["genre_key"]):
            return pd.DataFrame()

        df = df.copy()

        # If raw track-level data is passed, aggregate it
        if "popularity" in df.columns and "track_count" not in df.columns:
            df = self._aggregate_from_tracks(df)

        # Add period info
        df["period"] = period
        df["period_date"] = pd.Timestamp.now().date()

        # Ensure all expected columns exist with defaults
        for col in ["avg_popularity", "avg_danceability", "avg_energy", "avg_tempo"]:
            if col not in df.columns:
                df[col] = 0.0
        if "track_count" not in df.columns:
            df["track_count"] = 0

        result = df[["genre_key", "period", "period_date", "track_count",
                      "avg_popularity", "avg_danceability", "avg_energy", "avg_tempo"]].copy()

        self.logger.info(f"Genre analysis: {len(result)} genres analyzed")
        return result

    def _aggregate_from_tracks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate track-level data into genre-level statistics."""
        agg = df.groupby("genre_key").agg(
            track_count=("genre_key", "count"),
            avg_popularity=("popularity", "mean"),
            avg_danceability=("danceability", "mean") if "danceability" in df.columns else ("genre_key", "count"),
            avg_energy=("energy", "mean") if "energy" in df.columns else ("genre_key", "count"),
            avg_tempo=("tempo", "mean") if "tempo" in df.columns else ("genre_key", "count"),
        ).reset_index()

        return agg

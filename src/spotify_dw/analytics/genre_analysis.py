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

    REQUIRED_COLUMNS = ["genre_key"]

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

        result = df[
            [
                "genre_key",
                "period",
                "period_date",
                "track_count",
                "avg_popularity",
                "avg_danceability",
                "avg_energy",
                "avg_tempo",
            ]
        ].copy()

        self.logger.info(f"Genre analysis: {len(result)} genres analyzed")
        return result

    def _aggregate_from_tracks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate track-level data into genre-level statistics."""
        agg_dict: dict[str, tuple[str, str]] = {
            "track_count": ("genre_key", "count"),
            "avg_popularity": ("popularity", "mean"),
        }
        for col in ("danceability", "energy", "tempo"):
            if col in df.columns:
                agg_dict[f"avg_{col}"] = (col, "mean")

        agg = df.groupby("genre_key").agg(**agg_dict).reset_index()

        # Fill missing metric columns with 0.0
        for col in ("avg_danceability", "avg_energy", "avg_tempo"):
            if col not in agg.columns:
                agg[col] = 0.0

        return agg

"""Trending tracks analyzer — popularity velocity and ranking."""

import pandas as pd

from spotify_dw.analytics.base import BaseAnalyzer


class TrendingAnalyzer(BaseAnalyzer):
    """Analyzes track popularity changes over time windows.

    Computes:
        - popularity_delta: change in popularity between window start and end
        - velocity: rate of popularity change per day
        - rank: position among all tracks (by velocity, descending)
    """

    REQUIRED_COLUMNS = ["track_key", "date_key", "popularity"]

    def analyze(self, df: pd.DataFrame, window_days: int = 7) -> pd.DataFrame:
        """Compute trending metrics over a rolling window.

        Args:
            df: DataFrame with track_key, date_key, popularity (from fact_track_popularity).
            window_days: Size of the trending window in days.

        Returns:
            DataFrame with: track_key, window_start, window_end, popularity_delta, velocity, rank.
        """
        if not self._validate_input(df, self.REQUIRED_COLUMNS):
            return pd.DataFrame()

        df = df.copy()
        df["date"] = pd.to_datetime(df["date_key"].astype(str), format="%Y%m%d")

        # Get the latest and earliest popularity per track within the window
        max_date = df["date"].max()
        window_start = max_date - pd.Timedelta(days=window_days)
        window_df = df[df["date"] >= window_start]

        if window_df.empty:
            self.logger.warning("No data within the trending window")
            return pd.DataFrame()

        # Get first and last popularity per track in the window
        earliest = window_df.sort_values("date").groupby("track_key").first().reset_index()
        latest = window_df.sort_values("date").groupby("track_key").last().reset_index()

        merged = earliest[["track_key", "popularity", "date"]].merge(
            latest[["track_key", "popularity", "date"]],
            on="track_key",
            suffixes=("_start", "_end"),
        )

        # Compute delta and velocity
        merged["popularity_delta"] = merged["popularity_end"] - merged["popularity_start"]
        merged["days"] = (merged["date_end"] - merged["date_start"]).dt.days.clip(lower=1)
        merged["velocity"] = merged["popularity_delta"] / merged["days"]

        # Rank by velocity (highest first)
        merged = merged.sort_values("velocity", ascending=False).reset_index(drop=True)
        merged["rank"] = range(1, len(merged) + 1)

        result = merged[["track_key", "popularity_delta", "velocity", "rank"]].copy()
        result["window_start"] = window_start.date()
        result["window_end"] = max_date.date()

        self.logger.info(f"Trending analysis: {len(result)} tracks ranked")
        return result

"""Transformer for track dimension data."""

import pandas as pd

from spotify_dw.transformers.base import BaseTransformer


class TrackTransformer(BaseTransformer):
    """Cleans and normalizes track data for the dim_track table.

    - Normalizes track names (lowercase, strip whitespace)
    - Converts duration_ms to duration_seconds
    - Deduplicates by spotify_track_id (keeps first occurrence)
    """

    REQUIRED_COLUMNS = ["spotify_track_id", "track_name", "duration_ms"]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self._validate_schema(df, self.REQUIRED_COLUMNS)
        df = df.copy()

        # Normalize track names
        df["track_name"] = df["track_name"].astype(str).str.strip().str.lower()

        # Convert duration_ms to seconds
        df["duration_seconds"] = df["duration_ms"] / 1000.0

        # Normalize explicit to boolean
        if "explicit" in df.columns:
            df["explicit"] = df["explicit"].astype(bool)

        # Deduplicate by Spotify track ID, keeping first occurrence
        before = len(df)
        df = df.drop_duplicates(subset=["spotify_track_id"], keep="first")
        dupes = before - len(df)
        if dupes > 0:
            self.logger.info(f"Removed {dupes} duplicate tracks")

        self.logger.info(f"Transformed {len(df)} tracks")
        return df

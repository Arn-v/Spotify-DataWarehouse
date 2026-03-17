"""Transformer for audio features fact data."""

import pandas as pd

from spotify_dw.transformers.base import BaseTransformer

# Audio features that should be in [0, 1] range
NORMALIZED_FEATURES = [
    "danceability",
    "energy",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
]

TEMPO_BINS = {"slow": (0, 90), "mid": (90, 140), "fast": (140, float("inf"))}


class AudioFeaturesTransformer(BaseTransformer):
    """Cleans and validates audio features for the fact_audio_features table.

    - Validates 0-1 range for normalized features (clamps outliers)
    - Bins tempo into categories (slow < 90, mid 90-140, fast > 140)
    - Ensures correct types for key, mode, time_signature
    """

    REQUIRED_COLUMNS = ["spotify_track_id"]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self._validate_schema(df, self.REQUIRED_COLUMNS)
        df = df.copy()

        # Clamp normalized features to [0, 1]
        for feature in NORMALIZED_FEATURES:
            if feature in df.columns:
                df[feature] = df[feature].clip(lower=0.0, upper=1.0)

        # Bin tempo into categories
        if "tempo" in df.columns:
            df["tempo_category"] = pd.cut(
                df["tempo"],
                bins=[0, 90, 140, float("inf")],
                labels=["slow", "mid", "fast"],
                right=False,
            ).astype(str)
            # Replace 'nan' strings with None
            df["tempo_category"] = df["tempo_category"].replace("nan", None)

        # Cast integer fields
        for col in ["key", "mode", "time_signature"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        self.logger.info(f"Transformed audio features for {len(df)} tracks")
        return df

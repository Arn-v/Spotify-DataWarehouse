"""Transformer for artist dimension data."""

import pandas as pd

from spotify_dw.transformers.base import BaseTransformer


class ArtistTransformer(BaseTransformer):
    """Cleans and normalizes artist data for dim_artist and bridge tables.

    - Explodes multi-artist tracks into individual artist rows
    - Normalizes artist names (strip whitespace, lowercase)
    - Extracts genre associations per artist
    - Deduplicates artists
    """

    REQUIRED_COLUMNS = ["spotify_track_id", "artists"]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self._validate_schema(df, self.REQUIRED_COLUMNS)
        df = df.copy()

        # Determine the separator used in the artists column
        # Kaggle uses ";", API might use ","
        sample = df["artists"].dropna().head(50)
        sep = ";" if sample.str.contains(";").any() else ","

        # Explode multi-artist entries into separate rows
        df["artist_list"] = df["artists"].str.split(sep)
        exploded = df.explode("artist_list")

        # Normalize artist names
        exploded["artist_name"] = exploded["artist_list"].astype(str).str.strip().str.lower()

        # Drop empty artist names
        exploded = exploded[exploded["artist_name"].str.len() > 0]

        # Mark primary artist (first in the original list)
        exploded["is_primary"] = exploded.groupby("spotify_track_id").cumcount() == 0

        # Build unique artists DataFrame
        unique_artists = (
            exploded[["artist_name"]]
            .drop_duplicates(subset=["artist_name"])
            .reset_index(drop=True)
        )

        # Build track-artist bridge data
        bridge_data = exploded[["spotify_track_id", "artist_name", "is_primary"]].copy()

        # Attach genre info if available
        if "genre" in df.columns:
            genre_data = (
                exploded[["artist_name", "genre"]]
                .dropna(subset=["genre"])
                .drop_duplicates()
            )
            genre_data["genre"] = genre_data["genre"].astype(str).str.strip().str.lower()
        else:
            genre_data = pd.DataFrame(columns=["artist_name", "genre"])

        self.logger.info(
            f"Extracted {len(unique_artists)} unique artists, "
            f"{len(bridge_data)} track-artist links, "
            f"{len(genre_data)} artist-genre links"
        )

        # Store bridge and genre data as attributes for the pipeline to access
        self.bridge_data = bridge_data
        self.genre_data = genre_data

        return unique_artists

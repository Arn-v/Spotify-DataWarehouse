"""Extractor for Kaggle Spotify Tracks CSV dataset."""

import logging
from pathlib import Path

import pandas as pd

from spotify_dw.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)

# Column mapping: Kaggle CSV column name -> internal name
KAGGLE_COLUMN_MAP = {
    "track_id": "spotify_track_id",
    "track_name": "track_name",
    "artists": "artists",
    "album_name": "album_name",
    "track_genre": "genre",
    "popularity": "popularity",
    "duration_ms": "duration_ms",
    "explicit": "explicit",
    "danceability": "danceability",
    "energy": "energy",
    "loudness": "loudness",
    "speechiness": "speechiness",
    "acousticness": "acousticness",
    "instrumentalness": "instrumentalness",
    "liveness": "liveness",
    "valence": "valence",
    "tempo": "tempo",
    "time_signature": "time_signature",
    "key": "key",
    "mode": "mode",
}

REQUIRED_COLUMNS = {"track_id", "track_name", "artists", "popularity", "duration_ms"}


class KaggleCSVExtractor(BaseExtractor):
    """Extracts track data from Kaggle Spotify Tracks CSV files.

    Args:
        file_path: Path to the CSV file.
    """

    def __init__(self, file_path: str | Path) -> None:
        super().__init__()
        self.file_path = Path(file_path)

    def validate_source(self) -> bool:
        """Check that the CSV file exists and has the required columns."""
        if not self.file_path.exists():
            self.logger.error(f"File not found: {self.file_path}")
            return False

        try:
            sample = pd.read_csv(self.file_path, nrows=1)
            missing = REQUIRED_COLUMNS - set(sample.columns)
            if missing:
                self.logger.error(f"Missing required columns: {missing}")
                return False
        except Exception as e:
            self.logger.error(f"Failed to read CSV: {e}")
            return False

        return True

    def extract(self) -> pd.DataFrame:
        """Read the full CSV, validate, rename columns, and return."""
        if not self.validate_source():
            raise ValueError(f"Invalid source: {self.file_path}")

        self.logger.info("Reading Kaggle CSV", extra={"file": str(self.file_path)})
        df = pd.read_csv(self.file_path)

        # Rename columns that exist in the mapping
        rename_map = {k: v for k, v in KAGGLE_COLUMN_MAP.items() if k in df.columns}
        df = df.rename(columns=rename_map)

        # Drop rows with no track ID
        df = df.dropna(subset=["spotify_track_id"])

        self._log_extraction(len(df))
        return df

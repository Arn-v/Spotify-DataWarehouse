"""Unit tests for extractors."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from spotify_dw.extractors.kaggle_csv import KaggleCSVExtractor


class TestKaggleCSVExtractor:
    """Tests for KaggleCSVExtractor."""

    def _write_csv(self, df: pd.DataFrame, tmpdir: Path) -> Path:
        """Helper to write a DataFrame to a temp CSV."""
        path = tmpdir / "test.csv"
        df.to_csv(path, index=False)
        return path

    def test_extract_returns_dataframe(self, sample_tracks_df, tmp_path):
        path = self._write_csv(sample_tracks_df, tmp_path)
        extractor = KaggleCSVExtractor(path)
        result = extractor.extract()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5

    def test_columns_renamed(self, sample_tracks_df, tmp_path):
        path = self._write_csv(sample_tracks_df, tmp_path)
        extractor = KaggleCSVExtractor(path)
        result = extractor.extract()
        # track_id should be renamed to spotify_track_id
        assert "spotify_track_id" in result.columns
        assert "track_id" not in result.columns

    def test_validate_source_missing_file(self):
        extractor = KaggleCSVExtractor("/nonexistent/file.csv")
        assert extractor.validate_source() is False

    def test_validate_source_missing_columns(self, tmp_path):
        bad_df = pd.DataFrame({"wrong_col": [1, 2]})
        path = self._write_csv(bad_df, tmp_path)
        extractor = KaggleCSVExtractor(path)
        assert extractor.validate_source() is False

    def test_extract_drops_null_track_ids(self, sample_tracks_df, tmp_path):
        df = sample_tracks_df.copy()
        df.loc[0, "track_id"] = None
        path = self._write_csv(df, tmp_path)
        extractor = KaggleCSVExtractor(path)
        result = extractor.extract()
        assert len(result) == 4

    def test_extract_raises_on_invalid_source(self):
        extractor = KaggleCSVExtractor("/nonexistent.csv")
        with pytest.raises(ValueError, match="Invalid source"):
            extractor.extract()

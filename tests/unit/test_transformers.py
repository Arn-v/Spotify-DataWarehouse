"""Unit tests for transformers."""

import pandas as pd
import pytest

from spotify_dw.transformers.artist_transformer import ArtistTransformer
from spotify_dw.transformers.audio_features_transformer import AudioFeaturesTransformer
from spotify_dw.transformers.deduplicator import Deduplicator
from spotify_dw.transformers.track_transformer import TrackTransformer


class TestTrackTransformer:

    def test_normalizes_names(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "track_name": ["  Hello World  "],
            "duration_ms": [210000],
        })
        result = TrackTransformer().transform(df)
        assert result.iloc[0]["track_name"] == "hello world"

    def test_converts_duration(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "track_name": ["test"],
            "duration_ms": [180000],
        })
        result = TrackTransformer().transform(df)
        assert result.iloc[0]["duration_seconds"] == 180.0

    def test_deduplicates_tracks(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1", "1", "2"],
            "track_name": ["a", "a", "b"],
            "duration_ms": [100, 100, 200],
        })
        result = TrackTransformer().transform(df)
        assert len(result) == 2

    def test_raises_on_missing_columns(self):
        df = pd.DataFrame({"wrong": [1]})
        with pytest.raises(ValueError, match="Missing columns"):
            TrackTransformer().transform(df)


class TestArtistTransformer:

    def test_explodes_multi_artists(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "artists": ["Artist A;Artist B"],
        })
        transformer = ArtistTransformer()
        result = transformer.transform(df)
        # Should have 2 unique artists
        assert len(result) == 2

    def test_normalizes_artist_names(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "artists": ["  The Beatles  "],
        })
        transformer = ArtistTransformer()
        result = transformer.transform(df)
        assert result.iloc[0]["artist_name"] == "the beatles"

    def test_marks_primary_artist(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "artists": ["Main;Feat"],
        })
        transformer = ArtistTransformer()
        transformer.transform(df)
        bridge = transformer.bridge_data
        primary = bridge[bridge["is_primary"]]
        assert len(primary) == 1

    def test_extracts_genre_data(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "artists": ["Artist A"],
            "genre": ["Pop"],
        })
        transformer = ArtistTransformer()
        transformer.transform(df)
        assert len(transformer.genre_data) == 1
        assert transformer.genre_data.iloc[0]["genre"] == "pop"


class TestAudioFeaturesTransformer:

    def test_clamps_values(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "danceability": [1.5],
            "energy": [-0.3],
        })
        result = AudioFeaturesTransformer().transform(df)
        assert result.iloc[0]["danceability"] == 1.0
        assert result.iloc[0]["energy"] == 0.0

    def test_bins_tempo(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1", "2", "3"],
            "tempo": [70.0, 120.0, 175.0],
        })
        result = AudioFeaturesTransformer().transform(df)
        assert result.iloc[0]["tempo_category"] == "slow"
        assert result.iloc[1]["tempo_category"] == "mid"
        assert result.iloc[2]["tempo_category"] == "fast"

    def test_casts_integer_fields(self):
        df = pd.DataFrame({
            "spotify_track_id": ["1"],
            "key": [5.0],
            "mode": [1.0],
            "time_signature": [4.0],
        })
        result = AudioFeaturesTransformer().transform(df)
        assert result.iloc[0]["key"] == 5
        assert result.iloc[0]["mode"] == 1


class TestDeduplicator:

    def test_removes_existing_ids(self):
        df = pd.DataFrame({"spotify_track_id": ["1", "2", "3"]})
        dedup = Deduplicator(existing_ids={"1", "2"})
        result = dedup.transform(df)
        assert len(result) == 1
        assert result.iloc[0]["spotify_track_id"] == "3"

    def test_removes_internal_duplicates(self):
        df = pd.DataFrame({"spotify_track_id": ["1", "1", "2"]})
        dedup = Deduplicator()
        result = dedup.transform(df)
        assert len(result) == 2

    def test_empty_existing_ids(self):
        df = pd.DataFrame({"spotify_track_id": ["1", "2"]})
        dedup = Deduplicator()
        result = dedup.transform(df)
        assert len(result) == 2

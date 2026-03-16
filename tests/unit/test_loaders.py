"""Unit tests for loaders — uses in-memory SQLite."""

import pandas as pd

from spotify_dw.models.dim_genre import DimGenre
from spotify_dw.models.dim_track import DimTrack
from spotify_dw.models.fact_track_popularity import FactTrackPopularity


class TestPostgresLoaderWithSQLite:
    """Test loader logic using SQLite (ORM-based simple load, not Postgres upsert)."""

    def test_load_dim_track(self, db_session):
        """Loading tracks via ORM bulk insert."""
        tracks = [
            DimTrack(spotify_track_id="1abc", track_name="song one", duration_seconds=210.0),
            DimTrack(spotify_track_id="2def", track_name="song two", duration_seconds=180.0),
        ]
        db_session.bulk_save_objects(tracks)
        db_session.flush()

        result = db_session.query(DimTrack).all()
        assert len(result) == 2
        assert result[0].track_name == "song one"

    def test_load_dim_genre(self, db_session):
        """Loading genres via ORM."""
        genres = [DimGenre(genre_name="pop"), DimGenre(genre_name="rock")]
        db_session.bulk_save_objects(genres)
        db_session.flush()

        result = db_session.query(DimGenre).all()
        assert len(result) == 2

    def test_load_fact_popularity(self, db_session):
        """Loading popularity facts via ORM."""
        # First create a track
        track = DimTrack(spotify_track_id="1abc", track_name="test")
        db_session.add(track)
        db_session.flush()

        pop = FactTrackPopularity(
            track_key=track.track_key,
            date_key=20260316,
            popularity=85,
            source="kaggle",
        )
        db_session.add(pop)
        db_session.flush()

        result = db_session.query(FactTrackPopularity).all()
        assert len(result) == 1
        assert result[0].popularity == 85
        assert result[0].source == "kaggle"

    def test_loader_base_pre_load_checks_empty_df(self):
        """BaseLoader._pre_load_checks should return False for empty DataFrame."""
        from spotify_dw.loaders.base import BaseLoader

        class DummyLoader(BaseLoader):
            def load(self, df):
                return 0

        loader = DummyLoader()
        assert loader._pre_load_checks(pd.DataFrame()) is False

    def test_loader_base_pre_load_checks_valid_df(self):
        """BaseLoader._pre_load_checks should return True for non-empty DataFrame."""
        from spotify_dw.loaders.base import BaseLoader

        class DummyLoader(BaseLoader):
            def load(self, df):
                return 0

        loader = DummyLoader()
        assert loader._pre_load_checks(pd.DataFrame({"a": [1]})) is True

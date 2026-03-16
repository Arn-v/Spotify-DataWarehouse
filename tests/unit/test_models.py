"""Smoke tests for ORM models — verifies all tables create correctly."""

from sqlalchemy import inspect

from spotify_dw.models.base import Base


class TestModelsCreation:
    """Verify that all ORM models create tables in the database."""

    def test_all_tables_created(self, in_memory_db):
        """All expected tables should exist after metadata.create_all."""
        inspector = inspect(in_memory_db)
        tables = set(inspector.get_table_names())

        expected_tables = {
            "dim_track",
            "dim_artist",
            "dim_album",
            "dim_genre",
            "dim_date",
            "bridge_track_artist",
            "bridge_artist_genre",
            "fact_audio_features",
            "fact_track_popularity",
            "agg_trending_tracks",
            "agg_genre_stats",
            "agg_audio_profiles",
            "pipeline_run_log",
        }

        assert expected_tables.issubset(tables), f"Missing tables: {expected_tables - tables}"

    def test_dim_track_columns(self, in_memory_db):
        """dim_track should have the expected columns."""
        inspector = inspect(in_memory_db)
        columns = {col["name"] for col in inspector.get_columns("dim_track")}

        assert "track_key" in columns
        assert "spotify_track_id" in columns
        assert "track_name" in columns
        assert "album_key" in columns
        assert "duration_seconds" in columns
        assert "explicit" in columns

    def test_fact_audio_features_columns(self, in_memory_db):
        """fact_audio_features should have all audio feature columns."""
        inspector = inspect(in_memory_db)
        columns = {col["name"] for col in inspector.get_columns("fact_audio_features")}

        audio_cols = {"danceability", "energy", "loudness", "speechiness", "acousticness",
                      "instrumentalness", "liveness", "valence", "tempo", "tempo_category"}
        assert audio_cols.issubset(columns), f"Missing: {audio_cols - columns}"

    def test_pipeline_run_log_columns(self, in_memory_db):
        """pipeline_run_log should have observability columns."""
        inspector = inspect(in_memory_db)
        columns = {col["name"] for col in inspector.get_columns("pipeline_run_log")}

        assert "pipeline_name" in columns
        assert "status" in columns
        assert "rows_extracted" in columns
        assert "rows_loaded" in columns
        assert "errors_json" in columns
        assert "duration_seconds" in columns

    def test_base_metadata_has_all_models(self):
        """Base.metadata should have all registered tables."""
        table_names = set(Base.metadata.tables.keys())
        assert len(table_names) >= 13

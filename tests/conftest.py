"""Shared test fixtures for unit and integration tests."""

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from spotify_dw.models.base import Base


@pytest.fixture
def in_memory_db():
    """Create an in-memory SQLite database with all tables."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(in_memory_db) -> Session:
    """Provide a transactional test session that rolls back after each test."""
    session_factory = sessionmaker(bind=in_memory_db)
    session = session_factory()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def sample_tracks_df() -> pd.DataFrame:
    """Sample DataFrame matching the Kaggle CSV schema."""
    return pd.DataFrame(
        {
            "track_id": ["1abc", "2def", "3ghi", "4jkl", "5mno"],
            "artists": ["Artist A", "Artist B;Artist C", "Artist A", "Artist D", "Artist B"],
            "album_name": ["Album 1", "Album 2", "Album 1", "Album 3", "Album 4"],
            "track_name": ["Song One", "Song Two", "Song Three", "Song Four", "Song Five"],
            "popularity": [85, 72, 60, 45, 90],
            "duration_ms": [210000, 180000, 240000, 195000, 300000],
            "explicit": [False, True, False, False, True],
            "danceability": [0.8, 0.6, 0.3, 0.5, 0.9],
            "energy": [0.7, 0.9, 0.2, 0.4, 0.95],
            "loudness": [-5.0, -3.0, -12.0, -8.0, -2.0],
            "speechiness": [0.05, 0.1, 0.03, 0.04, 0.08],
            "acousticness": [0.2, 0.05, 0.85, 0.6, 0.03],
            "instrumentalness": [0.0, 0.0, 0.7, 0.1, 0.0],
            "liveness": [0.1, 0.3, 0.05, 0.08, 0.15],
            "valence": [0.6, 0.8, 0.2, 0.3, 0.95],
            "tempo": [120.0, 150.0, 70.0, 95.0, 175.0],
            "time_signature": [4, 4, 3, 4, 4],
            "key": [5, 0, 7, 2, 11],
            "mode": [1, 1, 0, 0, 1],
            "track_genre": ["pop", "electronic", "classical", "jazz", "hip-hop"],
        }
    )


@pytest.fixture
def sample_api_response() -> dict:
    """Sample Spotify API response matching spotipy's format."""
    return {
        "tracks": {
            "items": [
                {
                    "id": "spotify_id_1",
                    "name": "API Track One",
                    "popularity": 78,
                    "duration_ms": 225000,
                    "explicit": False,
                    "external_ids": {"isrc": "US1234567890"},
                    "album": {
                        "id": "album_id_1",
                        "name": "API Album",
                        "album_type": "album",
                        "release_date": "2024-01-15",
                        "total_tracks": 12,
                    },
                    "artists": [
                        {"id": "artist_id_1", "name": "API Artist"},
                    ],
                }
            ]
        }
    }

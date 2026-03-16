"""Pipeline: Load Kaggle historical CSV data into the data warehouse."""

from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from spotify_dw.extractors.kaggle_csv import KaggleCSVExtractor
from spotify_dw.loaders.postgres_loader import PostgresLoader
from spotify_dw.models.dim_album import DimAlbum
from spotify_dw.models.dim_artist import DimArtist
from spotify_dw.models.dim_date import DimDate
from spotify_dw.models.dim_genre import DimGenre
from spotify_dw.models.dim_track import DimTrack
from spotify_dw.models.fact_audio_features import FactAudioFeatures
from spotify_dw.models.fact_track_popularity import FactTrackPopularity
from spotify_dw.pipelines.base import BasePipeline, PipelineResult
from spotify_dw.transformers.artist_transformer import ArtistTransformer
from spotify_dw.transformers.audio_features_transformer import AudioFeaturesTransformer
from spotify_dw.transformers.track_transformer import TrackTransformer


class KaggleLoadPipeline(BasePipeline):
    """Loads historical Kaggle CSV data into all warehouse tables.

    Flow: CSV → Extract → Transform → Load into dim/fact tables.
    Idempotent via upserts — safe to run multiple times.
    """

    def __init__(self, session: Session, file_path: str | Path) -> None:
        super().__init__(session)
        self.file_path = Path(file_path)

    def run(self) -> PipelineResult:
        result = PipelineResult()

        # Extract
        extractor = KaggleCSVExtractor(self.file_path)
        raw_df = extractor.extract()
        result.rows_extracted = len(raw_df)

        # Transform tracks
        track_transformer = TrackTransformer()
        tracks_df = track_transformer.transform(raw_df)

        # Transform artists (also produces bridge and genre data)
        artist_transformer = ArtistTransformer()
        artists_df = artist_transformer.transform(raw_df)
        bridge_data = artist_transformer.bridge_data
        genre_data = artist_transformer.genre_data

        # Transform audio features
        audio_transformer = AudioFeaturesTransformer()
        audio_df = audio_transformer.transform(raw_df)

        # Ensure today's date exists in dim_date
        today = date.today()
        self._ensure_date(today)
        date_key = int(today.strftime("%Y%m%d"))

        total_loaded = 0

        # Load dim_genre
        if not genre_data.empty:
            unique_genres = genre_data[["genre"]].drop_duplicates().rename(columns={"genre": "genre_name"})
            genre_loader = PostgresLoader(
                self.session, DimGenre, conflict_columns=["genre_name"], batch_size=500
            )
            total_loaded += genre_loader.load(unique_genres)

        # Load dim_album
        if "album_name" in raw_df.columns:
            albums_df = self._prepare_albums(raw_df)
            if not albums_df.empty:
                album_loader = PostgresLoader(
                    self.session, DimAlbum, conflict_columns=["spotify_album_id"], batch_size=500
                )
                total_loaded += album_loader.load(albums_df)

        # Load dim_artist
        artist_loader = PostgresLoader(
            self.session, DimArtist, conflict_columns=["spotify_artist_id"], batch_size=500
        )
        # Generate synthetic IDs for Kaggle artists (no Spotify artist IDs in CSV)
        artists_df["spotify_artist_id"] = artists_df["artist_name"].apply(
            lambda name: f"kaggle_{hash(name) % 10**12}"
        )
        total_loaded += artist_loader.load(artists_df)

        # Load dim_track
        track_load_df = tracks_df[["spotify_track_id", "track_name", "duration_seconds", "explicit"]].copy()
        track_loader = PostgresLoader(
            self.session, DimTrack, conflict_columns=["spotify_track_id"], batch_size=500
        )
        total_loaded += track_loader.load(track_load_df)

        # Load fact_audio_features
        audio_load_df = self._prepare_audio_facts(audio_df, date_key)
        if not audio_load_df.empty:
            audio_loader = PostgresLoader(
                self.session, FactAudioFeatures, conflict_columns=[], update_on_conflict=False, batch_size=500
            )
            total_loaded += audio_loader.load_simple(audio_load_df)

        # Load fact_track_popularity
        pop_df = self._prepare_popularity_facts(tracks_df, date_key)
        if not pop_df.empty:
            pop_loader = PostgresLoader(
                self.session, FactTrackPopularity, conflict_columns=[], update_on_conflict=False, batch_size=500
            )
            total_loaded += pop_loader.load_simple(pop_df)

        result.rows_loaded = total_loaded
        self.logger.info(f"Kaggle pipeline complete: {result.rows_extracted} extracted, {total_loaded} loaded")
        return result

    def _ensure_date(self, d: date) -> None:
        """Insert today's date into dim_date if not present."""
        date_key = int(d.strftime("%Y%m%d"))
        existing = self.session.query(DimDate).filter_by(date_key=date_key).first()
        if not existing:
            dim_date = DimDate(
                date_key=date_key,
                full_date=d,
                year=d.year,
                quarter=(d.month - 1) // 3 + 1,
                month=d.month,
                month_name=d.strftime("%B"),
                week_of_year=d.isocalendar()[1],
                day_of_week=d.weekday(),
                is_weekend=d.weekday() >= 5,
            )
            self.session.add(dim_date)
            self.session.flush()

    def _prepare_albums(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract unique albums from the raw data."""
        albums = df[["album_name"]].drop_duplicates().dropna()
        albums["spotify_album_id"] = albums["album_name"].apply(
            lambda name: f"kaggle_album_{hash(name) % 10**12}"
        )
        return albums

    def _prepare_audio_facts(self, df: pd.DataFrame, date_key: int) -> pd.DataFrame:
        """Prepare audio features for fact table loading.

        Requires track_key lookups — maps spotify_track_id to track_key.
        """
        audio_cols = [
            "spotify_track_id", "danceability", "energy", "loudness", "speechiness",
            "acousticness", "instrumentalness", "liveness", "valence",
            "tempo", "tempo_category", "time_signature", "key", "mode",
        ]
        available = [c for c in audio_cols if c in df.columns]
        audio_df = df[available].copy()

        # Look up track_keys
        track_keys = self._get_track_key_map()
        audio_df["track_key"] = audio_df["spotify_track_id"].map(track_keys)
        audio_df = audio_df.dropna(subset=["track_key"])
        audio_df["track_key"] = audio_df["track_key"].astype(int)
        audio_df["snapshot_date_key"] = date_key
        audio_df = audio_df.drop(columns=["spotify_track_id"])

        return audio_df

    def _prepare_popularity_facts(self, df: pd.DataFrame, date_key: int) -> pd.DataFrame:
        """Prepare popularity data for fact table loading."""
        if "popularity" not in df.columns:
            return pd.DataFrame()

        pop_df = df[["spotify_track_id", "popularity"]].copy()
        track_keys = self._get_track_key_map()
        pop_df["track_key"] = pop_df["spotify_track_id"].map(track_keys)
        pop_df = pop_df.dropna(subset=["track_key"])
        pop_df["track_key"] = pop_df["track_key"].astype(int)
        pop_df["date_key"] = date_key
        pop_df["source"] = "kaggle"
        pop_df = pop_df.drop(columns=["spotify_track_id"])

        return pop_df

    def _get_track_key_map(self) -> dict[str, int]:
        """Query dim_track to build a spotify_track_id -> track_key mapping."""
        rows = self.session.query(DimTrack.spotify_track_id, DimTrack.track_key).all()
        return {row.spotify_track_id: row.track_key for row in rows}

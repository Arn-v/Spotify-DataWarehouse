"""Pipeline: Ingest live data from Spotify Web API into the data warehouse."""

from datetime import date

from sqlalchemy.orm import Session

from spotify_dw.extractors.spotify_api import SpotifyAPIExtractor
from spotify_dw.loaders.postgres_loader import PostgresLoader
from spotify_dw.models.dim_album import DimAlbum
from spotify_dw.models.dim_artist import DimArtist
from spotify_dw.models.dim_date import DimDate
from spotify_dw.models.dim_track import DimTrack
from spotify_dw.models.fact_audio_features import FactAudioFeatures
from spotify_dw.models.fact_track_popularity import FactTrackPopularity
from spotify_dw.pipelines.base import BasePipeline, PipelineResult
from spotify_dw.transformers.artist_transformer import ArtistTransformer
from spotify_dw.transformers.audio_features_transformer import AudioFeaturesTransformer
from spotify_dw.transformers.deduplicator import Deduplicator
from spotify_dw.transformers.track_transformer import TrackTransformer


class IngestionPipeline(BasePipeline):
    """Ingests live Spotify data: API → Transform → Dedup → Load.

    Gracefully handles API endpoint failures via SpotifyAPIExtractor.
    """

    def __init__(self, session: Session, genre_seeds: list[str] | None = None) -> None:
        super().__init__(session)
        self.genre_seeds = genre_seeds

    def run(self) -> PipelineResult:
        result = PipelineResult()

        # Extract from Spotify API
        extractor = SpotifyAPIExtractor(genre_seeds=self.genre_seeds)
        raw_df = extractor.extract()
        result.endpoint_statuses = extractor.endpoint_statuses

        if raw_df.empty:
            result.status = "partial"
            result.errors.append("No data extracted from any API endpoint")
            self.logger.warning("Ingestion produced no data")
            return result

        result.rows_extracted = len(raw_df)

        # Deduplicate against existing warehouse data
        existing_ids = self._get_existing_track_ids()
        deduplicator = Deduplicator(existing_ids=existing_ids)
        new_df = deduplicator.transform(raw_df)

        if new_df.empty:
            self.logger.info("No new tracks to load")
            result.rows_loaded = 0
            return result

        # Transform
        track_transformer = TrackTransformer()
        tracks_df = track_transformer.transform(new_df)

        artist_transformer = ArtistTransformer()
        artists_df = artist_transformer.transform(new_df)

        # Ensure today's date in dim_date
        today = date.today()
        self._ensure_date(today)
        date_key = int(today.strftime("%Y%m%d"))

        total_loaded = 0

        # Load dim_album
        if "spotify_album_id" in new_df.columns:
            albums_df = new_df[["spotify_album_id", "album_name", "album_type", "total_tracks"]].drop_duplicates(
                subset=["spotify_album_id"]
            )
            album_loader = PostgresLoader(
                self.session, DimAlbum, conflict_columns=["spotify_album_id"]
            )
            total_loaded += album_loader.load(albums_df)

        # Load dim_artist
        if "artist_ids" in new_df.columns:
            # Use real Spotify artist IDs from API
            artist_rows = []
            for _, row in new_df.iterrows():
                names = str(row.get("artists", "")).split(";")
                ids = str(row.get("artist_ids", "")).split(";")
                for name, aid in zip(names, ids):
                    if aid and name:
                        artist_rows.append({"spotify_artist_id": aid.strip(), "artist_name": name.strip().lower()})

            if artist_rows:
                import pandas as pd
                artists_load_df = pd.DataFrame(artist_rows).drop_duplicates(subset=["spotify_artist_id"])
                artist_loader = PostgresLoader(
                    self.session, DimArtist, conflict_columns=["spotify_artist_id"]
                )
                total_loaded += artist_loader.load(artists_load_df)

        # Load dim_track
        track_load_df = tracks_df[["spotify_track_id", "track_name", "duration_seconds", "explicit"]].copy()
        if "isrc" in tracks_df.columns:
            track_load_df["isrc"] = tracks_df["isrc"]
        track_loader = PostgresLoader(
            self.session, DimTrack, conflict_columns=["spotify_track_id"]
        )
        total_loaded += track_loader.load(track_load_df)

        # Fetch and load audio features
        track_ids = tracks_df["spotify_track_id"].tolist()
        audio_df = extractor.get_audio_features(track_ids)

        if not audio_df.empty:
            audio_transformer = AudioFeaturesTransformer()
            audio_df = audio_transformer.transform(audio_df)

            # Map to track_keys
            track_key_map = self._get_track_key_map()
            audio_df["track_key"] = audio_df["spotify_track_id"].map(track_key_map)
            audio_df = audio_df.dropna(subset=["track_key"])
            audio_df["track_key"] = audio_df["track_key"].astype(int)
            audio_df["snapshot_date_key"] = date_key
            audio_df = audio_df.drop(columns=["spotify_track_id"], errors="ignore")

            # Remove non-table columns
            table_cols = {c.name for c in FactAudioFeatures.__table__.columns}
            audio_load_cols = [c for c in audio_df.columns if c in table_cols]
            audio_df = audio_df[audio_load_cols]

            audio_loader = PostgresLoader(
                self.session, FactAudioFeatures, conflict_columns=[], update_on_conflict=False
            )
            total_loaded += audio_loader.load_simple(audio_df)

        # Load popularity facts
        if "popularity" in tracks_df.columns:
            track_key_map = self._get_track_key_map()
            pop_df = tracks_df[["spotify_track_id", "popularity"]].copy()
            pop_df["track_key"] = pop_df["spotify_track_id"].map(track_key_map)
            pop_df = pop_df.dropna(subset=["track_key"])
            pop_df["track_key"] = pop_df["track_key"].astype(int)
            pop_df["date_key"] = date_key
            pop_df["source"] = "api"
            pop_df = pop_df.drop(columns=["spotify_track_id"])

            pop_loader = PostgresLoader(
                self.session, FactTrackPopularity, conflict_columns=[], update_on_conflict=False
            )
            total_loaded += pop_loader.load_simple(pop_df)

        result.rows_loaded = total_loaded
        self.logger.info(f"Ingestion complete: {result.rows_extracted} extracted, {total_loaded} loaded")
        return result

    def _get_existing_track_ids(self) -> set[str]:
        """Get all spotify_track_ids currently in the warehouse."""
        rows = self.session.query(DimTrack.spotify_track_id).all()
        return {row.spotify_track_id for row in rows}

    def _get_track_key_map(self) -> dict[str, int]:
        rows = self.session.query(DimTrack.spotify_track_id, DimTrack.track_key).all()
        return {row.spotify_track_id: row.track_key for row in rows}

    def _ensure_date(self, d: date) -> None:
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

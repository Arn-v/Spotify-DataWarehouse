"""Pipeline: Run all analytics and write results to aggregate tables."""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from spotify_dw.analytics.audio_profile import AudioProfileAnalyzer
from spotify_dw.analytics.genre_analysis import GenreAnalyzer
from spotify_dw.analytics.trending import TrendingAnalyzer
from spotify_dw.models.agg_audio_profiles import AggAudioProfile
from spotify_dw.models.agg_genre_stats import AggGenreStats
from spotify_dw.models.agg_trending_tracks import AggTrendingTrack
from spotify_dw.pipelines.base import BasePipeline, PipelineResult


class AnalyticsPipeline(BasePipeline):
    """Reads warehouse data, runs all analytics, writes to aggregate tables.

    Analyzers:
        - TrendingAnalyzer → agg_trending_tracks
        - GenreAnalyzer → agg_genre_stats
        - AudioProfileAnalyzer → agg_audio_profiles
    """

    def __init__(self, session: Session) -> None:
        super().__init__(session)

    def run(self) -> PipelineResult:
        result = PipelineResult()
        total_loaded = 0

        # Run trending analysis
        trending_loaded = self._run_trending()
        total_loaded += trending_loaded

        # Run genre analysis
        genre_loaded = self._run_genre_analysis()
        total_loaded += genre_loaded

        # Run audio profiling
        profile_loaded = self._run_audio_profiling()
        total_loaded += profile_loaded

        result.rows_loaded = total_loaded
        self.logger.info(f"Analytics pipeline complete: {total_loaded} aggregate rows written")
        return result

    def _run_trending(self) -> int:
        """Run trending analysis and write to agg_trending_tracks."""
        # Read popularity facts
        query = text("""
            SELECT track_key, date_key, popularity
            FROM fact_track_popularity
            ORDER BY date_key
        """)
        pop_df = pd.read_sql(query, self.session.bind)

        if pop_df.empty:
            self.logger.info("No popularity data for trending analysis")
            return 0

        analyzer = TrendingAnalyzer()
        trending_df = analyzer.analyze(pop_df)

        if trending_df.empty:
            return 0

        # Clear old trending data and insert new
        self.session.query(AggTrendingTrack).delete()
        self.session.flush()

        records = trending_df.to_dict(orient="records")
        objects = [AggTrendingTrack(**r) for r in records]
        self.session.bulk_save_objects(objects)
        self.session.flush()

        self.logger.info(f"Trending: {len(objects)} tracks ranked")
        return len(objects)

    def _run_genre_analysis(self) -> int:
        """Run genre analysis and write to agg_genre_stats."""
        # Join tracks with genres and audio features
        query = text("""
            SELECT
                bag.genre_key,
                faf.danceability,
                faf.energy,
                faf.tempo,
                ftp.popularity
            FROM bridge_artist_genre bag
            JOIN bridge_track_artist bta ON bag.artist_key = bta.artist_key
            JOIN fact_audio_features faf ON bta.track_key = faf.track_key
            LEFT JOIN fact_track_popularity ftp ON bta.track_key = ftp.track_key
        """)

        try:
            genre_df = pd.read_sql(query, self.session.bind)
        except Exception as e:
            self.logger.warning(f"Genre analysis query failed: {e}")
            return 0

        if genre_df.empty:
            self.logger.info("No data for genre analysis")
            return 0

        analyzer = GenreAnalyzer()
        stats_df = analyzer.analyze(genre_df)

        if stats_df.empty:
            return 0

        # Clear old and insert new
        self.session.query(AggGenreStats).delete()
        self.session.flush()

        records = stats_df.to_dict(orient="records")
        objects = [AggGenreStats(**r) for r in records]
        self.session.bulk_save_objects(objects)
        self.session.flush()

        self.logger.info(f"Genre analysis: {len(objects)} genre stats written")
        return len(objects)

    def _run_audio_profiling(self) -> int:
        """Run audio profiling and write to agg_audio_profiles."""
        query = text("""
            SELECT danceability, energy, valence, tempo
            FROM fact_audio_features
            WHERE danceability IS NOT NULL
              AND energy IS NOT NULL
              AND valence IS NOT NULL
              AND tempo IS NOT NULL
        """)
        audio_df = pd.read_sql(query, self.session.bind)

        if audio_df.empty or len(audio_df) < 5:
            self.logger.info("Not enough audio data for profiling")
            return 0

        analyzer = AudioProfileAnalyzer(n_clusters=min(5, len(audio_df)))
        profiles_df = analyzer.analyze(audio_df)

        if profiles_df.empty:
            return 0

        # Clear old and insert new
        self.session.query(AggAudioProfile).delete()
        self.session.flush()

        records = profiles_df.to_dict(orient="records")
        objects = [AggAudioProfile(**r) for r in records]
        self.session.bulk_save_objects(objects)
        self.session.flush()

        self.logger.info(f"Audio profiling: {len(objects)} clusters written")
        return len(objects)

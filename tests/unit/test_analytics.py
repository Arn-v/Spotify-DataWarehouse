"""Unit tests for analytics modules."""

import pandas as pd

from spotify_dw.analytics.audio_profile import AudioProfileAnalyzer
from spotify_dw.analytics.genre_analysis import GenreAnalyzer
from spotify_dw.analytics.trending import TrendingAnalyzer


class TestTrendingAnalyzer:

    def test_computes_popularity_delta(self):
        df = pd.DataFrame({
            "track_key": [1, 1, 2, 2],
            "date_key": [20260310, 20260316, 20260310, 20260316],
            "popularity": [50, 80, 90, 85],
        })
        result = TrendingAnalyzer().analyze(df, window_days=7)
        assert len(result) == 2
        # Track 1 gained 30, track 2 lost 5
        track1 = result[result["track_key"] == 1].iloc[0]
        assert track1["popularity_delta"] == 30
        assert track1["rank"] == 1  # highest velocity

    def test_empty_input(self):
        result = TrendingAnalyzer().analyze(pd.DataFrame(), window_days=7)
        assert result.empty

    def test_single_date_point(self):
        df = pd.DataFrame({
            "track_key": [1],
            "date_key": [20260316],
            "popularity": [50],
        })
        result = TrendingAnalyzer().analyze(df, window_days=7)
        assert len(result) == 1
        assert result.iloc[0]["popularity_delta"] == 0


class TestGenreAnalyzer:

    def test_aggregates_genre_stats(self):
        df = pd.DataFrame({
            "genre_key": [1, 1, 2],
            "popularity": [80, 90, 60],
            "danceability": [0.8, 0.7, 0.5],
            "energy": [0.9, 0.8, 0.4],
            "tempo": [120, 130, 80],
        })
        result = GenreAnalyzer().analyze(df)
        assert len(result) == 2
        assert "period" in result.columns
        assert "avg_popularity" in result.columns

    def test_empty_input(self):
        result = GenreAnalyzer().analyze(pd.DataFrame())
        assert result.empty

    def test_pre_aggregated_data(self):
        df = pd.DataFrame({
            "genre_key": [1, 2],
            "track_count": [100, 50],
            "avg_popularity": [75.0, 60.0],
            "avg_danceability": [0.7, 0.5],
            "avg_energy": [0.8, 0.4],
            "avg_tempo": [125.0, 90.0],
        })
        result = GenreAnalyzer().analyze(df)
        assert len(result) == 2


class TestAudioProfileAnalyzer:

    def test_clusters_tracks(self):
        # Generate enough data points for 3 clusters
        import numpy as np
        np.random.seed(42)
        n = 30
        df = pd.DataFrame({
            "danceability": np.random.uniform(0, 1, n),
            "energy": np.random.uniform(0, 1, n),
            "valence": np.random.uniform(0, 1, n),
            "tempo": np.random.uniform(60, 200, n),
        })
        result = AudioProfileAnalyzer(n_clusters=3).analyze(df)
        assert len(result) == 3
        assert "cluster_label" in result.columns
        assert "centroid_tempo" in result.columns
        assert result["track_count"].sum() == n

    def test_too_few_points(self):
        df = pd.DataFrame({
            "danceability": [0.5],
            "energy": [0.5],
            "valence": [0.5],
            "tempo": [120],
        })
        result = AudioProfileAnalyzer(n_clusters=5).analyze(df)
        assert result.empty

    def test_label_generation(self):
        analyzer = AudioProfileAnalyzer()
        # High energy + high dance = Party Bangers
        assert analyzer._generate_label([0.8, 0.9, 0.5, 130]) == "Party Bangers"
        # Low energy + low valence = Chill Vibes
        assert analyzer._generate_label([0.3, 0.3, 0.3, 80]) == "Chill Vibes"

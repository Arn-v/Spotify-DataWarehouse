"""SQLAlchemy ORM models for the Spotify Data Warehouse star schema."""

from spotify_dw.models.agg_audio_profiles import AggAudioProfile
from spotify_dw.models.agg_genre_stats import AggGenreStats
from spotify_dw.models.agg_trending_tracks import AggTrendingTrack
from spotify_dw.models.base import Base
from spotify_dw.models.bridge_artist_genre import BridgeArtistGenre
from spotify_dw.models.bridge_track_artist import BridgeTrackArtist
from spotify_dw.models.dim_album import DimAlbum
from spotify_dw.models.dim_artist import DimArtist
from spotify_dw.models.dim_date import DimDate
from spotify_dw.models.dim_genre import DimGenre
from spotify_dw.models.dim_track import DimTrack
from spotify_dw.models.fact_audio_features import FactAudioFeatures
from spotify_dw.models.fact_track_popularity import FactTrackPopularity
from spotify_dw.models.pipeline_run_log import PipelineRunLog

__all__ = [
    "Base",
    "DimTrack",
    "DimArtist",
    "DimAlbum",
    "DimGenre",
    "DimDate",
    "BridgeTrackArtist",
    "BridgeArtistGenre",
    "FactAudioFeatures",
    "FactTrackPopularity",
    "AggTrendingTrack",
    "AggGenreStats",
    "AggAudioProfile",
    "PipelineRunLog",
]

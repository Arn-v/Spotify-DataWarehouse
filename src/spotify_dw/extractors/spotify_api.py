"""Extractor for Spotify Web API with graceful degradation."""

import logging
from typing import Any

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from config.settings import get_settings
from spotify_dw.extractors.base import BaseExtractor
from spotify_dw.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Genre seeds for the recommendations endpoint
DEFAULT_GENRE_SEEDS = ["pop", "rock", "hip-hop", "electronic", "r-n-b", "latin", "classical", "jazz", "country", "indie"]


class SpotifyAPIExtractor(BaseExtractor):
    """Extracts track data from multiple Spotify Web API endpoints.

    Implements graceful degradation: if an endpoint returns a 403 or fails,
    it logs a warning and skips that endpoint instead of crashing.

    Endpoints used:
        - Featured Playlists → tracks
        - New Releases → tracks
        - Recommendations by genre seeds → tracks
    """

    def __init__(self, genre_seeds: list[str] | None = None) -> None:
        super().__init__()
        self.genre_seeds = genre_seeds or DEFAULT_GENRE_SEEDS[:5]
        self.rate_limiter = RateLimiter(max_calls=25, period=30.0)
        self.endpoint_statuses: dict[str, str] = {}
        self._client: spotipy.Spotify | None = None

    def _get_client(self) -> spotipy.Spotify:
        """Create and return an authenticated spotipy client."""
        if self._client is None:
            settings = get_settings()
            auth_manager = SpotifyClientCredentials(
                client_id=settings.spotify_client_id,
                client_secret=settings.spotify_client_secret,
            )
            self._client = spotipy.Spotify(auth_manager=auth_manager, requests_timeout=10)
        return self._client

    def validate_source(self) -> bool:
        """Check that Spotify API credentials are configured."""
        settings = get_settings()
        if not settings.spotify_client_id or not settings.spotify_client_secret:
            self.logger.error("Spotify API credentials not configured")
            return False
        return True

    def extract(self) -> pd.DataFrame:
        """Pull tracks from all available endpoints and merge results."""
        if not self.validate_source():
            self.logger.warning("API credentials missing, returning empty DataFrame")
            return pd.DataFrame()

        all_tracks: list[dict[str, Any]] = []

        # Try each endpoint with graceful degradation
        all_tracks.extend(self._extract_featured_playlists())
        all_tracks.extend(self._extract_new_releases())
        all_tracks.extend(self._extract_recommendations())

        if not all_tracks:
            self.logger.warning("No tracks extracted from any endpoint")
            return pd.DataFrame()

        df = pd.DataFrame(all_tracks)

        # Deduplicate by spotify_track_id
        df = df.drop_duplicates(subset=["spotify_track_id"], keep="first")

        self._log_extraction(len(df))
        return df

    def _extract_featured_playlists(self) -> list[dict[str, Any]]:
        """Pull tracks from Spotify's featured playlists."""
        endpoint = "featured_playlists"
        try:
            client = self._get_client()
            tracks = []

            with self.rate_limiter:
                playlists_response = client.featured_playlists(limit=10)

            for playlist in playlists_response.get("playlists", {}).get("items", []):
                with self.rate_limiter:
                    playlist_tracks = client.playlist_tracks(playlist["id"], limit=50)

                for item in playlist_tracks.get("items", []):
                    track = item.get("track")
                    if track and track.get("id"):
                        tracks.append(self._parse_track(track))

            self.endpoint_statuses[endpoint] = "success"
            self.logger.info(f"Featured playlists: {len(tracks)} tracks")
            return tracks

        except Exception as e:
            self.endpoint_statuses[endpoint] = f"failed: {e}"
            self.logger.warning(f"Featured playlists endpoint failed: {e}")
            return []

    def _extract_new_releases(self) -> list[dict[str, Any]]:
        """Pull tracks from new album releases."""
        endpoint = "new_releases"
        try:
            client = self._get_client()
            tracks = []

            with self.rate_limiter:
                releases = client.new_releases(limit=20)

            for album in releases.get("albums", {}).get("items", []):
                with self.rate_limiter:
                    album_tracks = client.album_tracks(album["id"], limit=50)

                for track in album_tracks.get("items", []):
                    if track.get("id"):
                        parsed = self._parse_track_minimal(track, album)
                        tracks.append(parsed)

            self.endpoint_statuses[endpoint] = "success"
            self.logger.info(f"New releases: {len(tracks)} tracks")
            return tracks

        except Exception as e:
            self.endpoint_statuses[endpoint] = f"failed: {e}"
            self.logger.warning(f"New releases endpoint failed: {e}")
            return []

    def _extract_recommendations(self) -> list[dict[str, Any]]:
        """Pull tracks from recommendations by genre seeds."""
        endpoint = "recommendations"
        try:
            client = self._get_client()
            tracks = []

            for genre in self.genre_seeds:
                try:
                    with self.rate_limiter:
                        recs = client.recommendations(seed_genres=[genre], limit=50)

                    for track in recs.get("tracks", []):
                        if track.get("id"):
                            tracks.append(self._parse_track(track))
                except Exception as e:
                    self.logger.warning(f"Recommendations for genre '{genre}' failed: {e}")

            self.endpoint_statuses[endpoint] = "success"
            self.logger.info(f"Recommendations: {len(tracks)} tracks")
            return tracks

        except Exception as e:
            self.endpoint_statuses[endpoint] = f"failed: {e}"
            self.logger.warning(f"Recommendations endpoint failed: {e}")
            return []

    def get_audio_features(self, track_ids: list[str]) -> pd.DataFrame:
        """Fetch audio features for a batch of track IDs.

        Args:
            track_ids: List of Spotify track IDs (max 100 per API call).
        """
        client = self._get_client()
        all_features = []

        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i + 100]
            try:
                with self.rate_limiter:
                    features = client.audio_features(batch)
                if features:
                    all_features.extend([f for f in features if f is not None])
            except Exception as e:
                self.logger.warning(f"Audio features batch failed: {e}")

        if not all_features:
            return pd.DataFrame()

        df = pd.DataFrame(all_features)
        if "id" in df.columns:
            df = df.rename(columns={"id": "spotify_track_id"})

        return df

    def _parse_track(self, track: dict) -> dict[str, Any]:
        """Parse a full track object from the API into a flat dict."""
        album = track.get("album", {})
        artists = track.get("artists", [])

        return {
            "spotify_track_id": track["id"],
            "track_name": track.get("name", ""),
            "popularity": track.get("popularity", 0),
            "duration_ms": track.get("duration_ms", 0),
            "explicit": track.get("explicit", False),
            "isrc": track.get("external_ids", {}).get("isrc"),
            "album_name": album.get("name", ""),
            "spotify_album_id": album.get("id", ""),
            "album_type": album.get("album_type", ""),
            "release_date": album.get("release_date", ""),
            "total_tracks": album.get("total_tracks", 0),
            "artists": ";".join(a.get("name", "") for a in artists),
            "artist_ids": ";".join(a.get("id", "") for a in artists),
        }

    def _parse_track_minimal(self, track: dict, album: dict) -> dict[str, Any]:
        """Parse a minimal track object (from album_tracks) into a flat dict."""
        artists = track.get("artists", [])

        return {
            "spotify_track_id": track["id"],
            "track_name": track.get("name", ""),
            "popularity": 0,  # Not available from album_tracks
            "duration_ms": track.get("duration_ms", 0),
            "explicit": track.get("explicit", False),
            "album_name": album.get("name", ""),
            "spotify_album_id": album.get("id", ""),
            "album_type": album.get("album_type", ""),
            "release_date": album.get("release_date", ""),
            "total_tracks": album.get("total_tracks", 0),
            "artists": ";".join(a.get("name", "") for a in artists),
            "artist_ids": ";".join(a.get("id", "") for a in artists),
        }

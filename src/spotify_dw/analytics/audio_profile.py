"""Audio mood profiler — K-means clustering on audio features."""

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from spotify_dw.analytics.base import BaseAnalyzer

# Default cluster labels based on centroid characteristics
MOOD_LABELS = {
    "high_energy_dance": "Party Bangers",
    "chill_acoustic": "Chill Vibes",
    "intense_loud": "Workout Fuel",
    "mellow_slow": "Relaxation",
    "upbeat_positive": "Feel Good",
}

CLUSTER_FEATURES = ["danceability", "energy", "valence", "tempo"]


class AudioProfileAnalyzer(BaseAnalyzer):
    """Clusters tracks by audio features into mood segments.

    Uses K-means clustering on danceability, energy, valence, and tempo
    to identify marketing-relevant mood profiles.
    """

    REQUIRED_COLUMNS = CLUSTER_FEATURES

    def __init__(self, n_clusters: int = 5) -> None:
        super().__init__()
        self.n_clusters = n_clusters

    def analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cluster tracks and return cluster profiles.

        Args:
            df: DataFrame with audio feature columns (danceability, energy, valence, tempo).

        Returns:
            DataFrame with: cluster_id, cluster_label, centroid_*, track_count, snapshot_date.
        """
        if not self._validate_input(df, self.REQUIRED_COLUMNS):
            return pd.DataFrame()

        df = df.copy()
        features = df[CLUSTER_FEATURES].dropna()

        if len(features) < self.n_clusters:
            self.logger.warning(
                f"Not enough data points ({len(features)}) for {self.n_clusters} clusters"
            )
            return pd.DataFrame()

        # Scale features for clustering
        scaler = StandardScaler()
        scaled = scaler.fit_transform(features)

        # K-means clustering
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42, n_init=10)
        labels = kmeans.fit_predict(scaled)

        # Inverse-transform centroids to original scale
        centroids = scaler.inverse_transform(kmeans.cluster_centers_)

        # Build cluster profile DataFrame
        profiles = []
        for i in range(self.n_clusters):
            cluster_mask = labels == i
            track_count = int(cluster_mask.sum())

            # Auto-label based on centroid characteristics
            label = self._generate_label(centroids[i])

            profiles.append({
                "cluster_id": i,
                "cluster_label": label,
                "centroid_danceability": round(centroids[i][0], 3),
                "centroid_energy": round(centroids[i][1], 3),
                "centroid_valence": round(centroids[i][2], 3),
                "centroid_tempo": round(centroids[i][3], 1),
                "track_count": track_count,
                "snapshot_date": pd.Timestamp.now().date(),
            })

        result = pd.DataFrame(profiles)
        self.logger.info(f"Audio profiling: {self.n_clusters} clusters identified")
        return result

    def _generate_label(self, centroid: list[float]) -> str:
        """Generate a human-readable label based on centroid characteristics.

        centroid order: [danceability, energy, valence, tempo]
        """
        dance, energy, valence, tempo = centroid

        if energy > 0.7 and dance > 0.7:
            return "Party Bangers"
        elif energy < 0.4 and valence < 0.4:
            return "Chill Vibes"
        elif energy > 0.7 and tempo > 130:
            return "Workout Fuel"
        elif energy < 0.4 and tempo < 100:
            return "Relaxation"
        elif valence > 0.6 and dance > 0.6:
            return "Feel Good"
        elif energy > 0.6:
            return "High Energy"
        elif valence > 0.5:
            return "Upbeat"
        else:
            return "Moody"

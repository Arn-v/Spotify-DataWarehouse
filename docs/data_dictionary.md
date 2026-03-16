# Data Dictionary

## Dimension Tables

### dim_track
| Column | Type | Description |
|---|---|---|
| track_key | SERIAL PK | Surrogate key |
| spotify_track_id | VARCHAR(62) UNIQUE | Spotify's track identifier |
| track_name | VARCHAR(500) | Normalized track name (lowercase) |
| album_key | INT FK | Reference to dim_album |
| duration_seconds | FLOAT | Track length in seconds |
| explicit | BOOLEAN | Whether track has explicit lyrics |
| isrc | VARCHAR(20) | International Standard Recording Code |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last update time |

### dim_artist
| Column | Type | Description |
|---|---|---|
| artist_key | SERIAL PK | Surrogate key |
| spotify_artist_id | VARCHAR(62) UNIQUE | Spotify's artist identifier |
| artist_name | VARCHAR(500) | Normalized artist name (lowercase) |
| popularity | INT | Artist popularity score (0-100) |
| followers | INT | Number of followers |
| created_at | TIMESTAMP | Row creation time |
| updated_at | TIMESTAMP | Last update time |

### dim_album
| Column | Type | Description |
|---|---|---|
| album_key | SERIAL PK | Surrogate key |
| spotify_album_id | VARCHAR(62) UNIQUE | Spotify's album identifier |
| album_name | VARCHAR(500) | Album name |
| album_type | VARCHAR(20) | album / single / compilation |
| release_date | DATE | Album release date |
| total_tracks | INT | Number of tracks in album |

### dim_genre
| Column | Type | Description |
|---|---|---|
| genre_key | SERIAL PK | Surrogate key |
| genre_name | VARCHAR(100) UNIQUE | Genre name (lowercase) |

### dim_date
| Column | Type | Description |
|---|---|---|
| date_key | INT PK | YYYYMMDD format |
| full_date | DATE UNIQUE | Calendar date |
| year | INT | Year |
| quarter | INT | Quarter (1-4) |
| month | INT | Month (1-12) |
| month_name | VARCHAR(10) | Month name |
| week_of_year | INT | ISO week number |
| day_of_week | INT | 0=Monday, 6=Sunday |
| is_weekend | BOOLEAN | Saturday or Sunday |

## Bridge Tables

### bridge_track_artist
| Column | Type | Description |
|---|---|---|
| track_key | INT FK PK | Reference to dim_track |
| artist_key | INT FK PK | Reference to dim_artist |
| is_primary | BOOLEAN | Whether this is the primary (first-listed) artist |

### bridge_artist_genre
| Column | Type | Description |
|---|---|---|
| artist_key | INT FK PK | Reference to dim_artist |
| genre_key | INT FK PK | Reference to dim_genre |

## Fact Tables

### fact_audio_features
| Column | Type | Range | Description |
|---|---|---|---|
| audio_feature_key | SERIAL PK | | Surrogate key |
| track_key | INT FK | | Reference to dim_track |
| danceability | FLOAT | 0.0-1.0 | How suitable for dancing |
| energy | FLOAT | 0.0-1.0 | Perceptual intensity measure |
| loudness | FLOAT | dB | Overall loudness |
| speechiness | FLOAT | 0.0-1.0 | Presence of spoken words |
| acousticness | FLOAT | 0.0-1.0 | Acoustic confidence |
| instrumentalness | FLOAT | 0.0-1.0 | Predicts no vocals |
| liveness | FLOAT | 0.0-1.0 | Presence of audience |
| valence | FLOAT | 0.0-1.0 | Musical positiveness |
| tempo | FLOAT | BPM | Estimated tempo |
| tempo_category | VARCHAR(10) | slow/mid/fast | Binned: <90=slow, 90-140=mid, >140=fast |
| time_signature | INT | 3-7 | Estimated time signature |
| key | INT | 0-11 | Pitch class (0=C, 1=C#, ..., 11=B) |
| mode | INT | 0-1 | 0=minor, 1=major |
| snapshot_date_key | INT FK | | When this snapshot was taken |

### fact_track_popularity
| Column | Type | Description |
|---|---|---|
| popularity_key | SERIAL PK | Surrogate key |
| track_key | INT FK | Reference to dim_track |
| date_key | INT FK | Reference to dim_date |
| popularity | INT (0-100) | Spotify popularity score |
| source | VARCHAR(10) | 'api' or 'kaggle' |

**Note**: This is an append-only table. Each ingestion run adds a new snapshot, creating a time series of popularity changes.

## Aggregate Tables

### agg_trending_tracks
| Column | Type | Description |
|---|---|---|
| track_key | INT FK | Reference to dim_track |
| window_start | DATE | Start of trending window |
| window_end | DATE | End of trending window |
| popularity_delta | INT | Change in popularity |
| velocity | FLOAT | Popularity change per day |
| rank | INT | Rank by velocity (1 = fastest riser) |

### agg_genre_stats
| Column | Type | Description |
|---|---|---|
| genre_key | INT FK | Reference to dim_genre |
| period | VARCHAR(10) | 'weekly' or 'monthly' |
| period_date | DATE | Period timestamp |
| track_count | INT | Tracks in this genre |
| avg_popularity | FLOAT | Average popularity |
| avg_danceability | FLOAT | Average danceability |
| avg_energy | FLOAT | Average energy |
| avg_tempo | FLOAT | Average tempo (BPM) |

### agg_audio_profiles
| Column | Type | Description |
|---|---|---|
| cluster_id | INT | K-means cluster identifier |
| cluster_label | VARCHAR(50) | Human label (e.g., "Party Bangers") |
| centroid_tempo | FLOAT | Cluster center: tempo |
| centroid_energy | FLOAT | Cluster center: energy |
| centroid_valence | FLOAT | Cluster center: valence |
| centroid_danceability | FLOAT | Cluster center: danceability |
| track_count | INT | Tracks in this cluster |
| snapshot_date | DATE | When analysis was run |

## Observability

### pipeline_run_log
| Column | Type | Description |
|---|---|---|
| run_id | SERIAL PK | Surrogate key |
| pipeline_name | VARCHAR(50) | Pipeline class name |
| status | VARCHAR(20) | success / failure / partial |
| rows_extracted | INT | Rows pulled from source |
| rows_loaded | INT | Rows written to warehouse |
| errors_json | TEXT | JSON array of error messages |
| duration_seconds | FLOAT | Pipeline run duration |
| endpoint_statuses_json | TEXT | JSON: endpoint name → status |
| started_at | TIMESTAMP | Run start time |
| completed_at | TIMESTAMP | Run end time |

# Data Directory

## Kaggle Dataset

This project uses the **Spotify Tracks Dataset** from Kaggle.

### Download Instructions

1. Go to: https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset
2. Download the CSV file
3. Place it in `data/raw/` as `spotify_tracks.csv`

### Expected Columns

The CSV should contain these columns:
- `track_id` — Spotify track ID
- `artists` — Artist name(s)
- `album_name` — Album name
- `track_name` — Track name
- `popularity` — 0-100 popularity score
- `duration_ms` — Duration in milliseconds
- `explicit` — Boolean
- `danceability`, `energy`, `loudness`, `speechiness`, `acousticness`, `instrumentalness`, `liveness`, `valence` — Audio features (0-1 range, except loudness in dB)
- `tempo` — BPM
- `time_signature`, `key`, `mode` — Musical attributes
- `track_genre` — Genre label

### Loading

```bash
python scripts/seed_kaggle.py --file data/raw/spotify_tracks.csv
```

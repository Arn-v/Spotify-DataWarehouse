import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import DB_PATH


def create_tables():
  conn = sqlite3.connect(DB_PATH)
  c = conn.cursor()

  # raw table - stores data directly from spotify api
  c.execute('''CREATE TABLE IF NOT EXISTS raw_api_tracks (
      id TEXT PRIMARY KEY,
      name TEXT,
      artist_names TEXT,
      album_name TEXT,
      album_id TEXT,
      popularity INTEGER,
      duration_ms INTEGER,
      explicit INTEGER,
      release_date TEXT,
      danceability REAL,
      energy REAL,
      loudness REAL,
      speechiness REAL,
      acousticness REAL,
      instrumentalness REAL,
      liveness REAL,
      valence REAL,
      tempo REAL,
      time_signature INTEGER,
      key INTEGER,
      mode INTEGER,
      ingested_at TEXT
  )''')


  # raw table for kaggle historical tracks
  c.execute('''CREATE TABLE IF NOT EXISTS raw_kaggle_tracks (
      id TEXT PRIMARY KEY,
      name TEXT,
      artists TEXT,
      duration_ms INTEGER,
      explicit INTEGER,
      year INTEGER,
      popularity INTEGER,
      danceability REAL,
      energy REAL,
      key INTEGER,
      loudness REAL,
      mode INTEGER,
      speechiness REAL,
      acousticness REAL,
      instrumentalness REAL,
      liveness REAL,
      valence REAL,
      tempo REAL,
      release_date TEXT
  )''')

  c.execute('''CREATE TABLE IF NOT EXISTS raw_kaggle_artists (
      id TEXT PRIMARY KEY,
      name TEXT,
      followers INTEGER,
      genres TEXT,
      popularity INTEGER
  )''')


  # dimension table for artists
  c.execute('''CREATE TABLE IF NOT EXISTS dim_artists (
      artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
      artist_name TEXT UNIQUE,
      spotify_artist_id TEXT,
      followers INTEGER,
      genres TEXT,
      artist_popularity INTEGER,
      source TEXT
  )''')


  # date dimension - needed for decade-wise analysis
  c.execute('''CREATE TABLE IF NOT EXISTS dim_date (
      date_id INTEGER PRIMARY KEY AUTOINCREMENT,
      full_date TEXT UNIQUE,
      year INTEGER,
      month INTEGER,
      day INTEGER,
      decade INTEGER
  )''')

  # main fact table
  c.execute('''CREATE TABLE IF NOT EXISTS fact_tracks (
      track_id TEXT PRIMARY KEY,
      track_name TEXT,
      artist_id INTEGER,
      date_id INTEGER,
      source TEXT,
      popularity INTEGER,
      duration_ms INTEGER,
      explicit INTEGER,
      danceability REAL,
      energy REAL,
      loudness REAL,
      speechiness REAL,
      acousticness REAL,
      instrumentalness REAL,
      liveness REAL,
      valence REAL,
      tempo REAL,
      key INTEGER,
      mode INTEGER,
      is_high_energy INTEGER,
      is_danceable INTEGER,
      mood_label TEXT,
      tempo_category TEXT
  )''')

  c.execute('''CREATE TABLE IF NOT EXISTS fact_daily_trending (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      track_id TEXT,
      track_name TEXT,
      artist_name TEXT,
      popularity INTEGER,
      snapshot_date TEXT,
      playlist_source TEXT
  )''')

  conn.commit()
  conn.close()
  print("Tables created successfully")



if __name__ == '__main__':
    create_tables()

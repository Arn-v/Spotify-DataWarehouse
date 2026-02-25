import sqlite3
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import DB_PATH
from pipelines.ingestion.spotify_api import fetch_trending_tracks


def get_or_insert_artist(c, artist_name, source='api'):
  # insert artist if not already there, then return the id
  c.execute("INSERT OR IGNORE INTO dim_artists (artist_name, source) VALUES (?, ?)", (artist_name, source))
  c.execute("SELECT artist_id FROM dim_artists WHERE artist_name = ?", (artist_name,))
  return c.fetchone()[0]




def get_or_insert_date(c, release_date):
    if not release_date:
        release_date = "1900-01-01"

    # some tracks only have year, not full date
    if len(release_date) == 4:
        release_date = release_date + "-01-01"

    try:
        d = datetime.strptime(release_date, "%Y-%m-%d")
        year  = d.year
        month =d.month
        day = d.day
        decade = (year // 10) * 10
    except:
        release_date = "1900-01-01"
        year= 1900
        month = 1
        day= 1
        decade =1900

    c.execute("INSERT OR IGNORE INTO dim_date (full_date, year, month, day, decade) VALUES (?, ?, ?, ?, ?)",
              (release_date, year, month, day, decade))
    c.execute("SELECT date_id FROM dim_date WHERE full_date = ?", (release_date,))
    return c.fetchone()[0]




def run_pipeline1():

    tracks, playlist_map = fetch_trending_tracks()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    raw_inserted  = 0
    fact_inserted = 0
    trending_inserted = 0
    today = datetime.now().strftime("%Y-%m-%d")

    for t in tracks:
        tid = t.get('id')
        if not tid:
            continue

        c.execute('''INSERT OR IGNORE INTO raw_api_tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            tid, t.get('name'), t.get('artist_names'), t.get('album_name'), t.get('album_id'),
            t.get('popularity'), t.get('duration_ms'), t.get('explicit'), t.get('release_date'),
            t.get('danceability'), t.get('energy'), t.get('loudness'), t.get('speechiness'),
            t.get('acousticness'), t.get('instrumentalness'), t.get('liveness'), t.get('valence'),
            t.get('tempo'), t.get('time_signature'), t.get('key'), t.get('mode'), t.get('ingested_at')
        ))
        raw_inserted += c.rowcount

        # get first artist from comma separated string
        artist_names_str = t.get('artist_names', '')
        artist_split= artist_names_str.split(',')
        first_artist = artist_split[0].strip()

        artist_id = get_or_insert_artist(c, first_artist)
        date_id = get_or_insert_date(c, t.get('release_date'))

        energy = t.get('energy', 0) or 0
        dance = t.get('danceability', 0) or 0
        valence = t.get('valence', 0) or 0
        tempo =t.get('tempo', 0) or 0

        # mood classification based on valence
        mood = 'neutral'
        if valence > 0.6:
            mood = 'happy'
        elif valence < 0.4:
            mood = 'sad'

        # tempo category
        tempo_cat = 'mid'
        if tempo < 90:
            tempo_cat = 'slow'
        elif tempo > 140:
            tempo_cat = 'fast'

        if energy > 0.7:
            is_high_energy = 1
        else:
            is_high_energy = 0

        if dance > 0.7:
            is_danceable = 1
        else:
            is_danceable = 0

        c.execute('''INSERT OR IGNORE INTO fact_tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            tid, t.get('name'), artist_id, date_id, 'api',
            t.get('popularity'), t.get('duration_ms'), t.get('explicit'),
            dance, energy, t.get('loudness'), t.get('speechiness'),
            t.get('acousticness'), t.get('instrumentalness'), t.get('liveness'),
            valence, tempo, t.get('key'), t.get('mode'),
            is_high_energy, is_danceable,
            mood, tempo_cat
        ))
        fact_inserted += c.rowcount

    # insert into daily trending table
    for track_id, playlist_name in playlist_map:
        c.execute("SELECT name, artist_names, popularity FROM raw_api_tracks WHERE id = ?", (track_id,))
        row = c.fetchone()
        if row:
            c.execute('''INSERT INTO fact_daily_trending (track_id, track_name, artist_name, popularity, snapshot_date, playlist_source)
                         VALUES (?, ?, ?, ?, ?, ?)''',
                      (track_id, row[0], row[1], row[2], today, playlist_name))
            trending_inserted += 1

    conn.commit()
    conn.close()

    print(f"raw_api_tracks: {raw_inserted} new rows")
    print(f"fact_tracks: {fact_inserted} new rows")
    print(f"fact_daily_trending: {trending_inserted} rows added")




if __name__ == '__main__':
    run_pipeline1()

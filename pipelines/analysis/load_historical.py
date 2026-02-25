import sqlite3
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import DB_PATH
from pipelines.analysis.clean_transform import get_cleaned_df


def get_or_insert_artist(c, artist_name):
  c.execute("INSERT OR IGNORE INTO dim_artists (artist_name, source) VALUES (?, 'kaggle')", (artist_name,))
  c.execute("SELECT artist_id FROM dim_artists WHERE artist_name = ?", (artist_name,))
  return c.fetchone()[0]




def get_or_insert_date(c, release_date, year):
    if not release_date or str(release_date) == 'nan':
        release_date = str(year) + "-01-01"

    try:
        d = datetime.strptime(str(release_date)[:10], "%Y-%m-%d")
        yr = d.year
        mo  = d.month
        dy  = d.day
        decade = (yr // 10) * 10
    except:
        # fallback if date parsing fails
        yr = int(year)
        mo= 1
        dy = 1
        decade = (int(year) // 10) * 10
        release_date = str(year) + "-01-01"

    c.execute("INSERT OR IGNORE INTO dim_date (full_date, year, month, day, decade) VALUES (?, ?, ?, ?, ?)",
              (str(release_date)[:10], yr, mo, dy, decade))
    c.execute("SELECT date_id FROM dim_date WHERE full_date = ?", (str(release_date)[:10],))
    return c.fetchone()[0]




def run_historical_load(sample=100000):

    df = get_cleaned_df()
    df = df.head(sample)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    total = len(df)
    batch_size = 1000
    loaded= 0

    for i, row in df.iterrows():
        tid = row.get('id')
        if not tid:
            continue

        c.execute('''INSERT OR IGNORE INTO raw_kaggle_tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            str(tid), row.get('name'), row.get('artists'), row.get('duration_ms'),
            row.get('explicit'), row.get('year'), row.get('popularity'),
            row.get('danceability'), row.get('energy'), row.get('key'),
            row.get('loudness'), row.get('mode'), row.get('speechiness'),
            row.get('acousticness'), row.get('instrumentalness'), row.get('liveness'),
            row.get('valence'), row.get('tempo'), row.get('release_date')
        ))

        artist_id =get_or_insert_artist(c, str(row.get('artists', 'Unknown')))
        date_id = get_or_insert_date(c, row.get('release_date'), row.get('year', 1900))

        c.execute('''INSERT OR IGNORE INTO fact_tracks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            str(tid), row.get('name'), artist_id, date_id, 'kaggle',
            row.get('popularity'), row.get('duration_ms'), row.get('explicit'),
            row.get('danceability'), row.get('energy'), row.get('loudness'),
            row.get('speechiness'), row.get('acousticness'), row.get('instrumentalness'),
            row.get('liveness'), row.get('valence'), row.get('tempo'),
            row.get('key'), row.get('mode'),
            row.get('is_high_energy'), row.get('is_danceable'),
            row.get('mood_label'), row.get('tempo_category')
        ))

        loaded += 1

        # commit every 1000 rows to avoid memory issues
        if loaded % batch_size == 0:
            conn.commit()
        if loaded % 50000 == 0:
            print(f"Loaded {loaded} / {total} rows...")

    conn.commit()
    conn.close()
    print(f"Done. Total rows loaded: {loaded}")




if __name__ == '__main__':
    run_historical_load()

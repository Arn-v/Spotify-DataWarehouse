import sqlite3
import pandas as pd
import ast
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import DB_PATH, KAGGLE_ARTISTS_CSV


# genres are stored as python list strings in the csv - need to parse them
def parse_genres(val):
  try:
    parsed = ast.literal_eval(str(val))
    if isinstance(parsed, list):
      all_genres = []
      for g in parsed:
        all_genres.append(g)
      return ', '.join(all_genres)
  except:
    pass
  result = str(val)
  result = result.strip("[]'\" ")
  return result




def run_artists_load():

    print("Reading artists CSV...")
    df = pd.read_csv(KAGGLE_ARTISTS_CSV)
    df = df.head(200000)
    print(f"Artists loaded: {df.shape}")

    df = df.drop_duplicates(subset='id')
    df = df[df['id'].notna()]
    df = df[df['name'].notna()]

    df['genres'] = df['genres'].apply(parse_genres)

    followers_raw   = pd.to_numeric(df['followers'], errors='coerce')
    df['followers'] = followers_raw.fillna(0).astype(int)

    popularity_raw   = pd.to_numeric(df['popularity'], errors='coerce')
    df['popularity'] = popularity_raw.fillna(0).astype(int)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    raw_inserted = 0
    dim_updated  = 0

    for _, row in df.iterrows():

        c.execute('''INSERT OR IGNORE INTO raw_kaggle_artists VALUES (?, ?, ?, ?, ?)''', (
            str(row['id']), str(row['name']), int(row['followers']),
            str(row['genres']), int(row['popularity'])
        ))
        raw_inserted += c.rowcount

        # update the dim table with follower/genre info
        c.execute('''UPDATE dim_artists
                     SET spotify_artist_id = ?, followers = ?, genres = ?, artist_popularity = ?
                     WHERE artist_name = ?''',
                  (str(row['id']), int(row['followers']), str(row['genres']),
                   int(row['popularity']), str(row['name'])))
        dim_updated += c.rowcount

    conn.commit()
    conn.close()

    print(f"raw_kaggle_artists: {raw_inserted} new rows")
    print(f"dim_artists updated with genre/follower data: {dim_updated} rows")




if __name__ == '__main__':
    run_artists_load()

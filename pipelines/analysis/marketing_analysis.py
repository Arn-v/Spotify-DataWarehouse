import sqlite3
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import DB_PATH, ROOT

DATA_DIR = os.path.join(ROOT, 'data')


def run_analysis():

    conn = sqlite3.connect(DB_PATH)

    # trending tracks from today
    print("\n========== ANALYSIS 1: TRENDING SONGS TODAY ==========")
    df1 = pd.read_sql_query('''
        SELECT track_name, artist_name, popularity, snapshot_date, playlist_source
        FROM fact_daily_trending
        ORDER BY popularity DESC
        LIMIT 20
    ''', conn)
    print(df1.to_string(index=False))
    df1.to_csv(os.path.join(DATA_DIR, 'trending_today.csv'), index=False)


    print("\n========== ANALYSIS 2: TOP ARTISTS BY AVERAGE POPULARITY ==========")
    df2 = pd.read_sql_query('''
        SELECT da.artist_name, COUNT(*) as track_count, ROUND(AVG(ft.popularity), 2) as avg_popularity
        FROM fact_tracks ft
        JOIN dim_artists da ON ft.artist_id = da.artist_id
        WHERE ft.source = 'kaggle'
        GROUP BY da.artist_name
        HAVING track_count > 5
        ORDER BY avg_popularity DESC
        LIMIT 25
    ''', conn)
    print(df2.to_string(index=False))


    print("\n========== ANALYSIS 3: TOP GENRES BY TRACK COUNT ==========")
    df_genre_raw = pd.read_sql_query('''
        SELECT da.genres, ft.popularity
        FROM fact_tracks ft
        JOIN dim_artists da ON ft.artist_id = da.artist_id
        WHERE da.genres IS NOT NULL AND da.genres != '' AND da.genres != 'nan'
        AND ft.source = 'kaggle'
    ''', conn)

    genre_rows = []
    for _, row in df_genre_raw.iterrows():
        for g in str(row['genres']).split(','):
            g = g.strip()
            if g:
              genre_rows.append({'genre': g, 'popularity': row['popularity']})

    if genre_rows:
        gdf = pd.DataFrame(genre_rows)
        top_genres = gdf.groupby('genre').agg(
            track_count=('popularity', 'count'),
            avg_popularity=('popularity', 'mean')
        ).reset_index()
        top_genres = top_genres[top_genres['track_count'] > 50]
        top_genres['avg_popularity'] = top_genres['avg_popularity'].round(2)
        top_genres = top_genres.sort_values('track_count', ascending=False).head(20)
        print(top_genres.to_string(index=False))
        top_genres.to_csv(os.path.join(DATA_DIR, 'top_genres.csv'), index=False)
    else:
        print("No genre data found - make sure you ran load_artists.py")


    # how music audio profile changed across decades
    print("\n========== ANALYSIS 3b: AUDIO PROFILE BY DECADE ==========")
    df3 = pd.read_sql_query('''
        SELECT dd.decade,
               ROUND(AVG(ft.danceability), 3) as avg_danceability,
               ROUND(AVG(ft.energy), 3) as avg_energy,
               ROUND(AVG(ft.valence), 3) as avg_valence,
               ROUND(AVG(ft.acousticness), 3) as avg_acousticness,
               ROUND(AVG(ft.tempo), 1) as avg_tempo,
               COUNT(*) as track_count
        FROM fact_tracks ft
        JOIN dim_date dd ON ft.date_id = dd.date_id
        WHERE ft.source = 'kaggle'
        GROUP BY dd.decade
        ORDER BY dd.decade
    ''', conn)
    print(df3.to_string(index=False))
    df3.to_csv(os.path.join(DATA_DIR, 'decade_audio_profile.csv'), index=False)


    print("\n========== ANALYSIS 4: MOOD DISTRIBUTION ==========")
    df4 = pd.read_sql_query('''
        SELECT mood_label, COUNT(*) as count, ROUND(AVG(popularity), 2) as avg_popularity
        FROM fact_tracks
        GROUP BY mood_label
    ''', conn)
    print(df4.to_string(index=False))


    print("\n========== ANALYSIS 5: EXPLICIT VS NON-EXPLICIT ==========")
    df5 = pd.read_sql_query('''
        SELECT explicit,
               ROUND(AVG(popularity), 2) as avg_popularity,
               COUNT(*) as track_count
        FROM fact_tracks
        GROUP BY explicit
    ''', conn)
    print(df5.to_string(index=False))


    print("\n========== ANALYSIS 6: ENERGY vs DANCEABILITY CORRELATION ==========")
    df6 = pd.read_sql_query('''
        SELECT energy, danceability FROM fact_tracks WHERE source = 'kaggle' LIMIT 50000
    ''', conn)
    corr = df6['energy'].corr(df6['danceability'])
    print(f"Correlation between energy and danceability: {corr:.4f}")

    if corr > 0.5:
        print("Strong positive correlation - energetic songs tend to be more danceable")
    elif corr > 0.2:
        print("Moderate positive correlation")
    else:
        print("Weak or no significant correlation")

    conn.close()
    print("\nCSV files saved to data/ folder")




if __name__ == '__main__':
    run_analysis()

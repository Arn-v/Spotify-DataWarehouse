import sqlite3
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import DB_PATH, ROOT

CHARTS_DIR = os.path.join(ROOT, 'data', 'charts')


def save(filename):
  path = os.path.join(CHARTS_DIR, filename)
  plt.savefig(path, bbox_inches='tight')
  plt.close()
  print(f"Saved: {path}")




def run_visualize():

    os.makedirs(CHARTS_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Chart 1 - top 10 artists by avg popularity
    df = pd.read_sql_query('''
        SELECT da.artist_name, ROUND(AVG(ft.popularity), 2) as avg_popularity, COUNT(*) as track_count
        FROM fact_tracks ft
        JOIN dim_artists da ON ft.artist_id = da.artist_id
        WHERE ft.source = 'kaggle'
        GROUP BY da.artist_name
        HAVING track_count > 5
        ORDER BY avg_popularity DESC
        LIMIT 10
    ''', conn)
    df = df.sort_values('avg_popularity')
    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['artist_name'], df['avg_popularity'], color='steelblue')
    ax.bar_label(bars, fmt='%.1f', padding=3)
    ax.set_xlabel('Average Popularity')
    ax.set_title('Top 10 Artists by Average Popularity')
    ax.set_xlim(0, 100)
    plt.tight_layout()
    save('top_artists.png')


    # Chart 2 - top 15 genres by track count
    df_raw = pd.read_sql_query('''
        SELECT da.genres, ft.popularity
        FROM fact_tracks ft
        JOIN dim_artists da ON ft.artist_id = da.artist_id
        WHERE da.genres IS NOT NULL AND da.genres != '' AND da.genres != 'nan'
        AND ft.source = 'kaggle'
    ''', conn)

    rows = []
    for _, row in df_raw.iterrows():
      for g in str(row['genres']).split(','):
        g = g.strip()
        if g:
          rows.append({'genre': g, 'popularity': row['popularity']})

    if len(rows) > 0:
        gdf = pd.DataFrame(rows)
        top =gdf.groupby('genre').agg(track_count=('popularity', 'count')).reset_index()
        top= top[top['track_count'] > 50].sort_values('track_count', ascending=False).head(15)
        top =top.sort_values('track_count')
        fig, ax = plt.subplots(figsize=(10, 7))
        bars = ax.barh(top['genre'], top['track_count'], color='mediumseagreen')
        ax.bar_label(bars, padding=3)
        ax.set_xlabel('Number of Tracks')
        ax.set_title('Top 15 Genres by Track Count')
        plt.tight_layout()
        save('top_genres.png')


    # Chart 3 - music trends across decades
    df = pd.read_sql_query('''
        SELECT dd.decade,
               AVG(ft.danceability) as danceability,
               AVG(ft.energy) as energy,
               AVG(ft.valence) as valence
        FROM fact_tracks ft
        JOIN dim_date dd ON ft.date_id = dd.date_id
        WHERE ft.source = 'kaggle' AND dd.decade >= 1920
        GROUP BY dd.decade
        ORDER BY dd.decade
    ''', conn)
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(df['decade'], df['energy'],       marker='o', label='Energy',         color='tomato')
    ax.plot(df['decade'], df['danceability'], marker='s', label='Danceability',   color='steelblue')
    ax.plot(df['decade'], df['valence'],      marker='^', label='Valence (Mood)', color='mediumseagreen')
    ax.set_xlabel('Decade')
    ax.set_ylabel('Average Score (0-1)')
    ax.set_title('Music Trends Across Decades')
    ax.legend()
    ax.set_xticks(df['decade'])
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    save('decade_trends.png')


    # Chart 4 - mood distribution pie chart
    df = pd.read_sql_query('''
        SELECT mood_label, COUNT(*) as count
        FROM fact_tracks
        GROUP BY mood_label
    ''', conn)

    # build color list manually
    chart_colors = []
    for m in df['mood_label']:
        if m == 'happy':
            chart_colors.append('#f9c74f')
        elif m == 'neutral':
            chart_colors.append('#90be6d')
        elif m == 'sad':
            chart_colors.append('#577590')
        else:
            chart_colors.append('grey')

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(df['count'], labels=df['mood_label'], autopct='%1.1f%%',
           colors=chart_colors, startangle=140)
    ax.set_title('Track Mood Distribution')
    save('mood_distribution.png')


    # Chart 5 - explicit vs clean avg popularity
    df = pd.read_sql_query('''
        SELECT explicit, ROUND(AVG(popularity), 2) as avg_popularity, COUNT(*) as track_count
        FROM fact_tracks
        GROUP BY explicit
    ''', conn)
    df['label'] = df['explicit'].map({0: 'Clean', 1: 'Explicit'})
    fig, ax = plt.subplots(figsize=(7, 5))
    bars = ax.bar(df['label'], df['avg_popularity'], color=['#4ecdc4', '#ff6b6b'], width=0.4)
    ax.bar_label(bars, fmt='%.1f', padding=3)
    ax.set_ylabel('Average Popularity')
    ax.set_title('Avg Popularity: Explicit vs Clean Tracks')
    ax.set_ylim(0, 80)
    plt.tight_layout()
    save('explicit_vs_clean.png')


    # Chart 6 - energy vs danceability scatter (3000 sample)
    df = pd.read_sql_query('''
        SELECT energy, danceability FROM fact_tracks
        WHERE source = 'kaggle' AND energy IS NOT NULL AND danceability IS NOT NULL
        LIMIT 3000
    ''', conn)
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(df['energy'], df['danceability'], alpha=0.3, s=10, color='mediumpurple')
    ax.set_xlabel('Energy')
    ax.set_ylabel('Danceability')
    ax.set_title('Energy vs Danceability (3000 tracks)')
    ax.grid(linestyle='--', alpha=0.4)
    corr = df['energy'].corr(df['danceability'])
    corr_text = 'Correlation: ' + str(round(corr, 3))
    ax.text(0.05, 0.92, corr_text, transform=ax.transAxes,
            fontsize=10, bbox=dict(boxstyle='round', facecolor='white', alpha=0.7))
    plt.tight_layout()
    save('energy_vs_dance.png')

    conn.close()
    print(f"\nAll charts saved to: {CHARTS_DIR}")




if __name__ == '__main__':
    run_visualize()

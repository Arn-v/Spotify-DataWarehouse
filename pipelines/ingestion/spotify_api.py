import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime


def get_spotify_client():
  # setting up auth with client credentials flow
  auth = SpotifyClientCredentials(
    client_id=SPOTIPY_CLIENT_ID,
    client_secret=SPOTIPY_CLIENT_SECRET
  )
  sp = spotipy.Spotify(auth_manager=auth)
  return sp




def search_tracks(sp, query, limit=10):
    tracks = []
    try:
        results = sp.search(q=query, type='track', limit=limit)

        for t in results['tracks']['items']:
            if t is None:
                continue

            # build artist names string manually
            names = []
            for a in t['artists']:
                names.append(a['name'])
            artist_names = ', '.join(names)

            # build the track dict step by step
            track_info = {}
            track_info['id']           = t['id']
            track_info['name']         = t['name']
            track_info['artist_names'] = artist_names
            track_info['album_name']   = t['album']['name']
            track_info['album_id']     = t['album']['id']
            track_info['popularity']   = t.get('popularity', 0)
            track_info['duration_ms']  = t['duration_ms']
            track_info['release_date'] = t['album'].get('release_date', '')
            track_info['source_label'] = query

            if t['explicit'] == True:
                track_info['explicit'] = 1
            else:
                track_info['explicit'] = 0

            tracks.append(track_info)

    except Exception as e:
        print(f"Search failed for '{query}': {e}")
    return tracks




def fetch_trending_tracks():
    sp = get_spotify_client()

    # these search queries help pull trending/popular tracks
    search_queries = [
        "top hits 2025",
        "trending songs 2025",
        "best songs 2024",
        'popular music 2025',
        "new music 2025"
    ]

    all_tracks = []
    playlist_track_map = []
    seen_ids = set()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for query in search_queries:
        print(f"Searching: {query}")
        tracks = search_tracks(sp, query)

        for t in tracks:
            tid = t.get('id')
            if not tid or tid in seen_ids:
                continue
            seen_ids.add(tid)
            t['ingested_at'] = now
            all_tracks.append(t)
            playlist_track_map.append((tid, query))

    print(f"Total unique tracks fetched: {len(all_tracks)}")
    return all_tracks, playlist_track_map




if __name__ == '__main__':
    tracks, mapping = fetch_trending_tracks()
    for t in tracks[:3]:
        print(t)

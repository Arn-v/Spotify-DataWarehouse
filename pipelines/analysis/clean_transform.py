import pandas as pd
import sys
import os
import ast

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import KAGGLE_CSV


# artists column in csv is stored as a python list string - need to parse it
def clean_artists(val):
  try:
    parsed = ast.literal_eval(val)
    if isinstance(parsed, list):
      if len(parsed) > 0:
        return parsed[0]
  except:
    pass
  # if parsing didn't work just clean the string manually
  cleaned = str(val)
  cleaned = cleaned.strip("[]'\" ")
  return cleaned




def normalize_date(val):
  val = str(val).strip()
  # some tracks only have year like "1975"
  if len(val) == 4:
    return val + "-01-01"
  return val




def get_mood_label(v):
  if v > 0.6:
    return 'happy'
  elif v < 0.4:
    return 'sad'
  return 'neutral'




def get_tempo_category(t):
  if t < 90:
    return 'slow'
  elif t > 140:
    return 'fast'
  return 'mid'




def get_cleaned_df():
    print("Reading CSV...")
    df = pd.read_csv(KAGGLE_CSV)
    print(f"Before cleaning: {df.shape}")

    df = df.drop_duplicates(subset='id')
    df = df[df['id'].notna()]

    df['artists'] = df['artists'].apply(clean_artists)

    # remove rows with invalid values
    df = df[df['popularity'].between(0, 100)]
    df = df[df['tempo'] > 0]
    df = df[df['duration_ms'] >= 10000]

    numeric_cols = ['danceability', 'energy', 'loudness', 'speechiness',
                    'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    df['release_date'] = df['release_date'].apply(normalize_date)

    # explicit column type varies across datasets
    if df['explicit'].dtype == object:
        df['explicit'] = df['explicit'].map({'True': 1, 'False': 0, True: 1, False: 0})
        df['explicit'] = df['explicit'].fillna(0)
        df['explicit'] = df['explicit'].astype(int)
    else:
        df['explicit'] = df['explicit'].fillna(0)
        df['explicit'] = df['explicit'].astype(int)

    if 'year' not in df.columns:
        df['year'] = df['release_date'].str[:4].astype(int)

    df['decade']         = (df['year'] // 10) * 10
    df['is_high_energy'] = (df['energy'] > 0.7).astype(int)
    df['is_danceable']   = (df['danceability'] > 0.7).astype(int)

    df['mood_label']     = df['valence'].apply(get_mood_label)
    df['tempo_category'] = df['tempo'].apply(get_tempo_category)

    print(f"After cleaning: {df.shape}")
    return df




if __name__ == '__main__':
    df = get_cleaned_df()
    print(df.head())
    print(df.dtypes)

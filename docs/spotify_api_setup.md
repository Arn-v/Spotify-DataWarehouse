# Spotify API Setup Guide

## 1. Create a Spotify Account

If you don't have one, sign up at [spotify.com](https://www.spotify.com).

## 2. Register a Developer App

1. Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Log in with your Spotify account
3. Click **"Create App"**
4. Fill in:
   - **App name**: `Spotify Data Warehouse`
   - **App description**: `Data engineering project`
   - **Redirect URI**: `http://localhost:8888/callback` (required but not used)
5. Check the **Web API** checkbox
6. Accept the terms and click **Save**

## 3. Get Your Credentials

1. Open your newly created app
2. Click **"Settings"**
3. You'll see:
   - **Client ID** — copy this
   - **Client Secret** — click "View client secret" and copy it

## 4. Configure the Project

Copy the `.env.example` file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env`:
```
SPOTIFY_CLIENT_ID=your_actual_client_id
SPOTIFY_CLIENT_SECRET=your_actual_client_secret
```

## 5. Verify Access

```bash
python scripts/run_ingestion.py
```

If successful, you'll see tracks being extracted. If you see `403` errors for certain endpoints, the project handles this gracefully — it will skip unavailable endpoints and use the ones that work.

## Rate Limits

The Spotify API has rate limits (~180 requests/minute). The project includes a built-in rate limiter that automatically throttles requests to stay within limits.

## Important Notes

- **Never commit your `.env` file** — it's in `.gitignore`
- The project uses **Client Credentials** flow (no user login needed)
- Some endpoints may be restricted for new developer apps — the project gracefully degrades
- Free Spotify accounts work fine for API access

# Spotify Data Warehouse

[![CI](https://github.com/YOUR_USERNAME/spotify-data-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/spotify-data-warehouse/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-style data engineering project that builds a **star schema data warehouse** from Spotify data, runs **marketing analytics pipelines**, and serves insights through an **interactive Streamlit dashboard**.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│  Kaggle Dataset  │     │  Spotify Web API │
│  (Historical)    │     │  (Live Data)     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│          ETL Pipelines (OOP)            │
│  Extractors → Transformers → Loaders   │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│     PostgreSQL Data Warehouse           │
│     (Star Schema + Fact Tables)         │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│        Analytics Pipelines              │
│  Trending │ Genre │ Audio Mood Profiles │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│       Streamlit Dashboard (Plotly)      │
│  Overview │ Trending │ Genre │ Moods   │
└─────────────────────────────────────────┘
```

## Features

- **Dual data sources** — Historical Kaggle dataset + live Spotify Web API ingestion
- **Star schema DW** — Dimension tables, bridge tables, fact tables with proper normalization
- **OOP ETL pipelines** — Abstract base classes for Extractors, Transformers, Loaders, and Pipelines
- **Marketing analytics** — Trending tracks, genre analysis, audio mood profiling (K-means clustering)
- **Full observability** — Structured JSON logging, pipeline run history table, per-endpoint status tracking
- **Graceful degradation** — API extractor skips unavailable endpoints without crashing
- **Scheduled ingestion** — APScheduler-based daemon for periodic data pulls
- **Interactive dashboard** — Streamlit + Plotly with dark theme, drill-down charts, and filters
- **CI/CD** — GitHub Actions with lint + test (Postgres service container)
- **80%+ test coverage** — Unit tests (mocked) + integration tests (real Postgres)

## Tech Stack

Python 3.11+ · PostgreSQL 16 · SQLAlchemy 2.0 · Alembic · spotipy · pandas · scikit-learn · APScheduler · Streamlit · Plotly · pytest · ruff · Docker

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- (Optional) Spotify Developer account for live API ingestion

### Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/spotify-data-warehouse.git
cd spotify-data-warehouse

# Start PostgreSQL
docker-compose up -d

# Install dependencies
pip install -e ".[all]"

# Configure environment
cp .env.example .env
# Edit .env with your Spotify credentials (optional)

# Run database migrations
alembic upgrade head

# Load Kaggle data
python scripts/seed_kaggle.py --file data/raw/spotify_tracks.csv

# Run analytics
python scripts/run_analytics.py

# Launch dashboard
streamlit run dashboard/app.py
```

### Spotify API Setup (Optional)

See [docs/spotify_api_setup.md](docs/spotify_api_setup.md) for instructions on obtaining API credentials.

```bash
# Run one-off ingestion
python scripts/run_ingestion.py

# Or start the scheduler daemon
python scripts/run_scheduler.py
```

## Project Structure

```
src/spotify_dw/
├── models/          # SQLAlchemy ORM (star schema)
├── extractors/      # Data extraction (Spotify API, Kaggle CSV)
├── transformers/    # Data cleaning & transformation
├── loaders/         # Database loading (upsert)
├── pipelines/       # Pipeline orchestration
├── analytics/       # Marketing analytics (trending, genre, mood)
├── db/              # Database session + Alembic migrations
├── scheduler/       # APScheduler job definitions
└── utils/           # Rate limiter, validators
```

## Testing

```bash
# Run all tests
make test

# Unit tests only
make test-unit

# Lint
make lint
```

## License

[MIT](LICENSE)

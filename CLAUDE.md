# Spotify Data Warehouse

Production-style star schema data warehouse built from Spotify data (Kaggle CSV + live API), with marketing analytics pipelines and an interactive Streamlit dashboard.

## Quick Start

```bash
make dev                # Install all dependencies (editable + dev)
make db                 # Start PostgreSQL 16 container (port 5433)
make migrate            # Run Alembic migrations
make seed               # Load Kaggle CSV into warehouse
make ingest             # One-off Spotify API ingestion
make analytics          # Run analytics (trending, genre, mood)
make dashboard          # Launch Streamlit dashboard
make test               # Run all tests with coverage
make lint               # ruff check + format --check
make format             # Auto-format with ruff
```

## Project Layout

```
src/spotify_dw/          # Main package
  extractors/            # Data extraction (Spotify API, Kaggle CSV)
  transformers/          # Data cleaning & normalization
  loaders/               # Database loading (upsert dimensions, append facts)
  pipelines/             # ETL orchestration (ingestion, kaggle_load, analytics)
  analytics/             # Marketing analyzers (trending, genre, audio profiles)
  models/                # SQLAlchemy ORM (star schema)
  db/                    # SessionFactory + Alembic migrations
  scheduler/             # APScheduler job definitions
  utils/                 # Rate limiter, validators
config/                  # Pydantic settings + logging config
scripts/                 # CLI entry points (run_ingestion, run_analytics, seed_kaggle, run_scheduler)
dashboard/               # Streamlit app + pages (overview, trending, genre, mood)
tests/                   # Unit (SQLite) + integration (PostgreSQL)
```

## Data Flow

```
Spotify API / Kaggle CSV
  -> Extractors (with graceful degradation + rate limiting)
  -> Transformers (normalize, deduplicate)
  -> Loaders (upsert dims, append facts)
  -> Analyzers (trending velocity, genre stats, K-means clustering)
  -> Streamlit Dashboard (4 pages with Plotly charts)
```

## Database

- **Connection:** `postgresql://spotify:spotify@localhost:5433/spotify_dw` (port 5433, NOT 5432)
- **Schema:** 5 dimensions (`dim_track`, `dim_artist`, `dim_album`, `dim_genre`, `dim_date`) + 2 bridge tables + 2 fact tables + 3 aggregate tables + `pipeline_run_log`
- **Session:** Always use `SessionFactory.get_session()` context manager

## Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `SessionFactory` | `db/session.py` | Singleton DB session with connection pooling |
| `PipelineResult` | `pipelines/base.py` | Dataclass returned by every pipeline run |
| `TokenBucketRateLimiter` | `utils/rate_limiter.py` | 25 calls/30s for Spotify API |
| `SpotifyAPIExtractor` | `extractors/spotify_api.py` | Graceful degradation on endpoint failures |
| `IngestionPipeline` | `pipelines/ingestion.py` | API -> warehouse (scheduled every 6h) |
| `AnalyticsPipeline` | `pipelines/analytics.py` | Warehouse -> aggregate tables |
| `AudioProfileAnalyzer` | `analytics/audio_profile.py` | K-means mood clustering |

## Available Skills & Agents

- `/add-analyzer` — Scaffold a new analytics analyzer with tests
- `/add-dashboard-page` — Create a new Streamlit dashboard page
- `/refactor` — Safe refactoring with contract preservation
- `pipeline-debugger` agent — Debug ETL failures
- `dashboard-builder` agent — Build dashboard pages
- `code-reviewer` agent — Review code quality

## CI/CD

GitHub Actions on push/PR to `main`:
- **Lint job:** `ruff check` + `ruff format --check`
- **Test job:** Unit tests (SQLite) + integration tests (PostgreSQL 16 service container)

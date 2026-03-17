# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Setup
pip install -e ".[all]"          # Install all dependencies (dev + dashboard + notebooks)
docker-compose up -d             # Start PostgreSQL 16 (exposed on port 5433 to avoid conflicts with local Postgres)
alembic upgrade head             # Run database migrations

# Pipelines
python scripts/seed_kaggle.py --file data/raw/dataset.csv   # Load historical Kaggle data
python scripts/run_ingestion.py   # One-off Spotify API ingestion
python scripts/run_analytics.py   # Run analytics pipeline
python scripts/run_scheduler.py   # Start APScheduler daemon (ingestion + analytics every 6h)

# Dashboard
python -m streamlit run dashboard/app.py --server.headless true

# Testing
pytest tests/ -v --cov=src/spotify_dw --cov-report=term-missing   # All tests with coverage
pytest tests/unit/ -v                                               # Unit tests only
pytest tests/integration/ -v -m integration                         # Integration tests only
pytest tests/unit/test_transformers.py::TestTrackTransformer -v     # Single test class

# Linting
ruff check src/ tests/           # Lint
ruff format src/ tests/          # Auto-format
ruff format --check src/ tests/  # Check formatting without changing
```

## Known Issues & Workarounds

- **Port 5433**: Docker Postgres is mapped to port 5433 (not default 5432) because the dev machine has a local PostgreSQL installation occupying 5432. All config files (docker-compose.yml, alembic.ini, config/settings.py, .env) reflect this.
- **Streamlit first run**: Requires `--server.headless true` flag or pre-configured `~/.streamlit/credentials.toml` to skip the email prompt.
- **Name column lengths**: dim_track.track_name, dim_artist.artist_name, and dim_album.album_name are `String(1000)` — some Kaggle entries exceed 500 chars.
- **Kaggle CSV filename**: The actual file is `data/raw/dataset.csv` (not `spotify_tracks.csv` as referenced in some docs).

## Architecture

This is a Spotify Data Warehouse with two ETL pipelines and a star-schema PostgreSQL database.

### Two Pipelines

1. **Ingestion Pipeline** (`pipelines/ingestion.py`): Spotify API → Extract (3 endpoints with graceful degradation) → Deduplicate → Transform → Load into dimension/fact tables
2. **Analytics Pipeline** (`pipelines/analytics.py`): Read facts → TrendingAnalyzer (7-day velocity) + GenreAnalyzer (genre stats) + AudioProfileAnalyzer (K-means clustering) → Write aggregate tables

### OOP Hierarchy

All pipeline components follow abstract base class patterns in `src/spotify_dw/`:

- `extractors/base.py` → `BaseExtractor` (enforces `extract()`, `validate_source()`)
- `transformers/base.py` → `BaseTransformer` (enforces `transform(df)`, provides `_handle_nulls()`)
- `loaders/base.py` → `BaseLoader` (enforces `load(df)`, provides pre/post checks)
- `pipelines/base.py` → `BasePipeline` (enforces `run()`, wraps with `_setup/_teardown/_on_failure/_log_run`)
- `analytics/base.py` → `BaseAnalyzer` (enforces `analyze(df)`, provides `_validate_input/_export`)

When adding new components, subclass the appropriate base class.

### Database Schema (Star Schema)

- **Dimensions**: dim_track, dim_artist, dim_album, dim_genre, dim_date
- **Bridges**: bridge_track_artist, bridge_artist_genre (many-to-many)
- **Facts**: fact_audio_features, fact_track_popularity (append-only time series)
- **Aggregates**: agg_trending_tracks, agg_genre_stats, agg_audio_profiles
- **Observability**: pipeline_run_log (every pipeline execution logged with status, row counts, errors)

Models are in `src/spotify_dw/models/`. All dimensions use `TimestampMixin` for created_at/updated_at.

### Key Patterns

- **Graceful degradation**: `SpotifyAPIExtractor` tries 3 API endpoints independently; partial failures don't kill the pipeline
- **Upsert loading**: `PostgresLoader` uses `INSERT ... ON CONFLICT DO UPDATE` for dimensions, plain INSERT for append-only facts
- **SessionFactory singleton** (`db/session.py`): Lazy-init, context manager with auto-commit/rollback
- **Token-bucket rate limiter** (`utils/rate_limiter.py`): Thread-safe, 25 calls/30s default
- **PipelineResult dataclass**: Every pipeline returns status, row counts, errors, duration, endpoint statuses

### Config

Settings loaded from `.env` via Pydantic (`config/settings.py`). Key vars: `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, `DATABASE_URL`, `BATCH_SIZE`, `SCHEDULER_INTERVAL_HOURS`.

### CI

GitHub Actions (`.github/workflows/ci.yml`): ruff lint → unit tests with coverage → integration tests against PostgreSQL 16 service container. Python 3.11, ruff rules: E, F, I, N, W, UP. Line length: 120.

### Dashboard

Streamlit multi-page app in `dashboard/`. Pages: Overview (KPIs), Trending, Genre Analysis, Mood Profiles. Charts built with Plotly (`dashboard/components/charts.py`).

## Testing Notes

- Unit tests use in-memory SQLite via fixtures in `tests/conftest.py`
- Integration tests require a running PostgreSQL instance and `DATABASE_URL` env var
- The `@pytest.mark.integration` marker separates integration tests
- `responses` library is used to mock HTTP calls in unit tests

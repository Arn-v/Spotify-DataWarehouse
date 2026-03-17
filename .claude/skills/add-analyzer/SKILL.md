---
name: add-analyzer
description: Use this skill when creating a new analytics analyzer. Triggers on "add analyzer", "new analyzer", "create analysis", or any request to build a new analytical component for the analytics pipeline.
---

# Add New Analyzer

Follow these steps to create a new analyzer that plugs into the analytics pipeline.

## Step 1: Scaffold the analyzer

Create a new file in `src/spotify_dw/analytics/` that subclasses `BaseAnalyzer`:

```python
from spotify_dw.analytics.base import BaseAnalyzer

class NewAnalyzer(BaseAnalyzer):
    """One-line description of what this analyzer does."""

    def analyze(self, df):
        """Core analysis logic. Receives a DataFrame, returns results."""
        self._validate_input(df)
        # ... analysis logic ...
        return results

    def _export(self, results):
        """Write results to the appropriate agg_ table."""
        # Use SessionFactory and upsert pattern
        pass
```

## Step 2: Create the aggregate table

1. Add a new SQLAlchemy model in `src/spotify_dw/models/` following existing `agg_` table patterns
2. Create an Alembic migration: `alembic revision --autogenerate -m "add agg_<name> table"`
3. Apply: `alembic upgrade head`

## Step 3: Wire into the analytics pipeline

Edit `src/spotify_dw/pipelines/analytics.py`:
- Import the new analyzer
- Add it to the analyzer list in the pipeline's `run()` method
- It should follow the same pattern as TrendingAnalyzer, GenreAnalyzer, AudioProfileAnalyzer

## Step 4: Add tests

Create `tests/unit/test_<analyzer_name>.py`:
- Test `analyze()` with mock DataFrames
- Test edge cases (empty df, missing columns, nulls)
- Test `_export()` with SQLite fixtures from `tests/conftest.py`

Run: `pytest tests/unit/test_<analyzer_name>.py -v`

## Step 5: Add a dashboard page (optional)

If the analyzer produces user-facing insights, create a page in `dashboard/pages/` and chart functions in `dashboard/components/charts.py`.
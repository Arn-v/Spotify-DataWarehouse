# Testing Rules

- Unit tests use in-memory SQLite via fixtures in `tests/conftest.py` — not PostgreSQL
- Integration tests require running PostgreSQL and `DATABASE_URL` env var
- Mark all integration tests with `@pytest.mark.integration`
- Mock all HTTP calls with the `responses` library — never hit real Spotify API in tests
- Test command: `pytest tests/ -v --cov=src/spotify_dw --cov-report=term-missing`
- Every new component (extractor, transformer, loader, analyzer) must have corresponding unit tests
- Test file naming: `tests/unit/test_<module_name>.py`
- Never skip or xfail tests without a comment explaining why
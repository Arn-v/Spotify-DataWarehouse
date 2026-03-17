# Database Rules

- PostgreSQL 16 on port 5433 (NOT 5432) — always verify connection strings
- All schema changes go through Alembic migrations, never raw DDL
- Dimensions use `INSERT ... ON CONFLICT DO UPDATE` (upsert pattern)
- Facts are append-only — never update or delete fact rows
- All dimension tables must include `TimestampMixin` (created_at, updated_at)
- Use `SessionFactory.get_session()` context manager for all DB operations — never create raw engines or sessions
- String columns for names (`track_name`, `artist_name`, `album_name`) are `String(1000)` — do not reduce this
- Always commit via context manager auto-commit; never call `session.commit()` manually outside the factory pattern
# Pipeline Architecture Rules

- **OOP is mandatory across the entire codebase** — no standalone functions or procedural scripts. Every piece of logic must live inside a class. Utilities, helpers, services, pipeline components — all classes. If you're writing a function, it belongs as a method on a class.
- All new components MUST subclass the appropriate base class:
  - Extractors → `extractors/base.py::BaseExtractor`
  - Transformers → `transformers/base.py::BaseTransformer`
  - Loaders → `loaders/base.py::BaseLoader`
  - Pipelines → `pipelines/base.py::BasePipeline`
  - Analyzers → `analytics/base.py::BaseAnalyzer`
- Never bypass the base class — implement all enforced abstract methods
- Pipeline lifecycle is always: `_setup` → `run()` → `_teardown` (or `_on_failure`) → `_log_run`
- Every pipeline run must return a `PipelineResult` dataclass with status, row counts, errors, duration
- Every pipeline run must be logged to `pipeline_run_log` table
- Spotify API calls must go through the token-bucket rate limiter (25 calls/30s)
- Extractors must support graceful degradation — partial endpoint failures should not kill the full pipeline
- Config values come from Pydantic settings (`config/settings.py`) loaded from `.env` — no hardcoded secrets or connection strings
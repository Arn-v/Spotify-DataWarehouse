---
name: refactor
description: Use this skill when refactoring existing code. Triggers on "refactor", "clean up", "improve code", "restructure", "simplify", or when improving code quality without changing behavior.
---

# Safe Refactoring Workflow

## Step 1: Understand before changing

1. Read the file and its base class to understand the contract
2. `grep -r "from.*import.*ClassName" src/` — find all usages
3. `grep -r "ClassName" tests/` — find all tests covering this code
4. Run existing tests to establish a green baseline:
   ```bash
   pytest tests/ -v --tb=short 2>&1 | tail -20
   ```

## Step 2: Refactor rules

- **Never change the public interface** of base class methods (`extract`, `transform`, `load`, `run`, `analyze`) without updating all subclasses
- **Preserve the upsert pattern** — dimensions use `INSERT ... ON CONFLICT DO UPDATE`, facts use plain INSERT
- **Keep graceful degradation** — if the extractor handles partial failures, the refactored version must too
- **Respect SessionFactory singleton** — don't create new engines or sessions outside the factory
- **Keep config in Pydantic settings** — no hardcoded values for batch sizes, intervals, ports, credentials

## Step 3: Validate

After refactoring:
```bash
ruff check src/ tests/
ruff format --check src/ tests/
pytest tests/unit/ -v
pytest tests/integration/ -v -m integration  # if DB changes involved
```

All tests must pass. If a test fails, the refactor introduced a regression — fix it before committing.

## Step 4: Document

If the refactor changes any pattern or convention, update the relevant section in CLAUDE.md so future sessions are aware.
# Code Style & Conventions

- Line length: 120 characters max
- Linter: ruff with rules E, F, I, N, W, UP
- Always run `ruff check` and `ruff format --check` before considering any code change complete
- Use type hints on all function signatures
- Docstrings: Google style, required on all public methods
- Imports: isort-compatible ordering (stdlib → third-party → local), enforced by ruff's I rules
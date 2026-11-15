# Contributing to cachery

`uv` is the **only** supported tool for environment, dependency, and build
management on this project — no `pip`/`poetry`/`conda` instructions are maintained.

## One-time setup

```bash
uv python pin 3.10               # match the minimum supported version
uv sync --all-extras --all-groups  # core + all extras + dev tools into .venv
pre-commit install
```

## Day-to-day development

```bash
uv run pytest                            # run unit tests
uv run pytest -m integration             # integration tests (needs Docker)
uv run pytest --cov=cachery --cov-report=term-missing --cov-fail-under=90
uv run mypy --strict src/cachery
uv run pyright --strict src/cachery
uv run ruff check . && uv run ruff format --check .
```

## Adding a dependency — ALWAYS gate it first

```bash
echo 'newdep = "owner/repo"' >> deps.toml
uv run python scripts/freshness_gate.py deps.toml --max-age-days 30 --token "$GITHUB_TOKEN"
uv add --optional speed newdep           # only after the gate passes
```

The freshness gate fails if the dependency's GitHub repo has had no push in the
last 30 days. CI re-runs it weekly and files an issue when a dependency goes stale.

## Release

```bash
uv build      # produces dist/*.whl and dist/*.tar.gz
uv publish    # trusted publishing to PyPI (CI-only, OIDC)
```

## Ground rules

1. `src/cachery/` core must have **zero third-party runtime dependencies**.
   Optional extras are imported lazily with a clear `ImportError` naming the extra.
2. Every module ships with tests in the same change; keep coverage ≥ 90%.
3. `mypy --strict` and `pyright --strict` must pass.
4. Follow the existing `src/cachery/` module layout and keep the docs in `docs/` current.

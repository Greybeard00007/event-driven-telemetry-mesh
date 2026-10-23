.RECIPEPREFIX = >
.PHONY: setup up down logs test bench tui lint fmt security clean

setup:
> cp -n .env.example .env || true
> pip install -e ".[dev]"

up:
> docker compose up -d --build

down:
> docker compose down

logs:
> docker compose logs -f

test:
> pytest -v

bench:
> python scripts/benchmark_pipeline.py

tui:
> python scripts/telemetry_tui.py

lint:
> ruff check . || true

fmt:
> ruff format . || true

security:
> gitleaks detect --no-git -v || true

clean:
> find . -type d -name "__pycache__" -exec rm -rf {} +
> find . -type f -name "*.pyc" -delete
> rm -rf .pytest_cache .coverage htmlcov/

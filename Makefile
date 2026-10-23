.RECIPEPREFIX = >
.PHONY: setup up down logs test bench tui lint fmt security clean

VENV ?= .venv
PYTHON ?= $(shell if [ -f $(VENV)/bin/python ]; then echo $(VENV)/bin/python; else echo python3; fi)
PYTEST ?= $(shell if [ -f $(VENV)/bin/pytest ]; then echo $(VENV)/bin/pytest; else echo pytest; fi)

setup:
> cp -n .env.example .env || true
> $(PYTHON) -m pip install --upgrade pip
> $(PYTHON) -m pip install -e ".[dev]"

up:
> docker compose up -d --build

down:
> docker compose down

logs:
> docker compose logs -f

test:
> $(PYTEST) -v

bench:
> $(PYTHON) scripts/benchmark_pipeline.py

tui:
> $(PYTHON) scripts/telemetry_tui.py

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

# Development

## Setup

```bash
cp -n .env.example .env || true
make setup
```

## Common tasks

```bash
make up         # Start full multi-container stack (Redis, Timescale, Ollama, API, Workers, Grafana)
make down       # Stop all running containers
make logs       # Tail real-time service logs
make test       # Run complete pytest test suite (46 tests)
make bench      # Run pipeline throughput and latency benchmarks
make tui        # Launch ANSI terminal mission control
make lint       # Run ruff code linter
make fmt        # Auto-format codebase
make security   # Run gitleaks secret scanner
make clean      # Clean up caches and bytecode
```

## Adding dependencies

1. Add package requirements to `requirements.txt` (or dev dependencies to `pyproject.toml`).
2. Run `pip install -e ".[dev]"`.

## Testing & Quality

All changes must pass:
```bash
pytest -v
```
All 46 unit and integration tests run in under 2 seconds without requiring external network dependencies.

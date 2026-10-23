# Container Infrastructure (`docker/`)

This directory houses the container orchestration specifications, Dockerfiles, and runtime configurations for the Event-Driven Telemetry Mesh.

## Architecture

The containerized mesh comprises 7 coordinated services operating over an isolated bridge network (`mesh-net`):

```
+-------------------------------------------------------------------------+
|                               mesh-net                                  |
|                                                                         |
|  +-------------+       +---------------+       +---------------------+  |
|  |    Redis    |<------|   Ingestion   |<------|   Multi-Rig Sim     |  |
|  |   Streams   |       |   API Server  |       |   & WITS0 Parser    |  |
|  +------+------+       +-------+-------+       +---------------------+  |
|         |                      |                                        |
|         v                      v                                        |
|  +-------------+       +---------------+       +---------------------+  |
|  |   Stream    |------>|  TimescaleDB  |<------|       Grafana       |  |
|  |   Workers   |       |  Hypertables  |       |   Mission Control   |  |
|  +------+------+       +---------------+       +----------+----------+  |
|         |                                                 |             |
|         v                                                 v             |
|  +-------------+                               +---------------------+  |
|  |   Ollama    |                               |     Prometheus      |  |
|  |  LLM Pool   |                               |       Scraper       |  |
|  +-------------+                               +---------------------+  |
+-------------------------------------------------------------------------+
```

## Services

| Service | Port | Description | Health Check |
|---|---|---|---|
| `redis` | 6379 | Redis 7.4 Streams broker for real-time sensor buffers | `redis-cli ping` |
| `timescaledb` | 5432 | PostgreSQL 16 with TimescaleDB hypertables | `pg_isready -U postgres` |
| `ollama` | 11434 | Local LLM inference engine running `deepseek-r1:8b` | Process state |
| `ingestion-service` | 8000, 9191 | FastAPI REST/WebSocket API + WITS0 TCP receiver | HTTP `/health` |
| `stream-worker-1` | - | Asynchronous micro-batch consumer worker node 1 | Consumer heartbeat |
| `stream-worker-2` | - | Asynchronous micro-batch consumer worker node 2 | Consumer heartbeat |
| `prometheus` | 9090 | Metrics collection scraping `/metrics` every 5s | HTTP `/status` |
| `grafana` | 3000 | Operational Mission Control dashboards | HTTP `localhost:3000` |

## Quickstart

### Launch Full Cluster
```bash
docker compose up -d --build
```

### Launch Development Stack (with Live Reloading)
```bash
docker compose -f docker-compose.yml -f docker/docker-compose.dev.yml up -d
```

### View Live Aggregated Logs
```bash
docker compose logs -f
```

### Teardown Cluster
```bash
docker compose down -v
```

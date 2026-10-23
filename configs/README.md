# Configuration Directory (`configs/`)

This directory contains configuration templates, infrastructure schemas, and telemetry mesh operational settings.

## Directory Structure

```
configs/
├── telemetry_mesh.env          # Environment variable template for application services
├── prometheus/
│   └── prometheus.yml          # Prometheus metrics scraper job configurations
├── timescaledb/
│   └── init_timescaledb.sql    # TimescaleDB initialization DDL and hypertable schema
└── grafana/
    ├── dashboards/
    │   ├── dashboards.yml      # Grafana dashboard provisioning specification
    │   └── drilling_mission_control.json # Production Mission Control dashboard definition
    └── datasources/
        └── datasource.yml      # Automated TimescaleDB & Prometheus datasource setup
```

## Configuration Sections

### 1. Telemetry Mesh Environment (`telemetry_mesh.env`)
Configures broker connection URIs, TimescaleDB pool sizing, Ollama model parameters, sliding window lengths, and deterministic physics safety thresholds (WOB, RPM, Torque, SPP, Flow).

### 2. Prometheus Scraper (`prometheus/prometheus.yml`)
Configures scrape targets for the `/metrics` endpoint exposed by the FastAPI ingestion service on port 8000.

### 3. TimescaleDB DDL (`timescaledb/init_timescaledb.sql`)
Initializes hypertables partitioned along 1-day chunks for sensor streams and 7-day chunks for enriched anomaly diagnostic events.

### 4. Grafana Dashboards (`grafana/`)
Turnkey Mission Control telemetry visualizations including WOB vs. ROP cross-plots, SPP / Flow delta monitors, real-time stick-slip detection alerts, and active anomaly logs.

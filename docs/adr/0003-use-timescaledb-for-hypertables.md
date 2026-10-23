# ADR 0003: Use TimescaleDB for Partitioned Hypertables

## Status
Accepted

## Context
Drilling sensor telemetry generates millions of rows per day per well. Relational databases degrade query performance as tables grow past millions of records, while standard NoSQL databases lack rich analytical time-bucket aggregation functions.

## Decision
Adopt **TimescaleDB** (PostgreSQL 16 extension):
1. Partition sensor data automatically into **1-day chunk intervals** (`rig_telemetry`).
2. Partition anomaly records into **7-day chunk intervals** (`anomaly_events`).
3. Leverage Continuous Aggregates (`rig_telemetry_1m`) for 1-minute historical downsampled views without table scans.
4. Fallback in-memory storage layer for standalone development and testing environments.

## Consequences
- Fast time-series query execution regardless of historical table scale.
- Native SQL compatibility for business intelligence tools and Grafana datasources.

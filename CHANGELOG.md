# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-10-22
### Added
- High-throughput Redis Streams producer with pipelined ingestion and approximate trimming (`MAXLEN ~ 100,000`).
- Multi-protocol ingestion: WITSML 1.3.1 / 1.4.1.1 XML, high-frequency JSON micro-batches, WITS Level 0 TCP (`:9191`), and ETP v1.2 WebSockets (`/ws/etp`).
- Stateless asynchronous worker nodes with Consumer Groups (`XREADGROUP`) and automatic crash recovery (`XAUTOCLAIM`).
- Dead-Letter Queue (`telemetry:stream:dead-letter`) for poison-pill isolation, inspection, and replay.
- Sliding-window SHA-256 deduplication and idempotency engine.
- Sub-millisecond deterministic physics rule engine for kicks, pack-offs, stick-slip, motor stalls, excessive WOB, and lost circulation.
- Digital twin physics engine: Teale's Mechanical Specific Energy (MSE), Equivalent Circulating Density (ECD), and corrected $d$-exponent ($d_{\text{xc}}$).
- 3D Minimum Curvature Directional Wellbore Trajectory Engine ($\text{North}, \text{East}, \text{TVD}, \text{DLS}$).
- Asynchronous Ollama LLM worker pool with model cascading (SLM fast triage $\rightarrow$ DeepSeek-R1 deep reasoning).
- Vector RAG Knowledge Base grounded in API Standard 53 and IADC well control guidelines.
- TimescaleDB hypertables partitioned into 1-day chunks with continuous 1-minute aggregates.
- Native Prometheus metrics exposition (`/metrics`), turnkey Grafana provisioning (port `3000`), and emergency alert webhooks (Slack, Teams, PagerDuty).
- Real-time Mission Control Web Dashboard with Chart.js dual-axis graphs and compliance audit sign-off modal.
- Standalone ANSI Terminal TUI Mission Control (`scripts/telemetry_tui.py`).
- GitHub Actions CI/CD workflow (`.github/workflows/ci.yml`).
- 40 automated tests with 100% green pass rate.

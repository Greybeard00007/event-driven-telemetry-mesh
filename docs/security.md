# Security

## Principles

- **Local-first** — Oilfield sensor telemetry and AI reasoning models run strictly on owned rig or cloud-edge infrastructure; zero sensor curves leave the perimeter.
- **Private network by default** — Telemetry services bind to internal bridge networks or private VPCs/Tailscale overlays.
- **Least privilege** — Containers execute as unprivileged non-root users (`appuser:1000`).
- **Defense in depth** — Strict physical boundary input validation (`0-150 klbf` WOB, `0-8000 psi` SPP), secrets kept exclusively in `.env` (gitignored), and dead-letter queues isolating poison-pill attacks.
- **Tamper-Evident Well Control Sign-Offs** — Operational driller acknowledgments and mitigations are recorded with SHA-256 cryptographic signatures.

## Network Surface

| Service | Container Port | Exposure | Default Binding |
| :--- | :--- | :--- | :--- |
| **Ingestion API & Dashboard** | `8000` | HTTP / WebSocket | `127.0.0.1:8000` (or private overlay) |
| **WITS Level 0 TCP Listener** | `9191` | Raw TCP socket | Internal rig network / local edge |
| **Redis Streams Broker** | `6379` | Redis protocol | Internal container bridge only |
| **TimescaleDB** | `5432` | PostgreSQL protocol | Internal container bridge only |
| **Ollama Local LLM** | `11434` | HTTP REST API | Internal container bridge only |
| **Prometheus Scraper** | `9090` | HTTP | Internal container bridge |
| **Grafana Dashboard** | `3000` | HTTP | `127.0.0.1:3000` |

## Secret Scanning

`.env` is strictly gitignored. Gitleaks scans are configured in pre-commit and CI workflows.

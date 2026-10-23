# ADR 0001: Use Redis Streams for Telemetry Ingestion Buffer

## Status
Accepted

## Context
High-frequency drilling telemetry (50–100 Hz per well across hundreds of sensors) requires high-throughput, low-latency ingestion. Asynchronous AI worker pools exhibit variable inference latency (100–1500 ms). Direct synchronous coupling or heavyweight broker clusters (e.g. multi-node Kafka with Zookeeper/KRaft) introduce high operational complexity on rig-floor edge servers.

## Decision
Adopt **Redis Streams** as the central streaming backbone:
1. Pipelined batch ingestion (`xadd`) delivering $>9,700$ records/sec sustained throughput.
2. Approximate stream trimming (`MAXLEN ~ 100,000`) for bounded memory usage without $O(N)$ CPU pauses.
3. Consumer Groups (`XREADGROUP`) with explicit acknowledgment (`XACK`) and pending recovery (`XAUTOCLAIM`).
4. Dead-Letter Queue (`telemetry:stream:dead-letter`) for poison-pill isolation.

## Consequences
- Ultra-low latency buffering between rig sensors and inference workers.
- Minimal operational footprint suitable for offshore rig edge servers and Docker Compose.
- At-least-once delivery guarantees with automated worker crash recovery.

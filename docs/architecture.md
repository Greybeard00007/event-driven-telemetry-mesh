# Architecture

> Distributed, event-driven streaming topology decoupling real-time WITSML rig sensor ingestion from asynchronous AI inference workers.

```mermaid
flowchart TD
    subgraph Wellsites ["Offshore & Land Drilling Rigs"]
        R1["Rig Titan\n(WITSML 1.4.1.1 XML)"]
        R2["Rig Orion\n(JSON Batch 100Hz)"]
        R3["Rig Apex\n(Drill Floor MWD/LWD)"]
        R4["Legacy Land Rig\n(WITS Level 0 TCP :9191)"]
        R5["Modern Rig Network\n(ETP v1.2 WebSocket /ws/etp)"]
    end

    subgraph IngestionLayer ["Event Ingestion Broker (FastAPI & Redis Streams)"]
        P_INGEST["WITSML / JSON / ETP Ingestion Endpoints"]
        VALIDATOR["Physical Bounds Validator\n(0-150 klbf WOB, 0-8000 psi SPP)"]
        DEDUP["Sliding-Window Dedup Engine\n(SHA-256 Fingerprint)"]
        QUEUE["Non-Blocking Ingestion Buffer\n(Bounded Async Queue)"]
        P_STREAM["Redis Streams Producer\n(Pipeline xadd MAXLEN ~ 100,000)"]
    end

    subgraph RedisBroker ["Redis 7.4 In-Memory Streaming Broker"]
        STREAM_RAW[("Stream: telemetry:stream:raw")]
        STREAM_ANOM[("Stream: telemetry:stream:anomalies")]
        STREAM_DLQ[("Stream: telemetry:stream:dead-letter")]
    end

    subgraph WorkerPool ["Stateless Asynchronous Worker Nodes"]
        CG["Consumer Group: telemetry:workers:group\n(XREADGROUP > & XAUTOCLAIM)"]
        W1["Worker Node 1\n(Sliding Window Buffer)"]
        W2["Worker Node 2\n(Sliding Window Buffer)"]
        RULES["Tier-1 Deterministic Threshold Engine\n(Kick, Stick-Slip, Pack-Off, Stall)"]
        ML_DET["Streaming ML Drift Detector\n(Welford's EWMA & Z-Score)"]
        RAG["Vector RAG Knowledge Base\n(API RP 53 & IADC SOPs)"]
    end

    subgraph AIWorkerPool ["Ollama AI Worker Pool"]
        SEM["Concurrency Semaphore\n(4 Concurrent Inferences)"]
        OLLAMA["Local Ollama Service\n(deepseek-r1:8b / llama3.2:1b)"]
        FALLBACK["Petroleum Engineering\nDomain Fallback Heuristic"]
    end

    subgraph PersistenceLayer ["TimescaleDB (PostgreSQL 16)"]
        HYPER_RAW[("Hypertable: rig_telemetry\n(1-day chunk intervals)")]
        HYPER_ANOM[("Hypertable: anomaly_events\n(Root-cause & Recommendations)")]
        AUDIT[("Audit Trail\n(SHA-256 Compliance Sign-Offs)")]
        MAT_VIEW["Continuous Aggregate\n(1-minute Downsampled View)"]
    end

    subgraph BroadcastLayer ["Real-Time Operations & Alerts"]
        WS_HUB["WebSocket Broadcast Hub\n(/ws/live)"]
        DASHBOARD["Mission Control Dashboard\n(Chart.js, AI Feed, Acknowledge Modal)"]
        ALERTS["Incident Webhook Dispatcher\n(Slack, Teams, PagerDuty)"]
        PROM["Prometheus & Grafana\n(:9090 & :3000)"]
    end

    Wellsites --> P_INGEST
    P_INGEST --> VALIDATOR
    VALIDATOR --> DEDUP
    DEDUP --> QUEUE
    QUEUE --> P_STREAM
    P_STREAM --> STREAM_RAW

    STREAM_RAW --> CG
    CG --> W1 & W2
    W1 & W2 --> RULES & ML_DET
    W1 & W2 -. "Poison Pills" .-> STREAM_DLQ

    RULES & ML_DET -- "Normal Batch" --> HYPER_RAW
    RULES & ML_DET -- "Anomaly Detected" --> RAG
    RAG --> SEM
    SEM --> OLLAMA
    OLLAMA -. "Offline Fallback" .-> FALLBACK
    OLLAMA & FALLBACK --> STREAM_ANOM
    OLLAMA & FALLBACK --> HYPER_ANOM
    OLLAMA & FALLBACK --> ALERTS

    W1 & W2 -- "XACK msg_id" --> STREAM_RAW
    W1 & W2 --> WS_HUB
    STREAM_ANOM --> WS_HUB
    WS_HUB --> DASHBOARD
    HYPER_RAW --> MAT_VIEW
    DASHBOARD --> AUDIT
    P_INGEST & W1 & W2 --> PROM
```

## Core Principles

1. **Local-First & Air-Gapped by Default**:
   All sensor parsing, physics calculations, time-series storage, and LLM inference run entirely within the perimeter on owned rig or edge-node hardware. Zero telemetry leaves the private network.

2. **Decoupled Asynchronous Streaming**:
   High-frequency 50–100 Hz rig telemetry cannot block on neural network inference. Redis Streams acts as an elastic buffer, isolating ingestion latency from inference latency.

3. **Two-Tier Safety Architecture**:
   - **Tier 1 (Sub-millisecond Physics & ML)**: Deterministic physics thresholds evaluate every single sample in 42 µs ($>21,000$ ev/s).
   - **Tier 2 (AI Root-Cause Reasoning)**: Only anomalous sensor windows are dispatched to the Ollama pool, grounded with vector RAG against API RP 53 well control guidelines.

4. **At-Least-Once Delivery with Idempotency**:
   Redis Streams Consumer Groups track unacknowledged messages via Pending Entries Lists (PEL). Stale messages from crashed workers are reclaimed automatically via `XAUTOCLAIM`, while incoming duplicate frames are filtered via SHA-256 fingerprinting.

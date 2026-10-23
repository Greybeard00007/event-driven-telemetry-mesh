# ADR 0004: Two-Tier Evaluation (Deterministic Physics Guardrail before LLM)

## Status
Accepted

## Context
Language models are non-deterministic, computationally expensive, and lack sub-millisecond guarantees required for instantaneous rig equipment protection. Directing 10,000 sensor ticks/sec directly into an LLM causes severe queue congestion and safety risks.

## Decision
Implement a **Two-Tier Safety Architecture**:
1. **Tier 1 (Microsecond Physics & ML)**: Every sensor tick evaluates against deterministic rules (WOB, RPM, Torque, SPP, Flow Balance) and streaming statistical drift (Welford's EWMA) in **42 µs** ($>21,000$ ev/s).
2. **Tier 2 (AI Root-Cause Reasoning)**: Only anomalous sensor windows are routed to the Ollama LLM worker pool, contextualized with a 30-sample pre-event curve history and vector-retrieved API RP 53 well control guidelines.

## Consequences
- Guaranteed sub-millisecond physical threshold trips.
- LLM inference capacity is preserved strictly for complex root-cause reasoning and crew advisories.

# ADR 0002: Use Ollama for Local-First AI Inference

## Status
Accepted

## Context
Upstream oil and gas drilling telemetry contains commercially sensitive, confidential formation evaluation data. Offshore and remote land rigs frequently experience VSAT satellite bandwidth constraints or total network blackouts. Cloud LLM APIs (OpenAI, Anthropic) cannot be relied upon for mission-critical well control safety.

## Decision
Utilize **local Ollama** instances as the primary inference engine:
1. Models execute directly on owned local hardware (e.g. `deepseek-r1:8b`, `llama3.2:1b`).
2. Structured JSON response formatting enforced via prompt contracts.
3. Concurrency limited via asynchronous semaphores to protect local GPU/CPU VRAM.
4. Built-in petroleum engineering domain fallback heuristic if the daemon is offline or downloading models.

## Consequences
- 100% data privacy: Zero telemetry bytes leave the rig perimeter.
- Air-gap capability during satellite communications interruptions.
- Reliable structured diagnostics delivered in real time to drillers.

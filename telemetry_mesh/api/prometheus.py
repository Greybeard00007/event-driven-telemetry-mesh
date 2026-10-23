"""Prometheus standard metrics exposition generator."""

import time
from typing import Any, Dict


def generate_prometheus_metrics(
    published_total: int,
    processed_total: int,
    anomalies_total: int,
    duplicates_dropped: int,
    dlq_total: int,
    stream_length: int,
    consumer_pending: int,
    active_rigs: int,
    active_websockets: int,
) -> str:
    """Generate standard OpenMetrics / Prometheus plaintext exposition format."""
    now_ts = int(time.time() * 1000)

    lines = [
        "# HELP telemetry_events_published_total Total sensor records published to Redis Streams",
        "# TYPE telemetry_events_published_total counter",
        f"telemetry_events_published_total {published_total}",
        "",
        "# HELP telemetry_events_processed_total Total records consumed and evaluated by worker nodes",
        "# TYPE telemetry_events_processed_total counter",
        f"telemetry_events_processed_total {processed_total}",
        "",
        "# HELP telemetry_anomalies_detected_total Total operational anomalies diagnosed by threshold & ML engines",
        "# TYPE telemetry_anomalies_detected_total counter",
        f"telemetry_anomalies_detected_total {anomalies_total}",
        "",
        "# HELP telemetry_duplicates_dropped_total Total duplicate records dropped by idempotency filter",
        "# TYPE telemetry_duplicates_dropped_total counter",
        f"telemetry_duplicates_dropped_total {duplicates_dropped}",
        "",
        "# HELP telemetry_dlq_routed_total Total messages routed to the dead-letter queue",
        "# TYPE telemetry_dlq_routed_total counter",
        f"telemetry_dlq_routed_total {dlq_total}",
        "",
        "# HELP telemetry_stream_length Current length of primary Redis raw telemetry stream",
        "# TYPE telemetry_stream_length gauge",
        f"telemetry_stream_length {stream_length}",
        "",
        "# HELP telemetry_consumer_lag_pending Unacknowledged pending messages in worker consumer group",
        "# TYPE telemetry_consumer_lag_pending gauge",
        f"telemetry_consumer_lag_pending {consumer_pending}",
        "",
        "# HELP telemetry_active_rigs_count Total drilling rigs currently active",
        "# TYPE telemetry_active_rigs_count gauge",
        f"telemetry_active_rigs_count {active_rigs}",
        "",
        "# HELP telemetry_active_websocket_clients Current number of connected WebSocket dashboard subscribers",
        "# TYPE telemetry_active_websocket_clients gauge",
        f"telemetry_active_websocket_clients {active_websockets}",
        "",
    ]
    return "\n".join(lines) + "\n"

"""Tests for Audit Trail compliance signing and Prometheus metrics generation."""

from telemetry_mesh.api.prometheus import generate_prometheus_metrics
from telemetry_mesh.persistence.audit import AuditTrailManager


def test_prometheus_metrics_format():
    """Test standard OpenMetrics exposition format generation."""
    metrics_output = generate_prometheus_metrics(
        published_total=12500,
        processed_total=12480,
        anomalies_total=14,
        duplicates_dropped=20,
        dlq_total=2,
        stream_length=450,
        consumer_pending=5,
        active_rigs=3,
        active_websockets=2,
    )

    assert "telemetry_events_published_total 12500" in metrics_output
    assert "telemetry_events_processed_total 12480" in metrics_output
    assert "telemetry_anomalies_detected_total 14" in metrics_output
    assert "telemetry_dlq_routed_total 2" in metrics_output
    assert "telemetry_consumer_lag_pending 5" in metrics_output
    assert "telemetry_active_rigs_count 3" in metrics_output


def test_audit_trail_signature():
    """Test cryptographic signoff logging and retrieval."""
    mgr = AuditTrailManager()

    entry = mgr.record_signoff(
        anomaly_id="anom-1234-abcd",
        rig_id="RIG-TITAN",
        driller_name="Sarah Connor (Toolpusher)",
        action_taken="Shut in annular preventer and alerted superintendent",
        comments="SIDPP stabilized at 380 psi",
    )

    assert entry.audit_id is not None
    assert len(entry.signature_hash) == 64  # SHA-256
    assert entry.driller_name == "Sarah Connor (Toolpusher)"

    history = mgr.get_entries(rig_id="RIG-TITAN")
    assert len(history) == 1
    assert history[0].anomaly_id == "anom-1234-abcd"

"""Tests for Ollama AI worker pool and domain fallback diagnostics."""

from datetime import datetime, timezone
import pytest
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity, AnomalyType
from telemetry_mesh.workers.ollama_pool import OllamaWorkerPool


@pytest.fixture
def sample_kick_anomaly() -> AnomalyEvent:
    return AnomalyEvent(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        timestamp=datetime.now(timezone.utc),
        anomaly_type=AnomalyType.KICK_INFLUX,
        severity=AnomalySeverity.CRITICAL,
        rule_triggered="FLOW_OUT_EXCEEDS_FLOW_IN_INFLUX_RULE",
        threshold_details={"flow_in": 600.0, "flow_out": 750.0, "delta_percent": 25.0},
        current_metrics={"flow_in": 600.0, "flow_out": 750.0, "spp": 2600.0},
        window_metrics=[{"timestamp": "2026-10-22T12:00:00Z", "flow_in": 600.0, "flow_out": 600.0}],
    )


@pytest.mark.asyncio
async def test_ollama_domain_fallback(sample_kick_anomaly):
    """Test that when Ollama is offline, domain heuristic generates high-quality drilling advisory."""
    # Point to nonexistent port to trigger connection error
    pool = OllamaWorkerPool(
        base_url="http://127.0.0.1:54321",
        timeout_seconds=0.5,
        fallback_mock=True,
    )

    diagnosis = await pool.diagnose_anomaly(sample_kick_anomaly)
    assert diagnosis is not None
    assert diagnosis.confidence_score >= 0.90
    assert diagnosis.urgency_level == AnomalySeverity.CRITICAL
    assert "hydrostatic" in diagnosis.root_cause_analysis.lower()
    assert "annular preventer" in diagnosis.drilling_recommendation.lower()
    assert len(diagnosis.preventive_measures) > 0

    await pool.close()


def test_ollama_prompt_construction(sample_kick_anomaly):
    """Test engineering prompt generation includes structured sensor context."""
    pool = OllamaWorkerPool()
    prompt = pool._build_prompt(sample_kick_anomaly)

    assert "Senior Petroleum" in prompt
    assert "RIG-TITAN" in prompt
    assert "KICK_INFLUX" in prompt
    assert "drilling_recommendation" in prompt

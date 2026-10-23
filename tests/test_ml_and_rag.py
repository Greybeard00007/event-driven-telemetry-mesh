"""Tests for Streaming ML Anomaly Detector and Drilling RAG Knowledge Base."""

from datetime import datetime, timezone
import pytest
from telemetry_mesh.models.anomaly import AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.workers.ml_anomaly import StreamingMlDetector
from telemetry_mesh.workers.rag_knowledge import DrillingKnowledgeBase


def test_streaming_ml_gradual_washout_detection():
    """Test unsupervised detection of gradual SPP washout."""
    detector = StreamingMlDetector(z_threshold=3.0)

    # Feed 30 steady baseline samples to calibrate running mean
    for i in range(30):
        rec = RigTelemetry(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            depth_md=8500.0 + i * 0.1,
            wob=28.0,
            rpm=120.0,
            torque=14.5,
            flow_in=620.0,
            flow_out=618.0,
            spp=3000.0,
            rop=45.0,
        )
        detector.evaluate(rec)

    # Now simulate gradual SPP bleed while mud pump rate stays solid
    # SPP drops from 3000 down to 2600 psi
    anomaly = None
    for step in range(5):
        bleed_rec = RigTelemetry(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            depth_md=8503.0 + step * 0.1,
            wob=28.0,
            rpm=120.0,
            torque=14.5,
            flow_in=620.0,  # Pump steady
            flow_out=618.0,
            spp=2650.0 - (step * 20),  # SPP bled significantly
            rop=45.0,
        )
        anomaly = detector.evaluate(bleed_rec)
        if anomaly:
            break

    assert anomaly is not None
    assert anomaly.anomaly_type == AnomalyType.SENSOR_DRIFT
    assert "WASHOUT" in anomaly.rule_triggered


def test_drilling_rag_retrieval():
    """Test retrieval of exact operational SOPs for kick and stick-slip."""
    kb = DrillingKnowledgeBase()

    # Query for Kick
    kick_results = kb.retrieve("KICK_INFLUX flow_out exceeds flow_in pit gain", top_k=1)
    assert len(kick_results) >= 1
    assert "API Standard 53" in kick_results[0]["title"]
    assert "Hard Shut-In" in kick_results[0]["content"]

    # Query for Stick-Slip
    stick_results = kb.retrieve("STICK_SLIP torsional vibration RPM oscillation", top_k=1)
    assert len(stick_results) >= 1
    assert "SoftTorque" in stick_results[0]["title"]
    assert "Reduce Weight on Bit" in stick_results[0]["content"]

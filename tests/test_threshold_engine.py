"""Tests for deterministic threshold rules engine."""

from datetime import datetime, timezone
import pytest
from telemetry_mesh.models.anomaly import AnomalySeverity, AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.workers.sliding_window import RigSlidingWindow
from telemetry_mesh.workers.threshold_engine import ThresholdEngine


@pytest.fixture
def baseline_window() -> RigSlidingWindow:
    """Pre-fills a 15-sample normal drilling window."""
    window = RigSlidingWindow(rig_id="RIG-TITAN", max_samples=30)
    for i in range(15):
        rec = RigTelemetry(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            timestamp=datetime.now(timezone.utc),
            depth_md=8500.0 + i * 0.1,
            wob=28.0,
            rpm=120.0,
            torque=14.5,
            flow_in=620.0,
            flow_out=618.0,
            spp=2950.0,
            rop=45.0,
        )
        window.append(rec)
    return window


def test_normal_drilling_no_anomaly(baseline_window):
    """Test that normal drilling parameters trigger no anomalies."""
    engine = ThresholdEngine()
    current = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8501.6,
        wob=28.2,
        rpm=121.0,
        torque=14.8,
        flow_in=620.0,
        flow_out=619.0,
        spp=2955.0,
        rop=45.5,
    )
    anomaly = engine.evaluate(current, baseline_window)
    assert anomaly is None


def test_detect_kick_influx(baseline_window):
    """Test detection of formation fluid influx (flow_out > flow_in)."""
    engine = ThresholdEngine(flow_delta_percent=15.0)
    # Flow In = 600 gpm, Flow Out surges to 750 gpm (+25%)
    current = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8502.0,
        wob=28.0,
        rpm=120.0,
        torque=14.5,
        flow_in=600.0,
        flow_out=750.0,
        spp=2700.0,
        rop=45.0,
    )
    anomaly = engine.evaluate(current, baseline_window)
    assert anomaly is not None
    assert anomaly.anomaly_type == AnomalyType.KICK_INFLUX
    assert anomaly.severity == AnomalySeverity.CRITICAL
    assert "INFLUX" in anomaly.rule_triggered


def test_detect_pack_off(baseline_window):
    """Test detection of pack-off (SPP spike + flow out restriction)."""
    engine = ThresholdEngine(spp_max=4800.0)
    # SPP jumps by 750 psi and flow_out drops below 90% of flow_in
    current = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8502.0,
        wob=28.0,
        rpm=120.0,
        torque=22.0,
        flow_in=620.0,
        flow_out=520.0,
        spp=3750.0,  # ~800 psi delta above 2950 baseline
        rop=30.0,
    )
    anomaly = engine.evaluate(current, baseline_window)
    assert anomaly is not None
    assert anomaly.anomaly_type == AnomalyType.PACK_OFF
    assert anomaly.severity == AnomalySeverity.HIGH


def test_detect_motor_stall(baseline_window):
    """Test detection of downhole motor stall (RPM collapse + torque surge)."""
    engine = ThresholdEngine(torque_max=35.0)
    current = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8502.0,
        wob=30.0,
        rpm=5.0,        # Near 0
        torque=39.5,    # Exceeds max 35 kft-lbf
        flow_in=620.0,
        flow_out=618.0,
        spp=3200.0,
        rop=2.0,
    )
    anomaly = engine.evaluate(current, baseline_window)
    assert anomaly is not None
    assert anomaly.anomaly_type == AnomalyType.MOTOR_STALL
    assert anomaly.severity == AnomalySeverity.HIGH


def test_detect_excessive_wob(baseline_window):
    """Test detection of excessive weight on bit."""
    engine = ThresholdEngine(wob_max=55.0)
    current = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8502.0,
        wob=62.5,  # Exceeds 55 klbf
        rpm=120.0,
        torque=24.0,
        flow_in=620.0,
        flow_out=618.0,
        spp=2950.0,
        rop=35.0,
    )
    anomaly = engine.evaluate(current, baseline_window)
    assert anomaly is not None
    assert anomaly.anomaly_type == AnomalyType.EXCESSIVE_WOB


def test_detect_lost_circulation(baseline_window):
    """Test detection of lost circulation (flow out drops < 75% of flow in)."""
    engine = ThresholdEngine()
    current = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8502.0,
        wob=28.0,
        rpm=120.0,
        torque=14.0,
        flow_in=600.0,
        flow_out=380.0,  # Only ~63% return
        spp=2900.0,
        rop=45.0,
    )
    anomaly = engine.evaluate(current, baseline_window)
    assert anomaly is not None
    assert anomaly.anomaly_type == AnomalyType.LOST_CIRCULATION
    assert anomaly.severity == AnomalySeverity.HIGH

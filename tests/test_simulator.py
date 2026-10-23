"""Tests for multi-rig telemetry simulator and anomaly injection."""

import asyncio
import pytest
from telemetry_mesh.ingestion.simulator import RigState, TelemetrySimulator
from telemetry_mesh.models.anomaly import AnomalyType


def test_rig_state_baseline_generation():
    """Verify that simulated rig state generates valid baseline telemetry."""
    rig = RigState(rig_id="RIG-ALPHA", well_id="WELL-101", start_depth=5000.0)
    assert rig.rig_id == "RIG-ALPHA"
    assert rig.well_id == "WELL-101"
    assert rig.active_anomaly is None

    reading = rig.generate_next(delta_seconds=0.2)
    assert reading.rig_id == "RIG-ALPHA"
    assert reading.depth_md >= 5000.0
    assert 10.0 <= reading.wob <= 50.0
    assert 80.0 <= reading.rpm <= 160.0
    assert reading.spp > 0.0


def test_rig_state_stick_slip_injection():
    """Verify anomaly injection changes physical characteristics."""
    rig = RigState(rig_id="RIG-BETA", well_id="WELL-102")
    rig.inject_anomaly(AnomalyType.STICK_SLIP, duration_seconds=10.0)
    assert rig.active_anomaly == AnomalyType.STICK_SLIP

    readings = [rig.generate_next(delta_seconds=0.2) for _ in range(15)]
    # In stick-slip, torque spikes or RPM drops severely
    has_elevated_torque_or_low_rpm = any(r.torque > 25.0 or r.rpm < 40.0 for r in readings)
    assert has_elevated_torque_or_low_rpm is True

    rig.clear_anomaly()
    assert rig.active_anomaly is None


def test_rig_state_kick_influx_injection():
    """Verify kick/influx anomaly raises flow out above flow in."""
    rig = RigState(rig_id="RIG-GAMMA", well_id="WELL-103")
    rig.inject_anomaly(AnomalyType.KICK_INFLUX, duration_seconds=10.0)

    reading = rig.generate_next(delta_seconds=0.2)
    assert reading.flow_out > reading.flow_in


@pytest.mark.asyncio
async def test_telemetry_simulator_batch_generation():
    """Verify TelemetrySimulator emits batches from all configured rigs."""
    sim = TelemetrySimulator(num_rigs=2, frequency_hz=10.0)
    assert len(sim.rigs) == 2

    received_batches = []

    async def callback(batch):
        received_batches.append(batch)
        if len(received_batches) >= 2:
            sim.stop()

    task = asyncio.create_task(sim.run_loop(callback))
    await asyncio.sleep(0.35)
    sim.stop()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(received_batches) >= 1
    assert len(received_batches[0]) == 2

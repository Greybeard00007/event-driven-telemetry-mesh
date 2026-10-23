"""Tests for RigSlidingWindow and WindowManager."""

from datetime import datetime, timezone
import pytest
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.workers.sliding_window import RigSlidingWindow, WindowManager


def test_sliding_window_capacity():
    """Test that window keeps maximum N samples and computes stats."""
    window = RigSlidingWindow(rig_id="RIG-TEST", max_samples=10)

    for i in range(15):
        rec = RigTelemetry(
            rig_id="RIG-TEST",
            well_id="WELL-TEST",
            timestamp=datetime.now(timezone.utc),
            depth_md=1000.0 + i,
            wob=20.0 + i,
            rpm=100.0 + (i * 2),
            torque=10.0,
            flow_in=500.0,
            flow_out=500.0,
            spp=2500.0,
            rop=30.0,
        )
        window.append(rec)

    assert window.size() == 10
    records = window.get_records()
    assert len(records) == 10
    # First record in window should be i=5 (depth_md=1005.0)
    assert records[0].depth_md == 1005.0
    assert records[-1].depth_md == 1014.0


def test_sliding_window_statistics():
    """Test statistics computation for sensor channels."""
    window = RigSlidingWindow(rig_id="RIG-TEST", max_samples=5)
    values = [10.0, 20.0, 30.0, 40.0, 50.0]

    for v in values:
        rec = RigTelemetry(
            rig_id="RIG-TEST",
            well_id="WELL-TEST",
            depth_md=1000.0,
            wob=v,
            rpm=100.0,
            torque=10.0,
            flow_in=500.0,
            flow_out=500.0,
            spp=2500.0,
            rop=30.0,
        )
        window.append(rec)

    stats = window.compute_stats("wob")
    assert stats["count"] == 5
    assert stats["min"] == 10.0
    assert stats["max"] == 50.0
    assert stats["mean"] == 30.0
    assert stats["delta"] == 40.0


def test_window_manager_multi_rig():
    """Test WindowManager maintains independent windows per rig."""
    wm = WindowManager(max_samples=10)

    rec_a = RigTelemetry(
        rig_id="RIG-A", well_id="WELL-A", depth_md=5000.0,
        wob=25.0, rpm=100.0, torque=12.0, flow_in=500.0, flow_out=500.0, spp=2000.0, rop=30.0
    )
    rec_b = RigTelemetry(
        rig_id="RIG-B", well_id="WELL-B", depth_md=7000.0,
        wob=30.0, rpm=110.0, torque=14.0, flow_in=600.0, flow_out=600.0, spp=2400.0, rop=35.0
    )

    wm.add(rec_a)
    wm.add(rec_b)

    assert wm.get_window("RIG-A").size() == 1
    assert wm.get_window("RIG-B").size() == 1
    assert wm.get_window("RIG-A").get_records()[0].depth_md == 5000.0
    assert wm.get_window("RIG-B").get_records()[0].depth_md == 7000.0

"""Tests for Digital Twin Physics Engine (MSE, ECD, d_xc)."""

from datetime import datetime, timezone
import pytest
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.workers.physics_engine import DrillingPhysicsEngine


def test_mse_calculation():
    """Test Teale's Mechanical Specific Energy calculation."""
    engine = DrillingPhysicsEngine(bit_diameter_in=8.5)

    # Standard drilling: WOB 30 klbf, RPM 120, Torque 15 kft-lbf, ROP 50 ft/hr
    mse = engine.compute_mse(wob_klbf=30.0, rpm=120.0, torque_kft_lbf=15.0, rop_ft_hr=50.0)
    assert mse > 0.0
    # Expected value around 7-25 kpsi for normal sandstone/shale
    assert 5.0 < mse < 30.0

    # Test stationary / off-bottom (ROP near 0)
    mse_off_bottom = engine.compute_mse(wob_klbf=0.0, rpm=120.0, torque_kft_lbf=4.0, rop_ft_hr=0.0)
    assert mse_off_bottom == 0.0


def test_ecd_calculation():
    """Test Equivalent Circulating Density calculation."""
    engine = DrillingPhysicsEngine(nominal_mud_weight_ppg=10.5)

    # At 8,000 ft TVD with 3000 psi SPP
    ecd = engine.compute_ecd(flow_in_gpm=620.0, spp_psi=3000.0, tvd_ft=8000.0)
    assert ecd > 10.5
    # Expected dynamic increment around 0.6 - 1.2 ppg
    assert 11.0 <= ecd <= 12.0


def test_d_exponent_calculation():
    """Test corrected d-exponent calculation for pore pressure analysis."""
    engine = DrillingPhysicsEngine(bit_diameter_in=8.5, nominal_mud_weight_ppg=10.5, normal_pore_pressure_grad_ppg=9.0)

    d_xc = engine.compute_d_exponent(rop_ft_hr=45.0, rpm=120.0, wob_klbf=28.0)
    assert d_xc is not None
    # Standard normal trend is typically between 1.0 and 2.0
    assert 0.8 < d_xc < 2.5


def test_full_physics_telemetry_evaluation():
    """Test complete physics evaluation of a RigTelemetry frame."""
    engine = DrillingPhysicsEngine()
    rec = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        timestamp=datetime.now(timezone.utc),
        depth_md=8500.0,
        depth_tvd=7800.0,
        wob=28.0,
        rpm=120.0,
        torque=14.5,
        flow_in=620.0,
        flow_out=618.0,
        spp=2950.0,
        rop=45.0,
    )

    results = engine.evaluate_telemetry(rec)
    assert "mse_kpsi" in results
    assert "ecd_ppg" in results
    assert "d_xc" in results
    assert "drilling_efficiency_pct" in results
    assert results["ecd_ppg"] > 10.0

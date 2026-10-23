"""Tests for Directional Wellbore Trajectory Engine."""

import pytest
from telemetry_mesh.workers.trajectory_engine import WellboreTrajectoryEngine


def test_trajectory_vertical_section():
    """Test vertical section calculation."""
    engine = WellboreTrajectoryEngine(rig_id="RIG-TITAN", well_id="WELL-001")

    p1 = engine.add_survey_station(md=0.0, inc_deg=0.0, azi_deg=0.0)
    assert p1.tvd == 0.0
    assert p1.north == 0.0
    assert p1.east == 0.0

    p2 = engine.add_survey_station(md=2000.0, inc_deg=0.0, azi_deg=0.0)
    assert p2.tvd == 2000.0
    assert p2.north == 0.0
    assert p2.east == 0.0
    assert p2.dogleg_severity == 0.0


def test_trajectory_deviated_kickoff():
    """Test Minimum Curvature calculation along build and tangent sections."""
    engine = WellboreTrajectoryEngine(rig_id="RIG-TITAN", well_id="WELL-001")

    # Tie-in
    engine.add_survey_station(md=0.0, inc_deg=0.0, azi_deg=0.0)
    # Kickoff point
    engine.add_survey_station(md=3000.0, inc_deg=0.0, azi_deg=0.0)
    # Build section to 30 deg inclination towards NE (Azimuth 45 deg)
    p3 = engine.add_survey_station(md=4000.0, inc_deg=30.0, azi_deg=45.0)

    assert p3.tvd < 4000.0  # TVD must be less than MD due to deviation
    assert p3.north > 0.0   # Moved North
    assert p3.east > 0.0    # Moved East
    assert p3.dogleg_severity > 0.0
    assert 44.0 <= p3.closure_azimuth <= 46.0

    trajectory = engine.get_trajectory()
    assert len(trajectory) == 3

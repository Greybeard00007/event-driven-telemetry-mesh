"""Directional Wellbore Trajectory Engine using the Minimum Curvature Method (API / SPE Standard)."""

from datetime import datetime, timezone
import math
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class SurveyStation(BaseModel):
    """MWD Directional survey station."""

    md: float = Field(..., ge=0.0, description="Measured depth (ft)")
    inc_deg: float = Field(..., ge=0.0, le=180.0, description="Inclination in degrees (0 = vertical)")
    azi_deg: float = Field(..., ge=0.0, le=360.0, description="Azimuth in degrees (0 = North, 90 = East)")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TrajectoryPoint(BaseModel):
    """Calculated 3D Cartesian coordinates along well path."""

    md: float
    tvd: float
    north: float
    east: float
    inc_deg: float
    azi_deg: float
    dogleg_severity: float  # deg / 100 ft
    closure_distance: float
    closure_azimuth: float


class WellboreTrajectoryEngine:
    """Calculates 3D wellbore trajectory and dogleg severity using Minimum Curvature."""

    def __init__(self, rig_id: str, well_id: str):
        self.rig_id = rig_id
        self.well_id = well_id
        self.stations: List[SurveyStation] = []
        self.computed_path: List[TrajectoryPoint] = []

    def add_survey_station(self, md: float, inc_deg: float, azi_deg: float) -> TrajectoryPoint:
        """Add new survey station and compute incremental 3D coordinates."""
        station = SurveyStation(md=md, inc_deg=inc_deg, azi_deg=azi_deg)
        self.stations.append(station)

        if len(self.stations) == 1:
            # First station (surface tie-in)
            point = TrajectoryPoint(
                md=md,
                tvd=md * math.cos(math.radians(inc_deg)),
                north=0.0,
                east=0.0,
                inc_deg=inc_deg,
                azi_deg=azi_deg,
                dogleg_severity=0.0,
                closure_distance=0.0,
                closure_azimuth=0.0,
            )
            self.computed_path.append(point)
            return point

        # Previous station and current station
        s1 = self.stations[-2]
        s2 = self.stations[-1]
        prev_pt = self.computed_path[-1]

        delta_md = s2.md - s1.md
        if delta_md <= 0.0:
            return prev_pt

        # Convert to radians
        i1 = math.radians(s1.inc_deg)
        i2 = math.radians(s2.inc_deg)
        a1 = math.radians(s1.azi_deg)
        a2 = math.radians(s2.azi_deg)

        # Dogleg angle calculation (beta)
        cos_beta = math.cos(i2 - i1) - (math.sin(i1) * math.sin(i2) * (1.0 - math.cos(a2 - a1)))
        cos_beta = max(-1.0, min(1.0, cos_beta))
        beta = math.acos(cos_beta)

        # Ratio factor F
        if beta > 1e-6:
            rf = (2.0 / beta) * math.tan(beta / 2.0)
        else:
            rf = 1.0  # L'Hopital limit as beta -> 0

        # Coordinate increments
        delta_tvd = (delta_md / 2.0) * (math.cos(i1) + math.cos(i2)) * rf
        delta_north = (delta_md / 2.0) * (math.sin(i1) * math.cos(a1) + math.sin(i2) * math.cos(a2)) * rf
        delta_east = (delta_md / 2.0) * (math.sin(i1) * math.sin(a1) + math.sin(i2) * math.sin(a2)) * rf

        tvd = prev_pt.tvd + delta_tvd
        north = prev_pt.north + delta_north
        east = prev_pt.east + delta_east

        # Dogleg Severity (DLS) in deg/100 ft
        dls = (math.degrees(beta) / delta_md) * 100.0

        closure_dist = math.sqrt(north**2 + east**2)
        closure_azi = math.degrees(math.atan2(east, north)) % 360.0

        point = TrajectoryPoint(
            md=round(s2.md, 2),
            tvd=round(tvd, 2),
            north=round(north, 2),
            east=round(east, 2),
            inc_deg=round(s2.inc_deg, 2),
            azi_deg=round(s2.azi_deg, 2),
            dogleg_severity=round(dls, 2),
            closure_distance=round(closure_dist, 2),
            closure_azimuth=round(closure_azi, 2),
        )
        self.computed_path.append(point)
        return point

    def get_trajectory(self) -> List[TrajectoryPoint]:
        return self.computed_path

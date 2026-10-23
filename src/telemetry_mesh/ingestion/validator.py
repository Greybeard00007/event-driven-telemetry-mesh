"""Sensor telemetry validation and boundary verification."""

from typing import List, Tuple
from telemetry_mesh.models.telemetry import RigTelemetry


class TelemetryValidator:
    """Validates physical range constraints and sanitizes sensor readings."""

    # Reasonable physical extremes for drilling rigs
    LIMITS = {
        "depth_md": (0.0, 45_000.0),       # Depth up to 45,000 ft
        "wob": (0.0, 150.0),               # Weight on bit up to 150 klbf
        "rpm": (0.0, 500.0),               # Rotary table / motor speed
        "torque": (0.0, 100.0),            # Torque up to 100 kft-lbf
        "flow_in": (0.0, 2000.0),          # Mud flow in gpm
        "flow_out": (0.0, 2500.0),         # Mud return gpm
        "spp": (0.0, 8000.0),              # Standpipe pressure psi
        "rop": (0.0, 500.0),               # Rate of penetration ft/hr
    }

    @classmethod
    def validate_record(cls, record: RigTelemetry) -> Tuple[bool, List[str]]:
        """Verify whether a single RigTelemetry record conforms to physics boundaries."""
        issues: List[str] = []

        if not record.rig_id or record.rig_id.strip() == "":
            issues.append("Missing rig_id")

        if not record.well_id or record.well_id.strip() == "":
            issues.append("Missing well_id")

        for attr, (min_v, max_v) in cls.LIMITS.items():
            val = getattr(record, attr, None)
            if val is not None:
                if val < min_v or val > max_v:
                    issues.append(f"{attr}={val} exceeds physical range [{min_v}, {max_v}]")

        return len(issues) == 0, issues

    @classmethod
    def filter_valid(cls, records: List[RigTelemetry]) -> Tuple[List[RigTelemetry], List[Tuple[RigTelemetry, List[str]]]]:
        """Filter a batch into valid records and rejected invalid records with reasons."""
        valid: List[RigTelemetry] = []
        invalid: List[Tuple[RigTelemetry, List[str]]] = []

        for record in records:
            is_valid, errors = cls.validate_record(record)
            if is_valid:
                valid.append(record)
            else:
                invalid.append((record, errors))

        return valid, invalid

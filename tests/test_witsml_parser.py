"""Tests for WITSML XML and JSON ingestion parsers and validator."""

from datetime import datetime, timezone
import pytest
from telemetry_mesh.ingestion.parser import TelemetryParser
from telemetry_mesh.ingestion.validator import TelemetryValidator
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.models.witsml import WitsmlLog


def test_witsml_xml_roundtrip():
    """Test generating WITSML XML and parsing it back into RigTelemetry."""
    original_records = [
        RigTelemetry(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            timestamp=datetime(2026, 10, 22, 12, 0, 0, tzinfo=timezone.utc),
            depth_md=8500.0,
            wob=28.5,
            rpm=120.0,
            torque=15.0,
            flow_in=620.0,
            flow_out=619.0,
            spp=2950.0,
            rop=45.0,
            gamma=72.0,
        ),
        RigTelemetry(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            timestamp=datetime(2026, 10, 22, 12, 0, 1, tzinfo=timezone.utc),
            depth_md=8500.5,
            wob=29.0,
            rpm=121.0,
            torque=15.2,
            flow_in=620.0,
            flow_out=620.0,
            spp=2960.0,
            rop=46.0,
            gamma=74.0,
        ),
    ]

    xml_content = WitsmlLog.generate_witsml_xml("RIG-TITAN", "WELL-001", original_records)
    assert "<logs" in xml_content
    assert "<logCurveInfo>" in xml_content

    parsed_records = TelemetryParser.parse_witsml_xml(xml_content)
    assert len(parsed_records) == 2
    assert parsed_records[0].rig_id == "RIG-TITAN"
    assert parsed_records[0].wob == 28.5
    assert parsed_records[0].rpm == 120.0
    assert parsed_records[1].depth_md == 8500.5


def test_json_payload_parser():
    """Test parsing JSON telemetry objects."""
    payload = {
        "records": [
            {
                "rig_id": "RIG-ORION",
                "well_id": "WELL-002",
                "depth_md": 9100.0,
                "wob": 32.0,
                "rpm": 110.0,
                "torque": 16.5,
                "flow_in": 550.0,
                "flow_out": 548.0,
                "spp": 2800.0,
                "rop": 40.0,
            }
        ]
    }
    records = TelemetryParser.parse_json_payload(payload)
    assert len(records) == 1
    assert records[0].rig_id == "RIG-ORION"
    assert records[0].torque == 16.5


def test_telemetry_validator_valid():
    """Test validation of within-bound telemetry."""
    rec = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8500.0,
        wob=25.0,
        rpm=120.0,
        torque=15.0,
        flow_in=600.0,
        flow_out=600.0,
        spp=3000.0,
        rop=50.0,
    )
    is_valid, errors = TelemetryValidator.validate_record(rec)
    assert is_valid is True
    assert len(errors) == 0


def test_telemetry_validator_out_of_bounds():
    """Test rejection of out-of-physical-boundary telemetry."""
    rec = RigTelemetry(
        rig_id="",  # Missing rig
        well_id="WELL-001",
        depth_md=8500.0,
        wob=999.0,  # Exceeds 150 klbf physical max
        rpm=120.0,
        torque=15.0,
        flow_in=600.0,
        flow_out=600.0,
        spp=3000.0,
        rop=50.0,
    )
    is_valid, errors = TelemetryValidator.validate_record(rec)
    assert is_valid is False
    assert any("wob" in err for err in errors)
    assert any("Missing rig_id" in err for err in errors)

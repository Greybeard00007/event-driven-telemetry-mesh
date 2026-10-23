"""Tests for WITS Level 0 and ETP v1.2 protocols."""

import asyncio
import json
import pytest
from telemetry_mesh.ingestion.etp import EtpSession, ETP_PROTO_CORE, ETP_PROTO_CHANNEL_STREAMING, MSG_REQUEST_SESSION, MSG_CHANNEL_DATA
from telemetry_mesh.ingestion.wits0 import Wits0Parser
from telemetry_mesh.models.telemetry import RigTelemetry


def test_wits0_packet_parsing():
    """Test parsing standard WITS Level 0 ASCII packets."""
    raw_packet = (
        "&&\r\n"
        "01088520.50\r\n"
        "011028.40\r\n"
        "011114.90\r\n"
        "0112122.0\r\n"
        "011345.00\r\n"
        "01142940.0\r\n"
        "0117620.0\r\n"
        "0118619.0\r\n"
        "012165.0\r\n"
        "!!\r\n"
    )
    record = Wits0Parser.parse_packet(raw_packet, rig_id="RIG-TITAN", well_id="WELL-001")
    assert record is not None
    assert record.rig_id == "RIG-TITAN"
    assert record.depth_md == 8520.50
    assert record.wob == 28.40
    assert record.rpm == 122.0
    assert record.flow_in == 620.0
    assert record.flow_out == 619.0
    assert record.gamma == 65.0


def test_wits0_packet_format_roundtrip():
    """Test serializing RigTelemetry to WITS0 ASCII format and re-parsing."""
    original = RigTelemetry(
        rig_id="RIG-TEST",
        well_id="WELL-TEST",
        depth_md=9200.0,
        wob=31.5,
        rpm=115.0,
        torque=16.0,
        flow_in=580.0,
        flow_out=580.0,
        spp=2850.0,
        rop=42.0,
        gamma=78.0,
    )
    formatted = Wits0Parser.format_packet(original)
    assert formatted.startswith("&&\r\n")
    assert formatted.endswith("!!\r\n")

    parsed = Wits0Parser.parse_packet(formatted, rig_id="RIG-TEST", well_id="WELL-TEST")
    assert parsed is not None
    assert parsed.depth_md == 9200.0
    assert parsed.wob == 31.5
    assert parsed.rpm == 115.0
    assert parsed.gamma == 78.0


@pytest.mark.asyncio
async def test_etp_session_handshake_and_data():
    """Test ETP v1.2 session negotiation and channel streaming frame ingestion."""
    sent_messages = []

    class MockWebSocket:
        async def send_text(self, text: str):
            sent_messages.append(text)

    ws = MockWebSocket()
    session = EtpSession(ws)

    # 1. RequestSession Handshake
    req_session = {
        "header": {"protocol": ETP_PROTO_CORE, "messageType": MSG_REQUEST_SESSION, "messageId": 1},
        "body": {"applicationName": "TestClient"},
    }
    await session.handle_message(json.dumps(req_session))
    assert session.is_open is True
    assert len(sent_messages) == 1
    resp = json.loads(sent_messages[0])
    assert resp["body"]["sessionId"] == session.session_id

    # 2. ChannelData Streaming Frame
    received_telemetry = []

    def on_telemetry(records):
        received_telemetry.extend(records)

    channel_frame = {
        "header": {"protocol": ETP_PROTO_CHANNEL_STREAMING, "messageType": MSG_CHANNEL_DATA, "messageId": 2},
        "body": {
            "data": [
                {
                    "rigId": "ETP-RIG-01",
                    "wellId": "WELL-ETP",
                    "indexes": [1761180000.0],
                    "values": {
                        "depth_md": 8800.0,
                        "wob": 29.0,
                        "rpm": 125.0,
                        "torque": 15.5,
                        "flow_in": 610.0,
                        "flow_out": 608.0,
                        "spp": 2900.0,
                        "rop": 44.0,
                    },
                }
            ]
        },
    }
    await session.handle_message(json.dumps(channel_frame), on_telemetry=on_telemetry)
    assert len(received_telemetry) == 1
    assert received_telemetry[0].rig_id == "ETP-RIG-01"
    assert received_telemetry[0].depth_md == 8800.0

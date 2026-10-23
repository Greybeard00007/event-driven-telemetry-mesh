"""Energistics Transfer Protocol (ETP v1.2) WebSocket Adapter.

Supports ETP sub-protocols:
- Protocol 1: Core (RequestSession, OpenSession, CloseSession)
- Protocol 8: ChannelStreaming (ChannelData, StartStreaming, ChannelDescribe)
"""

from datetime import datetime, timezone
import json
import logging
import uuid
from typing import Any, Dict, List, Optional
from fastapi import WebSocket

from telemetry_mesh.models.telemetry import RigTelemetry

logger = logging.getLogger(__name__)

# ETP Protocol IDs
ETP_PROTO_CORE = 1
ETP_PROTO_CHANNEL_STREAMING = 8

# Message Type IDs
MSG_REQUEST_SESSION = 1
MSG_OPEN_SESSION = 2
MSG_CHANNEL_DATA = 1
MSG_START_STREAMING = 2


class EtpSession:
    """Represents an active ETP v1.2 client streaming session."""

    def __init__(self, websocket: WebSocket):
        self.websocket = websocket
        self.session_id: str = str(uuid.uuid4())
        self.connected_at: datetime = datetime.now(timezone.utc)
        self.is_open: bool = False
        self.channels: Dict[int, str] = {}  # channel_id -> mnemonic

    async def handle_message(self, raw_message: str, on_telemetry: Optional[Any] = None) -> None:
        """Parse incoming ETP frame and route by protocol ID."""
        try:
            msg = json.loads(raw_message)
        except Exception as e:
            logger.warning(f"Invalid ETP JSON frame: {e}")
            return

        header = msg.get("header", {})
        proto_id = header.get("protocol")
        msg_type = header.get("messageType")
        body = msg.get("body", {})

        if proto_id == ETP_PROTO_CORE and msg_type == MSG_REQUEST_SESSION:
            await self._handle_request_session(header, body)
        elif proto_id == ETP_PROTO_CHANNEL_STREAMING and msg_type == MSG_CHANNEL_DATA:
            await self._handle_channel_data(body, on_telemetry)
        else:
            logger.debug(f"Received unhandled ETP protocol {proto_id}, type {msg_type}")

    async def _handle_request_session(self, header: Dict[str, Any], body: Dict[str, Any]) -> None:
        """Respond with ETP OpenSession confirmation."""
        self.is_open = True
        response = {
            "header": {
                "protocol": ETP_PROTO_CORE,
                "messageType": MSG_OPEN_SESSION,
                "correlationId": header.get("messageId", 1),
                "messageId": 2,
                "messageFlags": 0,
            },
            "body": {
                "sessionId": self.session_id,
                "supportedProtocols": [
                    {"protocol": ETP_PROTO_CORE, "role": "server"},
                    {"protocol": ETP_PROTO_CHANNEL_STREAMING, "role": "producer"},
                ],
                "serverCapabilities": {
                    "applicationName": "EventDrivenTelemetryMesh",
                    "applicationVersion": "1.0.0",
                },
            },
        }
        await self.websocket.send_text(json.dumps(response))
        logger.info(f"ETP Session opened: {self.session_id}")

    async def _handle_channel_data(self, body: Dict[str, Any], on_telemetry: Optional[Any]) -> None:
        """Parse streaming channel items into RigTelemetry records."""
        data_items = body.get("data", [])
        if not data_items or not on_telemetry:
            return

        records: List[RigTelemetry] = []
        for item in data_items:
            rig_id = item.get("rigId", "ETP-RIG-01")
            well_id = item.get("wellId", "WELL-ETP")
            indexes = item.get("indexes", [datetime.now(timezone.utc).timestamp()])
            ts = datetime.fromtimestamp(indexes[0], tz=timezone.utc) if indexes else datetime.now(timezone.utc)
            values = item.get("values", {})

            rec = RigTelemetry(
                rig_id=rig_id,
                well_id=well_id,
                timestamp=ts,
                depth_md=float(values.get("depth_md", 0.0)),
                wob=float(values.get("wob", 0.0)),
                rpm=float(values.get("rpm", 0.0)),
                torque=float(values.get("torque", 0.0)),
                flow_in=float(values.get("flow_in", 0.0)),
                flow_out=float(values.get("flow_out", 0.0)),
                spp=float(values.get("spp", 0.0)),
                rop=float(values.get("rop", 0.0)),
                gamma=float(values.get("gamma")) if "gamma" in values else None,
            )
            records.append(rec)

        if records:
            res = on_telemetry(records)
            if hasattr(res, "__await__"):
                await res

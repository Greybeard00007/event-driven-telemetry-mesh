"""WebSocket Connection Manager for real-time telemetry and anomaly streaming."""

import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional, Set
from fastapi import WebSocket, WebSocketDisconnect

from telemetry_mesh.models.anomaly import AnomalyEvent
from telemetry_mesh.models.telemetry import RigTelemetry

logger = logging.getLogger(__name__)


class WebSocketManager:
    """Manages active client WebSocket connections and broadcasts real-time events."""

    def __init__(self):
        # Set of active connections
        self.active_connections: Set[WebSocket] = set()
        # Per-connection subscribed rig filters (None = all rigs)
        self.subscriptions: Dict[WebSocket, Optional[str]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, rig_id: Optional[str] = None) -> None:
        """Register a new WebSocket client."""
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
            self.subscriptions[websocket] = rig_id
        logger.info(f"WebSocket client connected ({len(self.active_connections)} total). Subscribed to: {rig_id or 'ALL'}")

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a disconnected client."""
        async with self._lock:
            self.active_connections.discard(websocket)
            self.subscriptions.pop(websocket, None)
        logger.info(f"WebSocket client disconnected ({len(self.active_connections)} remaining)")

    async def broadcast_telemetry(self, records: List[RigTelemetry]) -> None:
        """Broadcast telemetry batch to connected clients with rig filtering."""
        if not self.active_connections or not records:
            return

        payload_by_rig: Dict[str, List[Dict[str, Any]]] = {}
        all_payload: List[Dict[str, Any]] = []

        for r in records:
            d = r.model_dump(mode="json")
            all_payload.append(d)
            if r.rig_id not in payload_by_rig:
                payload_by_rig[r.rig_id] = []
            payload_by_rig[r.rig_id].append(d)

        dead_connections: List[WebSocket] = []

        for ws in list(self.active_connections):
            target_rig = self.subscriptions.get(ws)
            if target_rig:
                rig_records = payload_by_rig.get(target_rig, [])
                if not rig_records:
                    continue
                msg = {"type": "telemetry_batch", "records": rig_records}
            else:
                msg = {"type": "telemetry_batch", "records": all_payload}

            try:
                await ws.send_json(msg)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            await self.disconnect(ws)

    async def broadcast_anomaly(self, anomaly: AnomalyEvent) -> None:
        """Broadcast anomaly alert with LLM diagnosis to all clients."""
        if not self.active_connections:
            return

        msg = {
            "type": "anomaly_alert",
            "anomaly": anomaly.model_dump(mode="json"),
        }

        dead_connections: List[WebSocket] = []
        for ws in list(self.active_connections):
            target_rig = self.subscriptions.get(ws)
            if target_rig and target_rig != anomaly.rig_id:
                continue

            try:
                await ws.send_json(msg)
            except Exception:
                dead_connections.append(ws)

        for ws in dead_connections:
            await self.disconnect(ws)

    @property
    def client_count(self) -> int:
        return len(self.active_connections)


ws_manager = WebSocketManager()

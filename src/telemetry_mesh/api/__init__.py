"""API module containing FastAPI app, routes, and WebSocket connection manager."""

from telemetry_mesh.api.app import app, create_app
from telemetry_mesh.api.websocket_manager import ws_manager

__all__ = ["app", "create_app", "ws_manager"]

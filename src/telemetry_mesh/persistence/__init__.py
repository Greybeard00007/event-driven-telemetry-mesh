"""Persistence module for TimescaleDB and local telemetry repository."""

from telemetry_mesh.persistence.db import get_db_pool, close_db_pool, is_db_available, set_db_pool
from telemetry_mesh.persistence.repository import TelemetryRepository

__all__ = [
    "get_db_pool",
    "close_db_pool",
    "is_db_available",
    "set_db_pool",
    "TelemetryRepository",
]

"""Shared fixtures for telemetry mesh test suite."""

from datetime import datetime, timezone
import pytest
import fakeredis.aioredis as fake_aioredis
from telemetry_mesh.broker.redis_client import set_redis_client
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.persistence.repository import TelemetryRepository


@pytest.fixture
async def fake_redis():
    """Provide an in-memory FakeRedis instance for isolated broker testing."""
    client = fake_aioredis.FakeRedis(decode_responses=False)
    set_redis_client(client)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def sample_telemetry() -> RigTelemetry:
    """Provide a standard baseline rig telemetry reading."""
    return RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        timestamp=datetime.now(timezone.utc),
        depth_md=8500.0,
        depth_tvd=7820.0,
        wob=28.5,
        rpm=120.0,
        torque=14.8,
        flow_in=620.0,
        flow_out=618.5,
        spp=2950.0,
        rop=45.0,
        gamma=65.0,
        mud_gas=120.0,
        active_pit_vol=450.0,
    )


@pytest.fixture
def in_memory_repository() -> TelemetryRepository:
    """Provide an in-memory repository without requiring live TimescaleDB server."""
    return TelemetryRepository(pool=None)

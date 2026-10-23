"""Tests for FastAPI REST endpoints and WebSocket manager."""

import pytest
from httpx import ASGITransport, AsyncClient
from telemetry_mesh.api.app import create_app
from telemetry_mesh.api.websocket_manager import WebSocketManager
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.ingestion.simulator import TelemetrySimulator
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalySeverity, AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.models.witsml import WitsmlLog
from telemetry_mesh.persistence.repository import TelemetryRepository


@pytest.fixture
def test_app(fake_redis):
    """Instantiate FastAPI app with pre-wired mock components."""
    app = create_app()
    producer = TelemetryProducer(redis_client=fake_redis)
    repository = TelemetryRepository(pool=None)
    simulator = TelemetrySimulator(num_rigs=2, frequency_hz=1.0)

    app.state.redis = fake_redis
    app.state.producer = producer
    app.state.repository = repository
    app.state.simulator = simulator
    return app


@pytest.mark.asyncio
async def test_rest_ingest_json_batch(test_app):
    """Test POST /api/v1/telemetry/batch endpoint."""
    async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
        payload = {
            "records": [
                {
                    "rig_id": "RIG-TITAN",
                    "well_id": "WELL-001",
                    "depth_md": 8500.0,
                    "wob": 28.0,
                    "rpm": 120.0,
                    "torque": 15.0,
                    "flow_in": 620.0,
                    "flow_out": 619.0,
                    "spp": 2950.0,
                    "rop": 45.0,
                }
            ]
        }
        resp = await ac.post("/api/v1/telemetry/batch", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["accepted"] == 1
        assert len(data["stream_message_ids"]) == 1


@pytest.mark.asyncio
async def test_rest_ingest_witsml_xml(test_app):
    """Test POST /api/v1/telemetry/witsml XML endpoint."""
    rec = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        depth_md=8550.0,
        wob=30.0,
        rpm=125.0,
        torque=16.0,
        flow_in=610.0,
        flow_out=610.0,
        spp=2900.0,
        rop=48.0,
    )
    xml_str = WitsmlLog.generate_witsml_xml("RIG-TITAN", "WELL-001", [rec])

    async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/telemetry/witsml",
            content=xml_str.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["accepted"] == 1


@pytest.mark.asyncio
async def test_rest_health_and_metrics(test_app):
    """Test GET /api/v1/health and GET /api/v1/metrics."""
    async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
        health_resp = await ac.get("/api/v1/health")
        assert health_resp.status_code == 200
        h_data = health_resp.json()
        assert "services" in h_data
        assert h_data["services"]["redis_broker"] == "connected"

        metrics_resp = await ac.get("/api/v1/metrics")
        assert metrics_resp.status_code == 200
        m_data = metrics_resp.json()
        assert "published_total" in m_data


@pytest.mark.asyncio
async def test_rest_simulator_inject(test_app):
    """Test POST /api/v1/simulator/inject endpoint."""
    async with AsyncClient(transport=ASGITransport(app=test_app), base_url="http://test") as ac:
        resp = await ac.post(
            "/api/v1/simulator/inject",
            json={
                "rig_id": "RIG-TITAN",
                "anomaly_type": "STICK_SLIP",
                "duration_seconds": 15.0,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "injected"
        assert data["anomaly_type"] == "STICK_SLIP"


@pytest.mark.asyncio
async def test_websocket_manager_broadcast():
    """Test WebSocketManager state and broadcasting without active clients."""
    mgr = WebSocketManager()
    assert mgr.client_count == 0

    # Broadcasting with 0 clients should not raise error
    await mgr.broadcast_telemetry([])
    await mgr.broadcast_anomaly(
        AnomalyEvent(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            anomaly_type=AnomalyType.STICK_SLIP,
            severity=AnomalySeverity.HIGH,
            rule_triggered="TEST_RULE",
        )
    )

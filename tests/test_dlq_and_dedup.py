"""Tests for Dead-Letter Queue (DLQ) and Deduplication engine."""

import asyncio
from datetime import datetime, timezone
import pytest
from telemetry_mesh.broker.dedup import DeduplicationEngine
from telemetry_mesh.broker.dlq import DeadLetterQueue
from telemetry_mesh.models.telemetry import RigTelemetry


@pytest.mark.asyncio
async def test_dlq_routing_and_replay(fake_redis):
    """Test routing failed message to DLQ, inspecting, and replaying to target stream."""
    dlq = DeadLetterQueue(redis_client=fake_redis, dlq_stream_name="test:stream:dlq")

    # 1. Route message to DLQ
    raw_payload = {"rig_id": "RIG-FAIL", "wob": "corrupt_data"}
    dlq_id = await dlq.route_to_dlq(
        original_msg_id="1700000000-0",
        raw_fields=raw_payload,
        error_reason="Float conversion error on wob",
        delivery_count=3,
    )
    assert dlq_id is not None

    # 2. Check stats & inspect
    stats = await dlq.get_dlq_stats()
    assert stats["length"] == 1

    messages = await dlq.inspect_dlq(count=5)
    assert len(messages) == 1
    assert messages[0]["original_id"] == "1700000000-0"
    assert "Float conversion" in messages[0]["error_reason"]

    # 3. Replay back into active stream
    replayed = await dlq.replay_dlq_message(dlq_id, target_stream="test:stream:replayed")
    assert replayed is True

    # DLQ should now be empty after replay
    after_stats = await dlq.get_dlq_stats()
    assert after_stats["length"] == 0


def test_deduplication_engine():
    """Test sliding window idempotency filtering."""
    engine = DeduplicationEngine(window_seconds=10.0)

    rec1 = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        timestamp=datetime(2026, 10, 22, 12, 0, 0, tzinfo=timezone.utc),
        depth_md=8500.0,
        wob=28.0,
        rpm=120.0,
        torque=15.0,
        flow_in=600.0,
        flow_out=600.0,
        spp=2900.0,
        rop=45.0,
    )

    # First time -> not duplicate
    assert engine.is_duplicate(rec1) is False
    # Second time with identical key -> duplicate!
    assert engine.is_duplicate(rec1) is True

    # Modified timestamp or depth -> not duplicate
    rec2 = rec1.model_copy(update={"depth_md": 8501.0})
    assert engine.is_duplicate(rec2) is False

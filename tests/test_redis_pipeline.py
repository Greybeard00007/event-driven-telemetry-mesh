"""Tests for Redis Streams Producer/Consumer pipeline and consumer groups."""

import asyncio
from datetime import datetime, timezone
import pytest
from telemetry_mesh.broker.consumer import TelemetryConsumer
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.models.telemetry import RigTelemetry


@pytest.mark.asyncio
async def test_producer_consumer_batch_flow(fake_redis):
    """Test full cycle of producing a batch, consuming it via Consumer Group, and acking."""
    stream_name = "test:stream:raw"
    group_name = "test:group:workers"

    producer = TelemetryProducer(redis_client=fake_redis, stream_raw=stream_name)
    consumer = TelemetryConsumer(
        redis_client=fake_redis,
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="test-worker-1",
        batch_size=10,
        block_ms=500,
    )

    # 1. Initialize consumer group
    await consumer.initialize()

    # 2. Produce 5 telemetry records
    records = [
        RigTelemetry(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            timestamp=datetime.now(timezone.utc),
            depth_md=8000.0 + i,
            wob=25.0,
            rpm=110.0,
            torque=14.0,
            flow_in=600.0,
            flow_out=600.0,
            spp=2800.0,
            rop=40.0,
        )
        for i in range(5)
    ]

    msg_ids = await producer.publish_batch_direct(records)
    assert len(msg_ids) == 5
    assert producer.total_published == 5

    # 3. Read batch via consumer group
    consumed = await consumer.read_batch(count=10)
    assert len(consumed) == 5
    assert consumed[0][1].rig_id == "RIG-TITAN"
    assert consumed[0][1].depth_md == 8000.0
    assert consumed[-1][1].depth_md == 8004.0

    # 4. Acknowledge messages
    ack_ids = [item[0] for item in consumed]
    acked_count = await consumer.ack(*ack_ids)
    assert acked_count == 5

    # 5. Subsequent read should return empty (all consumed and acked)
    empty_read = await consumer.read_batch(count=10)
    assert len(empty_read) == 0


@pytest.mark.asyncio
async def test_producer_backpressure_buffer():
    """Test producer queue capacity and backpressure drop logic."""
    import fakeredis.aioredis as fake_aioredis
    client = fake_aioredis.FakeRedis(decode_responses=False)

    # Small buffer size of 3 items
    producer = TelemetryProducer(redis_client=client, buffer_size=3)

    rec = RigTelemetry(
        rig_id="RIG-TEST", well_id="WELL-TEST", depth_md=1000.0,
        wob=20.0, rpm=100.0, torque=10.0, flow_in=500.0, flow_out=500.0, spp=2000.0, rop=30.0
    )

    # Enqueue 3 items (filling buffer)
    assert await producer.enqueue(rec) is True
    assert await producer.enqueue(rec) is True
    assert await producer.enqueue(rec) is True

    # 4th item should be rejected (backpressure drop to protect memory)
    assert await producer.enqueue(rec) is False
    assert producer.total_dropped == 1

    await client.aclose()

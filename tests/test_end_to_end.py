"""End-to-end integration test verifying the complete streaming and inference pipeline."""

import asyncio
from datetime import datetime, timezone
import pytest

from telemetry_mesh.broker.consumer import TelemetryConsumer
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.models.anomaly import AnomalyEvent, AnomalyType
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.persistence.repository import TelemetryRepository
from telemetry_mesh.workers.ollama_pool import OllamaWorkerPool
from telemetry_mesh.workers.stream_worker import StreamWorkerNode
from telemetry_mesh.workers.threshold_engine import ThresholdEngine


@pytest.mark.asyncio
async def test_full_pipeline_end_to_end(fake_redis):
    """Verify complete lifecycle: Ingestion -> Redis Streams -> Worker -> Threshold -> Ollama -> DB."""
    stream_raw = "e2e:stream:raw"
    stream_anomalies = "e2e:stream:anomalies"
    group_name = "e2e:workers:group"

    producer = TelemetryProducer(
        redis_client=fake_redis,
        stream_raw=stream_raw,
        stream_anomalies=stream_anomalies,
    )
    consumer = TelemetryConsumer(
        redis_client=fake_redis,
        stream_name=stream_raw,
        group_name=group_name,
        consumer_name="e2e-worker",
        batch_size=20,
        block_ms=200,
    )

    repository = TelemetryRepository(pool=None)
    threshold_engine = ThresholdEngine()
    ollama_pool = OllamaWorkerPool(
        base_url="http://127.0.0.1:54321",  # triggers fallback heuristic
        fallback_mock=True,
    )

    detected_anomalies: list[AnomalyEvent] = []

    async def anomaly_cb(event: AnomalyEvent):
        detected_anomalies.append(event)

    worker = StreamWorkerNode(
        redis_client=fake_redis,
        consumer=consumer,
        producer=producer,
        ollama_pool=ollama_pool,
        repository=repository,
        threshold_engine=threshold_engine,
        on_anomaly_callback=anomaly_cb,
    )

    await worker.start()

    # Step 1: Ingest 15 baseline records to populate the sliding window
    baseline_batch = [
        RigTelemetry(
            rig_id="RIG-TITAN",
            well_id="WELL-001",
            timestamp=datetime.now(timezone.utc),
            depth_md=8500.0 + (i * 0.2),
            wob=28.0,
            rpm=120.0,
            torque=14.5,
            flow_in=620.0,
            flow_out=618.0,
            spp=2950.0,
            rop=45.0,
        )
        for i in range(15)
    ]
    await producer.publish_batch_direct(baseline_batch)

    # Allow worker to process baseline batch
    await asyncio.sleep(0.3)
    assert worker.processed_records_count == 15
    assert len(detected_anomalies) == 0

    # Step 2: Inject an anomalous record (Kick / Fluid Influx: Flow Out surges 30% over Flow In)
    kick_record = RigTelemetry(
        rig_id="RIG-TITAN",
        well_id="WELL-001",
        timestamp=datetime.now(timezone.utc),
        depth_md=8503.2,
        wob=28.0,
        rpm=120.0,
        torque=14.5,
        flow_in=600.0,
        flow_out=800.0,  # Severe influx!
        spp=2650.0,
        rop=45.0,
    )
    await producer.publish_batch_direct([kick_record])

    # Allow worker to process anomaly and execute Ollama diagnosis
    await asyncio.sleep(0.4)

    # Step 3: Verify worker behavior
    assert worker.processed_records_count == 16
    assert worker.anomalies_detected_count == 1
    assert len(detected_anomalies) == 1

    anomaly = detected_anomalies[0]
    assert anomaly.anomaly_type == AnomalyType.KICK_INFLUX
    assert anomaly.llm_diagnosis is not None
    assert anomaly.llm_diagnosis.confidence_score >= 0.90
    assert "annular preventer" in anomaly.llm_diagnosis.drilling_recommendation.lower()

    # Step 4: Verify persistence in repository
    cached_telemetry = await repository.get_recent_telemetry(rig_id="RIG-TITAN", limit=50)
    assert len(cached_telemetry) == 16

    cached_anomalies = await repository.get_recent_anomalies(rig_id="RIG-TITAN", limit=10)
    assert len(cached_anomalies) == 1
    assert cached_anomalies[0].anomaly_type == AnomalyType.KICK_INFLUX

    # Step 5: Clean shutdown
    await worker.stop()

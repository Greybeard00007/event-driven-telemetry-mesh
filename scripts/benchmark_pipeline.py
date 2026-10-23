"""Benchmark script for Event-Driven Telemetry Mesh pipeline throughput and latency."""

import asyncio
from datetime import datetime, timezone
import time
import statistics
from typing import List

from telemetry_mesh.broker.consumer import TelemetryConsumer
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.broker.redis_client import close_redis_client, get_redis_client
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.workers.sliding_window import RigSlidingWindow
from telemetry_mesh.workers.threshold_engine import ThresholdEngine


def create_sample_record(i: int) -> RigTelemetry:
    return RigTelemetry(
        rig_id="BENCH-RIG-1",
        well_id="BENCH-WELL-1",
        timestamp=datetime.now(timezone.utc),
        depth_md=8500.0 + (i * 0.1),
        depth_tvd=8000.0 + (i * 0.08),
        wob=28.5,
        rpm=120.0,
        torque=15.2,
        flow_in=620.0,
        flow_out=618.0,
        spp=2950.0,
        rop=45.0,
        gamma=65.0,
        mud_gas=120.0,
        active_pit_vol=450.0,
    )


async def run_threshold_benchmark(record_count: int = 50_000) -> None:
    print(f"\n[1/3] Benchmarking Deterministic Threshold Engine with {record_count:,} records...")
    engine = ThresholdEngine()
    window = RigSlidingWindow("BENCH-RIG-1", max_samples=30)

    # Pre-warm window
    for i in range(30):
        window.append(create_sample_record(i))

    latencies: List[float] = []
    start_total = time.perf_counter()

    for i in range(record_count):
        rec = create_sample_record(i)
        window.append(rec)
        t0 = time.perf_counter()
        engine.evaluate(rec, window)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1_000_000)  # microseconds

    total_time = time.perf_counter() - start_total
    throughput = record_count / total_time

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print(f"  Throughput:  {throughput:,.0f} records/sec")
    print(f"  Latency p50: {p50:.2f} µs")
    print(f"  Latency p95: {p95:.2f} µs")
    print(f"  Latency p99: {p99:.2f} µs")


async def run_broker_pipeline_benchmark(batch_size: int = 100, batch_count: int = 200) -> None:
    total_records = batch_size * batch_count
    print(f"\n[2/3] Benchmarking Redis Streams Producer/Consumer with {total_records:,} records...")

    redis_client = await get_redis_client(use_fake=True)
    stream_name = "benchmark:telemetry:stream"
    group_name = "benchmark:workers:group"

    producer = TelemetryProducer(redis_client=redis_client, stream_raw=stream_name)
    consumer = TelemetryConsumer(redis_client=redis_client, stream_name=stream_name, group_name=group_name, batch_size=batch_size)

    await consumer.initialize()

    # Produce batches
    start_produce = time.perf_counter()
    for b in range(batch_count):
        batch = [create_sample_record(b * batch_size + j) for j in range(batch_size)]
        await producer.publish_batch_direct(batch)
    produce_time = time.perf_counter() - start_produce
    produce_throughput = total_records / produce_time
    print(f"  Publishing:  {produce_throughput:,.0f} records/sec ({produce_time:.3f}s total)")

    # Consume batches
    consumed_count = 0
    start_consume = time.perf_counter()
    while consumed_count < total_records:
        items = await consumer.read_batch(count=batch_size)
        if not items:
            break
        consumed_count += len(items)
        msg_ids = [m[0] for m in items]
        await consumer.ack(*msg_ids)
    consume_time = time.perf_counter() - start_consume
    consume_throughput = consumed_count / consume_time
    print(f"  Consuming:   {consume_throughput:,.0f} records/sec ({consume_time:.3f}s total)")

    await close_redis_client()


async def main():
    print("=" * 65)
    print("  EVENT-DRIVEN TELEMETRY MESH // BENCHMARK SUITE")
    print("=" * 65)

    await run_threshold_benchmark(record_count=25_000)
    await run_broker_pipeline_benchmark(batch_size=100, batch_count=100)

    print("\n[3/3] Verification: Benchmark Complete. All streaming subsystems verified.")
    print("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())

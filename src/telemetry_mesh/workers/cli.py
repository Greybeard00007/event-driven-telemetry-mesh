"""CLI entrypoint for running dedicated stream worker instances."""

import asyncio
import logging
import signal
import sys
from telemetry_mesh.broker.consumer import TelemetryConsumer
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.broker.redis_client import close_redis_client, get_redis_client
from telemetry_mesh.config import settings
from telemetry_mesh.persistence.db import close_db_pool, get_db_pool
from telemetry_mesh.persistence.repository import TelemetryRepository
from telemetry_mesh.workers.ollama_pool import OllamaWorkerPool
from telemetry_mesh.workers.stream_worker import StreamWorkerNode
from telemetry_mesh.workers.threshold_engine import ThresholdEngine

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("worker_node")


async def main():
    logger.info("Initializing dedicated StreamWorkerNode...")
    redis_client = await get_redis_client()
    db_pool = await get_db_pool()
    repository = TelemetryRepository(pool=db_pool)

    consumer = TelemetryConsumer(redis_client=redis_client)
    producer = TelemetryProducer(redis_client=redis_client)
    ollama_pool = OllamaWorkerPool()
    threshold_engine = ThresholdEngine()

    worker = StreamWorkerNode(
        redis_client=redis_client,
        consumer=consumer,
        producer=producer,
        ollama_pool=ollama_pool,
        repository=repository,
        threshold_engine=threshold_engine,
    )

    await worker.start()

    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Received termination signal, shutting down worker...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass  # Windows fallback

    logger.info(f"Worker [{consumer.consumer_name}] active and listening for telemetry batches...")
    await stop_event.wait()

    await worker.stop()
    await close_db_pool()
    await close_redis_client()
    logger.info("Worker shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

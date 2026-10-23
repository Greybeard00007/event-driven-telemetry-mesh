"""CLI entrypoint for standalone multi-rig telemetry simulator."""

import asyncio
import logging
import signal
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.broker.redis_client import close_redis_client, get_redis_client
from telemetry_mesh.config import settings
from telemetry_mesh.ingestion.simulator import TelemetrySimulator
from telemetry_mesh.models.telemetry import RigTelemetry

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("simulator_cli")


async def main():
    logger.info("Initializing standalone Rig Telemetry Simulator...")
    redis_client = await get_redis_client()
    producer = TelemetryProducer(redis_client=redis_client)
    await producer.start()

    simulator = TelemetrySimulator(
        num_rigs=settings.SIMULATOR_RIGS,
        frequency_hz=settings.SIMULATOR_FREQUENCY_HZ,
        auto_anomalies=True,
        anomaly_interval_sec=settings.SIMULATOR_AUTO_ANOMALY_INTERVAL_SEC,
    )

    async def on_batch(batch: list[RigTelemetry]):
        await producer.publish_batch_direct(batch)

    sim_task = asyncio.create_task(simulator.run_loop(on_batch))

    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Termination signal received, stopping simulator...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    await stop_event.wait()

    simulator.stop()
    if not sim_task.done():
        sim_task.cancel()
        try:
            await sim_task
        except asyncio.CancelledError:
            pass

    await producer.stop()
    await close_redis_client()
    logger.info("Simulator shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

"""FastAPI application factory and lifespan manager."""

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
from typing import AsyncGenerator
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from telemetry_mesh.api.routes import router
from telemetry_mesh.api.websocket_manager import ws_manager
from telemetry_mesh.broker.consumer import TelemetryConsumer
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.broker.redis_client import close_redis_client, get_redis_client
from telemetry_mesh.config import settings
from telemetry_mesh.ingestion.simulator import TelemetrySimulator
from telemetry_mesh.models.anomaly import AnomalyEvent
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.persistence.db import close_db_pool, get_db_pool
from telemetry_mesh.persistence.repository import TelemetryRepository
from telemetry_mesh.workers.ollama_pool import OllamaWorkerPool
from telemetry_mesh.workers.stream_worker import StreamWorkerNode
from telemetry_mesh.workers.threshold_engine import ThresholdEngine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan: startup and graceful shutdown of distributed components."""
    logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
    logger.info("Initializing Event-Driven Telemetry Mesh services...")

    # 1. Connect Redis & TimescaleDB
    redis_client = await get_redis_client()
    db_pool = await get_db_pool()
    app.state.redis = redis_client
    app.state.db_pool = db_pool

    # 2. Persistence Repository
    repository = TelemetryRepository(pool=db_pool)
    app.state.repository = repository

    # 3. Producer Pipeline
    producer = TelemetryProducer(redis_client=redis_client)
    await producer.start()
    app.state.producer = producer

    # 4. Asynchronous Consumer & Worker Node
    consumer = TelemetryConsumer(redis_client=redis_client)
    app.state.consumer = consumer

    ollama_pool = OllamaWorkerPool()
    threshold_engine = ThresholdEngine()

    # Callbacks for real-time WebSocket feeds
    async def on_anomaly(anomaly: AnomalyEvent):
        await ws_manager.broadcast_anomaly(anomaly)

    async def on_telemetry(batch: list[RigTelemetry]):
        await ws_manager.broadcast_telemetry(batch)

    worker = StreamWorkerNode(
        redis_client=redis_client,
        consumer=consumer,
        producer=producer,
        ollama_pool=ollama_pool,
        repository=repository,
        threshold_engine=threshold_engine,
        on_anomaly_callback=on_anomaly,
        on_telemetry_callback=on_telemetry,
    )
    await worker.start()
    app.state.worker = worker

    # 5. Background Multi-Rig Simulator
    simulator = TelemetrySimulator(
        num_rigs=settings.SIMULATOR_RIGS,
        frequency_hz=settings.SIMULATOR_FREQUENCY_HZ,
        auto_anomalies=True,
        anomaly_interval_sec=settings.SIMULATOR_AUTO_ANOMALY_INTERVAL_SEC,
    )
    app.state.simulator = simulator

    async def on_simulator_batch(batch: list[RigTelemetry]):
        # Enqueue simulator batches into producer
        await producer.enqueue_batch(batch)

    sim_task = asyncio.create_task(simulator.run_loop(on_simulator_batch), name="rig-simulator")
    app.state.sim_task = sim_task

    logger.info("All streaming mesh services initialized successfully.")

    yield

    # Shutdown sequence
    logger.info("Shutting down streaming mesh services...")
    simulator.stop()
    if sim_task and not sim_task.done():
        sim_task.cancel()
        try:
            await sim_task
        except asyncio.CancelledError:
            pass

    await worker.stop()
    await producer.stop()
    await close_db_pool()
    await close_redis_client()
    logger.info("Streaming mesh shutdown complete.")


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    app = FastAPI(
        title="Event-Driven Telemetry Mesh",
        description="High-throughput, fault-tolerant WITSML sensor ingestion and asynchronous AI inference architecture.",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)

    # Static files & dashboard route
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        @app.get("/", include_in_schema=False)
        @app.get("/dashboard", include_in_schema=False)
        async def serve_dashboard():
            return FileResponse(str(static_dir / "index.html"))

    return app


app = create_app()

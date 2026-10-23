"""Stateless asynchronous worker node for Redis Streams micro-batch processing."""

import asyncio
from datetime import datetime, timezone
import logging
import time
from typing import Callable, List, Optional
import redis.asyncio as aioredis

from telemetry_mesh.broker.consumer import TelemetryConsumer
from telemetry_mesh.broker.producer import TelemetryProducer
from telemetry_mesh.config import settings
from telemetry_mesh.models.anomaly import AnomalyEvent
from telemetry_mesh.models.telemetry import RigTelemetry
from telemetry_mesh.persistence.repository import TelemetryRepository
from telemetry_mesh.workers.ollama_pool import OllamaWorkerPool
from telemetry_mesh.workers.sliding_window import WindowManager
from telemetry_mesh.workers.threshold_engine import ThresholdEngine

logger = logging.getLogger(__name__)


class StreamWorkerNode:
    """Stateless worker node consuming micro-batches from Redis Streams and routing anomalies to Ollama."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        worker_id: Optional[str] = None,
        consumer: Optional[TelemetryConsumer] = None,
        producer: Optional[TelemetryProducer] = None,
        ollama_pool: Optional[OllamaWorkerPool] = None,
        repository: Optional[TelemetryRepository] = None,
        threshold_engine: Optional[ThresholdEngine] = None,
        on_anomaly_callback: Optional[Callable[[AnomalyEvent], None]] = None,
        on_telemetry_callback: Optional[Callable[[List[RigTelemetry]], None]] = None,
    ):
        self.redis = redis_client
        self.worker_id = worker_id or settings.REDIS_CONSUMER_NAME
        self.consumer = consumer or TelemetryConsumer(redis_client=self.redis, consumer_name=self.worker_id)
        self.producer = producer or TelemetryProducer(redis_client=self.redis)
        self.ollama_pool = ollama_pool or OllamaWorkerPool()
        self.repository = repository or TelemetryRepository()
        self.threshold_engine = threshold_engine or ThresholdEngine()
        self.window_manager = WindowManager()

        self.on_anomaly_callback = on_anomaly_callback
        self.on_telemetry_callback = on_telemetry_callback

        self._running = False
        self._loop_task: Optional[asyncio.Task] = None
        self._reclaim_interval_sec = 15.0
        self._last_reclaim_time = time.time()

        # Telemetry metrics
        self.processed_records_count: int = 0
        self.anomalies_detected_count: int = 0
        self.reclaimed_count: int = 0

    async def start(self) -> None:
        """Initialize consumer groups and start the worker ingestion loop."""
        if self._running:
            return

        logger.info(f"Starting StreamWorkerNode [{self.consumer.consumer_name}]...")
        await self.consumer.initialize()
        await self.producer.start()

        self._running = True
        self._loop_task = asyncio.create_task(self._process_loop(), name=f"worker-{self.consumer.consumer_name}")

    async def stop(self) -> None:
        """Gracefully shut down the worker loop."""
        self._running = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None

        await self.producer.stop()
        await self.ollama_pool.close()
        logger.info(f"StreamWorkerNode [{self.consumer.consumer_name}] stopped. Processed: {self.processed_records_count}")

    async def _process_loop(self) -> None:
        """Continuous micro-batch processing loop."""
        while self._running:
            try:
                # 1. Periodically reclaim stale unacked messages from dead workers
                now = time.time()
                if now - self._last_reclaim_time > self._reclaim_interval_sec:
                    self._last_reclaim_time = now
                    stale_items = await self.consumer.reclaim_stale_messages()
                    if stale_items:
                        self.reclaimed_count += len(stale_items)
                        await self._handle_batch(stale_items)

                # 2. Read new micro-batch from Redis Stream
                batch = await self.consumer.read_batch()
                if batch:
                    await self._handle_batch(batch)
                else:
                    # Short pause if stream is empty to yield event loop
                    await asyncio.sleep(0.01)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in worker processing loop: {e}", exc_info=True)
                await asyncio.sleep(0.2)

    async def _handle_batch(self, batch: List[tuple[str, RigTelemetry]]) -> None:
        """Process an individual micro-batch of telemetry records."""
        msg_ids: List[str] = []
        records: List[RigTelemetry] = []
        anomalies_to_diagnose: List[AnomalyEvent] = []

        for msg_id, record in batch:
            msg_ids.append(msg_id)
            records.append(record)

            # Update rolling sliding window for this rig
            window = self.window_manager.get_window(record.rig_id)
            window.append(record)

            # Evaluate deterministic threshold rules
            anomaly = self.threshold_engine.evaluate(record, window)
            if anomaly:
                anomalies_to_diagnose.append(anomaly)

        # 3. Asynchronously dispatch anomalies to Ollama worker pool
        if anomalies_to_diagnose:
            await self._dispatch_anomalies(anomalies_to_diagnose)

        # 4. Batch persist telemetry records to TimescaleDB
        if records:
            await self.repository.save_telemetry_batch(records)
            self.processed_records_count += len(records)
            if self.on_telemetry_callback:
                try:
                    res = self.on_telemetry_callback(records)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception as cb_err:
                    logger.debug(f"Telemetry callback error: {cb_err}")

        # 5. Acknowledge messages in Redis Streams
        if msg_ids:
            await self.consumer.ack(*msg_ids)

    async def _dispatch_anomalies(self, anomalies: List[AnomalyEvent]) -> None:
        """Route anomalous windows to Ollama LLM pool and publish enriched events."""
        tasks = [self._process_single_anomaly(a) for a in anomalies]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_single_anomaly(self, anomaly: AnomalyEvent) -> None:
        """Route one anomaly to Ollama, save enriched event, and publish to stream."""
        try:
            # LLM inference
            diagnosis = await self.ollama_pool.diagnose_anomaly(anomaly)
            anomaly.llm_diagnosis = diagnosis
            self.anomalies_detected_count += 1

            # Persist to TimescaleDB
            await self.repository.save_anomaly_event(anomaly)

            # Publish enriched anomaly to Redis stream
            await self.producer.publish_anomaly(anomaly)

            # Invoke callback (e.g. WebSocket broadcast)
            if self.on_anomaly_callback:
                try:
                    cb_res = self.on_anomaly_callback(anomaly)
                    if asyncio.iscoroutine(cb_res):
                        await cb_res
                except Exception as e:
                    logger.debug(f"Anomaly callback error: {e}")

            logger.info(
                f"[{anomaly.rig_id}] Anomaly '{anomaly.anomaly_type.value}' diagnosed by "
                f"{diagnosis.model_name}: {diagnosis.root_cause_analysis[:75]}..."
            )
        except Exception as e:
            logger.error(f"Failed processing anomaly {anomaly.anomaly_id}: {e}", exc_info=True)

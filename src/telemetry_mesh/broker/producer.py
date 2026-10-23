"""High-throughput Redis Streams Producer for rig telemetry."""

import asyncio
from datetime import datetime, timezone
import logging
import time
from typing import List, Optional
import redis.asyncio as aioredis

from telemetry_mesh.config import settings
from telemetry_mesh.models.anomaly import AnomalyEvent
from telemetry_mesh.models.telemetry import RigTelemetry

logger = logging.getLogger(__name__)


class TelemetryProducer:
    """Decoupled high-throughput producer that streams telemetry batches into Redis Streams."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        stream_raw: Optional[str] = None,
        stream_anomalies: Optional[str] = None,
        max_len: Optional[int] = None,
        buffer_size: int = 5000,
        flush_interval_sec: float = 0.05,
    ):
        self.redis = redis_client
        self.stream_raw = stream_raw or settings.REDIS_STREAM_RAW
        self.stream_anomalies = stream_anomalies or settings.REDIS_STREAM_ANOMALIES
        self.max_len = max_len or settings.REDIS_STREAM_MAX_LEN
        self.buffer_size = buffer_size
        self.flush_interval_sec = flush_interval_sec

        # Internal high-speed buffer to decouple HTTP ingestion from network I/O
        self._queue: asyncio.Queue[RigTelemetry] = asyncio.Queue(maxsize=buffer_size)
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False

        # Metrics
        self.total_published: int = 0
        self.total_batches: int = 0
        self.total_dropped: int = 0
        self.last_flush_time: float = time.time()

    async def start(self) -> None:
        """Start the background micro-batch flushing task."""
        if self._running:
            return
        self._running = True
        self._flush_task = asyncio.create_task(self._background_flusher(), name="producer-flusher")
        logger.info(f"TelemetryProducer started background flusher on stream '{self.stream_raw}'.")

    async def stop(self) -> None:
        """Flush remaining items in the queue and terminate the background task."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        # Drain any remaining records
        await self._flush_queue_remaining()
        logger.info(f"TelemetryProducer stopped. Total published: {self.total_published}")

    async def enqueue(self, telemetry: RigTelemetry) -> bool:
        """Non-blocking ingestion buffer enqueue.

        Returns True if enqueued, False if buffer is saturated (backpressure protection).
        """
        try:
            self._queue.put_nowait(telemetry)
            return True
        except asyncio.QueueFull:
            self.total_dropped += 1
            logger.warning("Telemetry ingestion buffer full! Dropping item to prevent memory blowout.")
            return False

    async def enqueue_batch(self, telemetries: List[RigTelemetry]) -> int:
        """Enqueue multiple records. Returns count successfully queued."""
        accepted = 0
        for item in telemetries:
            if await self.enqueue(item):
                accepted += 1
        return accepted

    async def publish_batch_direct(self, telemetries: List[RigTelemetry]) -> List[str]:
        """Publish a batch directly to Redis Streams using pipeline without going through internal queue."""
        if not telemetries:
            return []

        async with self.redis.pipeline(transaction=False) as pipe:
            for item in telemetries:
                data = item.to_redis_dict()
                pipe.xadd(
                    name=self.stream_raw,
                    fields=data,
                    maxlen=self.max_len,
                    approximate=True,
                )
            results = await pipe.execute()

        msg_ids = [res.decode("utf-8") if isinstance(res, bytes) else str(res) for res in results]
        self.total_published += len(msg_ids)
        self.total_batches += 1
        return msg_ids

    async def publish_anomaly(self, anomaly: AnomalyEvent) -> str:
        """Publish an evaluated anomaly to the anomalies stream."""
        data = anomaly.to_redis_dict()
        msg_id = await self.redis.xadd(
            name=self.stream_anomalies,
            fields=data,
            maxlen=self.max_len,
            approximate=True,
        )
        return msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)

    async def _background_flusher(self) -> None:
        """Background coroutine that flushes micro-batches periodically."""
        batch: List[RigTelemetry] = []

        while self._running:
            try:
                # Wait for items or timeout
                start_time = time.time()
                while len(batch) < settings.REDIS_BATCH_SIZE:
                    time_left = max(0.001, self.flush_interval_sec - (time.time() - start_time))
                    try:
                        item = await asyncio.wait_for(self._queue.get(), timeout=time_left)
                        batch.append(item)
                    except asyncio.TimeoutError:
                        break

                if batch:
                    await self.publish_batch_direct(batch)
                    batch.clear()
                    self.last_flush_time = time.time()
                else:
                    await asyncio.sleep(self.flush_interval_sec)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in producer background flusher: {e}", exc_info=True)
                await asyncio.sleep(0.1)

    async def _flush_queue_remaining(self) -> None:
        """Drain any items left in the queue."""
        remaining: List[RigTelemetry] = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            try:
                await self.publish_batch_direct(remaining)
            except Exception as e:
                logger.error(f"Failed to flush remaining queue items on shutdown: {e}")

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

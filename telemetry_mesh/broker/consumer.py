"""Redis Streams Consumer Group manager with micro-batching and fault tolerance."""

import asyncio
import logging
import socket
import uuid
from typing import Any, Dict, List, Optional, Tuple
import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from telemetry_mesh.config import settings
from telemetry_mesh.models.telemetry import RigTelemetry

logger = logging.getLogger(__name__)


class TelemetryConsumer:
    """Consumes micro-batches of telemetry records from Redis Streams with consumer group semantics."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        stream_name: Optional[str] = None,
        group_name: Optional[str] = None,
        consumer_name: Optional[str] = None,
        batch_size: Optional[int] = None,
        block_ms: Optional[int] = None,
    ):
        self.redis = redis_client
        self.stream_name = stream_name or settings.REDIS_STREAM_RAW
        self.group_name = group_name or settings.REDIS_CONSUMER_GROUP
        # Unique consumer name incorporating hostname and random UUID
        host = socket.gethostname()[:12]
        self.consumer_name = consumer_name or f"{host}-{uuid.uuid4().hex[:6]}"
        self.batch_size = batch_size or settings.REDIS_BATCH_SIZE
        self.block_ms = block_ms or settings.REDIS_BLOCK_MS

    async def initialize(self) -> None:
        """Create consumer group if it does not already exist."""
        try:
            # mkstream=True ensures the stream is created if missing
            await self.redis.xgroup_create(
                name=self.stream_name,
                groupname=self.group_name,
                id="$",  # Start consuming only new messages
                mkstream=True,
            )
            logger.info(f"Created consumer group '{self.group_name}' on stream '{self.stream_name}'.")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{self.group_name}' already exists.")
            else:
                logger.error(f"Error creating consumer group: {e}")
                raise

    async def read_batch(self, count: Optional[int] = None) -> List[Tuple[str, RigTelemetry]]:
        """Read a micro-batch of new unacknowledged messages.

        Returns a list of (message_id, RigTelemetry) tuples.
        """
        read_count = count or self.batch_size
        try:
            streams_dict = {self.stream_name: ">"}
            response = await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams=streams_dict,
                count=read_count,
                block=self.block_ms,
            )
        except Exception as e:
            logger.error(f"Error reading from consumer group {self.group_name}: {e}")
            return []

        if not response:
            return []

        results: List[Tuple[str, RigTelemetry]] = []
        for stream, messages in response:
            for msg_id, raw_fields in messages:
                msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)
                try:
                    telemetry = RigTelemetry.from_redis_dict(raw_fields)
                    results.append((msg_id_str, telemetry))
                except Exception as parse_err:
                    logger.warning(f"Failed to parse stream message {msg_id_str}: {parse_err}. Auto-acking to prevent stall.")
                    await self.ack(msg_id_str)

        return results

    async def ack(self, *message_ids: str) -> int:
        """Acknowledge one or more message IDs."""
        if not message_ids:
            return 0
        try:
            return await self.redis.xack(self.stream_name, self.group_name, *message_ids)
        except Exception as e:
            logger.error(f"Failed to ACK messages {message_ids}: {e}")
            return 0

    async def reclaim_stale_messages(
        self,
        min_idle_ms: Optional[int] = None,
        count: int = 20,
    ) -> List[Tuple[str, RigTelemetry]]:
        """Reclaim messages pending from failed or dead workers using XAUTOCLAIM."""
        idle_time = min_idle_ms or settings.REDIS_CLAIM_MIN_IDLE_MS
        reclaimed: List[Tuple[str, RigTelemetry]] = []

        try:
            # Try XAUTOCLAIM first (Redis 6.2+)
            res = await self.redis.xautoclaim(
                name=self.stream_name,
                groupname=self.group_name,
                consumername=self.consumer_name,
                min_idle_time=idle_time,
                start_id="0-0",
                count=count,
            )
            # Response: (next_start_id, messages, [deleted_ids])
            if res and len(res) >= 2:
                messages = res[1]
                for msg_id, fields in messages:
                    if not fields:
                        continue
                    msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)
                    try:
                        record = RigTelemetry.from_redis_dict(fields)
                        reclaimed.append((msg_id_str, record))
                    except Exception as err:
                        logger.warning(f"Failed parsing reclaimed msg {msg_id_str}: {err}")
                        await self.ack(msg_id_str)
        except Exception as e:
            logger.debug(f"xautoclaim not available or failed ({e}). Checking pending entries via xpending.")
            try:
                # Fallback to xpending & xclaim
                pending_info = await self.redis.xpending_range(
                    name=self.stream_name,
                    groupname=self.group_name,
                    min="-",
                    max="+",
                    count=count,
                )
                stale_ids = []
                for p in pending_info:
                    msg_id = p["message_id"]
                    idle = p["idle"]
                    if idle >= idle_time:
                        msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)
                        stale_ids.append(msg_id_str)

                if stale_ids:
                    claimed = await self.redis.xclaim(
                        name=self.stream_name,
                        groupname=self.group_name,
                        consumername=self.consumer_name,
                        min_idle_time=idle_time,
                        message_ids=stale_ids,
                    )
                    for msg_id, fields in claimed:
                        if not fields:
                            continue
                        msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)
                        try:
                            record = RigTelemetry.from_redis_dict(fields)
                            reclaimed.append((msg_id_str, record))
                        except Exception:
                            await self.ack(msg_id_str)
            except Exception as claim_err:
                logger.debug(f"Pending reclaim failed: {claim_err}")

        if reclaimed:
            logger.info(f"Consumer '{self.consumer_name}' reclaimed {len(reclaimed)} stale messages.")
        return reclaimed

    async def get_stream_stats(self) -> Dict[str, Any]:
        """Fetch stream info, consumer lag, and pending message count."""
        stats: Dict[str, Any] = {
            "stream_name": self.stream_name,
            "group_name": self.group_name,
            "consumer_name": self.consumer_name,
            "length": 0,
            "pending": 0,
        }
        try:
            info = await self.redis.xinfo_stream(self.stream_name)
            stats["length"] = info.get("length", 0)
        except Exception:
            pass

        try:
            pending = await self.redis.xpending(self.stream_name, self.group_name)
            stats["pending"] = pending.get("pending", 0) if isinstance(pending, dict) else pending[0]
        except Exception:
            pass

        return stats

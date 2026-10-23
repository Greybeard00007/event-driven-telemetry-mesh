"""Dead-Letter Queue (DLQ) for poison pill isolation and fault-tolerant retry management."""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """Manages dead-letter stream for unparseable or repeatedly failing micro-batch messages."""

    def __init__(
        self,
        redis_client: aioredis.Redis,
        dlq_stream_name: str = "telemetry:stream:dead-letter",
        max_retries: int = 3,
    ):
        self.redis = redis_client
        self.dlq_stream_name = dlq_stream_name
        self.max_retries = max_retries

    async def route_to_dlq(
        self,
        original_msg_id: str,
        raw_fields: Dict[Any, Any],
        error_reason: str,
        delivery_count: int = 1,
    ) -> str:
        """Move a failing message to the dead-letter stream with diagnostic metadata."""
        # Normalize fields to string
        norm_fields: Dict[str, str] = {}
        for k, v in raw_fields.items():
            k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            v_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            norm_fields[k_str] = v_str

        dlq_payload = {
            "original_id": original_msg_id,
            "error_reason": error_reason,
            "delivery_count": str(delivery_count),
            "evicted_at": datetime.now(timezone.utc).isoformat(),
            "payload_dump": json.dumps(norm_fields),
        }

        msg_id = await self.redis.xadd(self.dlq_stream_name, fields=dlq_payload)
        msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)
        logger.warning(
            f"Message '{original_msg_id}' routed to DLQ '{self.dlq_stream_name}' (ID: {msg_id_str}). Reason: {error_reason}"
        )
        return msg_id_str

    async def get_dlq_stats(self) -> Dict[str, Any]:
        """Fetch DLQ stream metrics."""
        try:
            info = await self.redis.xinfo_stream(self.dlq_stream_name)
            return {
                "dlq_stream": self.dlq_stream_name,
                "length": info.get("length", 0),
                "last_generated_id": str(info.get("last-generated-id", "0-0")),
            }
        except Exception:
            return {"dlq_stream": self.dlq_stream_name, "length": 0}

    async def inspect_dlq(self, count: int = 20) -> List[Dict[str, Any]]:
        """Retrieve recent dead-letter messages for inspection."""
        try:
            messages = await self.redis.xrevrange(self.dlq_stream_name, count=count)
            results = []
            for m_id, fields in messages:
                m_id_str = m_id.decode("utf-8") if isinstance(m_id, bytes) else str(m_id)
                norm = {}
                for k, v in fields.items():
                    k_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    v_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                    norm[k_str] = v_str
                results.append({"dlq_id": m_id_str, **norm})
            return results
        except Exception as e:
            logger.error(f"Error inspecting DLQ: {e}")
            return []

    async def replay_dlq_message(self, dlq_msg_id: str, target_stream: str) -> bool:
        """Replay a dead-letter message back into the active processing stream."""
        try:
            messages = await self.redis.xrange(self.dlq_stream_name, min=dlq_msg_id, max=dlq_msg_id)
            if not messages:
                return False

            _, fields = messages[0]
            raw_payload = fields.get(b"payload_dump") or fields.get("payload_dump")
            if not raw_payload:
                return False

            payload_str = raw_payload.decode("utf-8") if isinstance(raw_payload, bytes) else str(raw_payload)
            original_dict = json.loads(payload_str)

            # Re-insert into target stream
            await self.redis.xadd(target_stream, fields=original_dict)
            # Delete from DLQ
            await self.redis.xdel(self.dlq_stream_name, dlq_msg_id)
            logger.info(f"Replayed DLQ message {dlq_msg_id} into stream '{target_stream}'.")
            return True
        except Exception as e:
            logger.error(f"Error replaying DLQ message: {e}")
            return False

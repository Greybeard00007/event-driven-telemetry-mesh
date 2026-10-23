"""Redis connection manager and utility helpers."""

import logging
from typing import Optional
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError, RedisError

from telemetry_mesh.config import settings

logger = logging.getLogger(__name__)

_global_redis_client: Optional[aioredis.Redis] = None


async def get_redis_client(redis_url: Optional[str] = None, use_fake: bool = False) -> aioredis.Redis:
    """Retrieve or initialize the async Redis client."""
    global _global_redis_client

    if _global_redis_client is not None:
        return _global_redis_client

    if use_fake:
        import fakeredis.aioredis as fake_aioredis
        logger.info("Initializing in-memory FakeRedis instance for testing.")
        _global_redis_client = fake_aioredis.FakeRedis(decode_responses=True)
        return _global_redis_client

    url = redis_url or settings.REDIS_URL
    try:
        client = aioredis.from_url(
            url,
            decode_responses=False,  # Byte mode by default for high throughput
            max_connections=50,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
        )
        # Test connection
        await client.ping()
        logger.info(f"Successfully connected to Redis broker at {url}")
        _global_redis_client = client
        return _global_redis_client
    except (ConnectionError, RedisError) as e:
        logger.warning(f"Could not connect to live Redis ({e}). Falling back to FakeRedis for resilience.")
        import fakeredis.aioredis as fake_aioredis
        _global_redis_client = fake_aioredis.FakeRedis(decode_responses=False)
        return _global_redis_client


async def close_redis_client() -> None:
    """Close the global Redis client connection pool."""
    global _global_redis_client
    if _global_redis_client is not None:
        try:
            await _global_redis_client.aclose()
        except Exception as e:
            logger.debug(f"Error closing Redis client: {e}")
        finally:
            _global_redis_client = None


def set_redis_client(client: aioredis.Redis) -> None:
    """Set global Redis client instance directly (useful for tests)."""
    global _global_redis_client
    _global_redis_client = client

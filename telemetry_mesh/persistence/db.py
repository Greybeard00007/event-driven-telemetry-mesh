"""Database connection pool management for TimescaleDB."""

import asyncio
import logging
from typing import Optional
import asyncpg
from telemetry_mesh.config import settings

logger = logging.getLogger(__name__)

_global_pool: Optional[asyncpg.Pool] = None
_db_available: bool = False


async def get_db_pool() -> Optional[asyncpg.Pool]:
    """Retrieve or initialize the asyncpg connection pool."""
    global _global_pool, _db_available

    if _global_pool is not None:
        return _global_pool

    if not settings.DB_ENABLED:
        logger.info("TimescaleDB persistence is disabled by configuration.")
        _db_available = False
        return None

    try:
        pool = await asyncpg.create_pool(
            dsn=settings.DATABASE_URL,
            min_size=settings.DB_POOL_MIN_SIZE,
            max_size=settings.DB_POOL_MAX_SIZE,
            timeout=5.0,
            command_timeout=10.0,
        )
        # Test connection
        async with pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1")
            logger.info(f"Connected to TimescaleDB at {settings.DATABASE_URL}")
        _global_pool = pool
        _db_available = True
        return _global_pool
    except Exception as e:
        logger.warning(f"Could not connect to TimescaleDB ({e}). Falling back to in-memory store.")
        _db_available = False
        return None


async def close_db_pool() -> None:
    """Close the asyncpg connection pool."""
    global _global_pool, _db_available
    if _global_pool is not None:
        try:
            await _global_pool.close()
        except Exception as e:
            logger.debug(f"Error closing DB pool: {e}")
        finally:
            _global_pool = None
            _db_available = False


def is_db_available() -> bool:
    """Check if live TimescaleDB pool is active."""
    return _db_available


def set_db_pool(pool: Optional[asyncpg.Pool]) -> None:
    """Directly set DB pool (used for testing)."""
    global _global_pool, _db_available
    _global_pool = pool
    _db_available = pool is not None

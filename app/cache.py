"""Cache-aside for order reads.

Read path:   Redis GET -> hit? return : DB -> SETEX -> return
Write path:  DB commit -> refresh the cached copy (write-through)

The write path refreshes rather than deletes so a status change is visible to
readers immediately. The TTL is still the safety net: if a worker dies between
the DB commit and the cache write, the stale entry ages out on its own.
"""

from __future__ import annotations

import json
import logging

from redis.asyncio import Redis

from app import redis_client
from app.config import settings
from app.models import Order
from app.redis_client import Keys
from app.schemas import OrderOut

log = logging.getLogger(__name__)


def _serialize(order: Order) -> str:
    return OrderOut.model_validate(order).model_dump_json()


async def get_order(redis: Redis, order_id: str) -> OrderOut | None:
    try:
        raw = await redis.get(Keys.order(order_id))
    except Exception as exc:
        # A cache outage must degrade to "slow", never to "down".
        log.warning("cache read failed for %s: %s", order_id, exc)
        return None

    if raw is None:
        await redis_client.incr_stat(redis_client.STAT_CACHE_MISSES, redis=redis)
        return None

    await redis_client.incr_stat(redis_client.STAT_CACHE_HITS, redis=redis)
    try:
        return OrderOut.model_validate(json.loads(raw))
    except Exception as exc:
        # Poisoned/stale-shape entry: drop it and fall through to the DB.
        log.warning("discarding unreadable cache entry for %s: %s", order_id, exc)
        await redis.delete(Keys.order(order_id))
        return None


async def put_order(redis: Redis, order: Order) -> None:
    try:
        await redis.set(Keys.order(str(order.id)), _serialize(order), ex=settings.cache_ttl_seconds)
    except Exception as exc:
        log.warning("cache write failed for %s: %s", order.id, exc)


async def invalidate_order(redis: Redis, order_id: str) -> None:
    try:
        await redis.delete(Keys.order(order_id))
    except Exception as exc:
        log.warning("cache invalidation failed for %s: %s", order_id, exc)

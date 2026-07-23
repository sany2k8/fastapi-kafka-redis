"""Shared Redis connection pool and key naming."""

from __future__ import annotations

import logging

from redis.asyncio import Redis

from app.config import settings

log = logging.getLogger(__name__)

_client: Redis | None = None


def get_redis() -> Redis:
    """Lazily built, process-wide client (redis-py pools connections itself)."""
    global _client
    if _client is None:
        _client = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=30,
            socket_keepalive=True,
        )
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def ping() -> bool:
    try:
        return bool(await get_redis().ping())
    except Exception as exc:  # pragma: no cover - health probe
        log.warning("redis ping failed: %s", exc)
        return False


class Keys:
    """One place for every key pattern, so the namespace stays greppable."""

    @staticmethod
    def order(order_id: str) -> str:
        return f"order:{order_id}"

    @staticmethod
    def event_guard(event_id: str) -> str:
        return f"idem:event:{event_id}"

    @staticmethod
    def api_idempotency(key: str) -> str:
        return f"idem:api:{key}"

    STATS = "stats"


# --- Counters -------------------------------------------------------------
# A single hash keeps all counters together: one HINCRBY to write, one HGETALL
# to read the whole dashboard.

STAT_PUBLISHED = "published"
STAT_CONSUMED = "consumed"
STAT_COMPLETED = "completed"
STAT_FAILED = "failed"
STAT_DUPLICATES = "duplicates_skipped"
STAT_RETRIES = "retries_scheduled"
STAT_DLQ = "dead_lettered"
STAT_CACHE_HITS = "cache_hits"
STAT_CACHE_MISSES = "cache_misses"


async def incr_stat(field: str, amount: int = 1, *, redis: Redis | None = None) -> None:
    try:
        await (redis or get_redis()).hincrby(Keys.STATS, field, amount)
    except Exception as exc:  # counters are best-effort, never fail a request
        log.debug("stat %s not recorded: %s", field, exc)


async def read_stats(redis: Redis | None = None) -> dict[str, int]:
    raw = await (redis or get_redis()).hgetall(Keys.STATS)
    return {k: int(v) for k, v in raw.items()}


async def reset_stats(redis: Redis | None = None) -> None:
    await (redis or get_redis()).delete(Keys.STATS)

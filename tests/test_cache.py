"""Cache-aside behaviour, including how it degrades when Redis misbehaves."""

from __future__ import annotations

from app import cache
from app import repository as repo
from app.models import OrderStatus
from app.redis_client import Keys
from app.schemas import OrderCreate


async def make_order(session):
    order = await repo.create_order(
        session,
        OrderCreate(customer_id="c1", sku="WIDGET-1", quantity=1, amount_cents=500),
    )
    await session.commit()
    return order


async def test_miss_then_hit(redis, session):
    order = await make_order(session)
    assert await cache.get_order(redis, str(order.id)) is None  # cold

    await cache.put_order(redis, order)
    hit = await cache.get_order(redis, str(order.id))
    assert hit is not None
    assert hit.id == order.id
    assert hit.status is OrderStatus.PENDING


async def test_hits_and_misses_are_counted(redis, session):
    order = await make_order(session)
    await cache.get_order(redis, str(order.id))
    await cache.put_order(redis, order)
    await cache.get_order(redis, str(order.id))

    counters = await redis.hgetall(Keys.STATS)
    assert counters["cache_misses"] == "1"
    assert counters["cache_hits"] == "1"


async def test_entry_carries_a_ttl(redis, session):
    """The TTL is what makes a missed invalidation self-healing."""
    order = await make_order(session)
    await cache.put_order(redis, order)
    assert await redis.ttl(Keys.order(str(order.id))) > 0


async def test_write_through_reflects_new_status(redis, session):
    order = await make_order(session)
    await cache.put_order(redis, order)

    await repo.transition(session, order, OrderStatus.COMPLETED, note="done")
    await session.commit()
    await cache.put_order(redis, order)

    assert (await cache.get_order(redis, str(order.id))).status is OrderStatus.COMPLETED


async def test_invalidation_removes_the_entry(redis, session):
    order = await make_order(session)
    await cache.put_order(redis, order)
    await cache.invalidate_order(redis, str(order.id))
    assert await cache.get_order(redis, str(order.id)) is None


async def test_corrupt_entry_is_dropped_not_raised(redis, session):
    """A poisoned cache entry must degrade to a DB read, not a 500."""
    order = await make_order(session)
    await redis.set(Keys.order(str(order.id)), "{not json")

    assert await cache.get_order(redis, str(order.id)) is None
    assert await redis.exists(Keys.order(str(order.id))) == 0


async def test_redis_outage_degrades_to_miss(session):
    """Cache down must mean 'slow', never 'broken'."""

    class BrokenRedis:
        async def get(self, *_):
            raise ConnectionError("redis is down")

        async def set(self, *_, **__):
            raise ConnectionError("redis is down")

    order = await make_order(session)
    broken = BrokenRedis()
    assert await cache.get_order(broken, str(order.id)) is None
    await cache.put_order(broken, order)  # must not raise

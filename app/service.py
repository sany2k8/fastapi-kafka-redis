"""Order service — the read/write paths the API depends on."""

from __future__ import annotations

import logging
import uuid

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app import cache, redis_client
from app import repository as repo
from app.config import settings
from app.kafka.producer import publish_event
from app.models import Order
from app.redis_client import Keys
from app.schemas import OrderCreate, OrderEvent, OrderRead

log = logging.getLogger(__name__)


async def submit_order(
    session: AsyncSession,
    redis: Redis,
    payload: OrderCreate,
    *,
    idempotency_key: str | None = None,
) -> tuple[Order, bool]:
    """Accept an order and publish `order.created`.

    Returns `(order, was_duplicate)`.

    Client-facing idempotency: if the caller sends an `Idempotency-Key`, a
    Redis `SET NX` reserves it. A retried request with the same key returns
    the original order instead of creating a second one — which is exactly
    what a mobile client double-tapping "Pay" needs.
    """
    if idempotency_key:
        existing = await _resolve_idempotency_key(session, redis, idempotency_key)
        if existing is not None:
            return existing, True

    order = await repo.create_order(session, payload)
    await session.commit()

    if idempotency_key:
        await redis.set(
            Keys.api_idempotency(idempotency_key),
            str(order.id),
            ex=settings.api_idempotency_ttl_seconds,
        )

    # NOTE: DB commit and Kafka publish are two separate systems, so this is
    # a dual write. If the process dies in between, the order stays `pending`
    # forever. A production system closes that gap with a transactional
    # outbox (write the event to a table in the same transaction, relay it to
    # Kafka with CDC). Called out here rather than hidden.
    event = OrderEvent(
        order_id=order.id,
        customer_id=order.customer_id,
        sku=order.sku,
        quantity=order.quantity,
        amount_cents=order.amount_cents,
        fail_mode=payload.fail_mode,
    )
    await publish_event(event, attempt=1)
    await redis_client.incr_stat(redis_client.STAT_PUBLISHED, redis=redis)

    await cache.put_order(redis, order)
    log.info("published order.created for %s (event %s)", order.id, event.event_id)
    return order, False


async def _resolve_idempotency_key(session: AsyncSession, redis: Redis, key: str) -> Order | None:
    raw = await redis.get(Keys.api_idempotency(key))
    if not raw:
        return None
    try:
        return await repo.get_order(session, uuid.UUID(raw))
    except ValueError:
        return None


async def read_order(session: AsyncSession, redis: Redis, order_id: uuid.UUID) -> OrderRead | None:
    """Cache-aside read: Redis first, Postgres as the fallback and truth."""
    cached = await cache.get_order(redis, str(order_id))
    if cached is not None:
        return OrderRead(**cached.model_dump(), source="cache")

    order = await repo.get_order(session, order_id)
    if order is None:
        return None

    await cache.put_order(redis, order)
    return OrderRead.model_validate(order)

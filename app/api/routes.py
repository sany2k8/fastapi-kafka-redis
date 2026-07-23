"""HTTP surface."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app import db, redis_client, service
from app import repository as repo
from app.db import get_session
from app.kafka import producer
from app.models import OrderStatus
from app.redis_client import get_redis
from app.schemas import (
    DeadLetterOut,
    HealthOut,
    OrderAccepted,
    OrderCreate,
    OrderOut,
    OrderRead,
    Stats,
)

router = APIRouter()

SessionDep = Annotated[AsyncSession, Depends(get_session)]
RedisDep = Annotated[Redis, Depends(get_redis)]


# --------------------------------------------------------------------------
# Orders
# --------------------------------------------------------------------------


@router.post(
    "/orders",
    response_model=OrderAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit an order for asynchronous processing",
)
async def create_order(
    payload: OrderCreate,
    session: SessionDep,
    redis: RedisDep,
    response: Response,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    """Returns 202 immediately — the work happens on the Kafka consumer.

    Send an `Idempotency-Key` header to make client retries safe; a repeat of
    the same key returns the original order with 200 instead of creating a
    second one.
    """
    order, duplicate = await service.submit_order(
        session, redis, payload, idempotency_key=idempotency_key
    )
    if duplicate:
        response.status_code = status.HTTP_200_OK
        return OrderAccepted(
            id=order.id,
            status=OrderStatus(order.status),
            duplicate=True,
            message="Idempotency-Key already used; returning the original order",
        )
    return OrderAccepted(id=order.id, status=OrderStatus(order.status))


@router.get(
    "/orders/{order_id}",
    response_model=OrderRead,
    summary="Read an order (cache-aside: Redis first, Postgres as fallback)",
)
async def get_order(order_id: uuid.UUID, session: SessionDep, redis: RedisDep):
    order = await service.read_order(session, redis, order_id)
    if order is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "order not found")
    return order


@router.get("/orders", response_model=list[OrderOut], summary="List orders")
async def list_orders(
    session: SessionDep,
    status_filter: Annotated[OrderStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    return await repo.list_orders(session, status=status_filter, limit=limit, offset=offset)


# --------------------------------------------------------------------------
# Operations
# --------------------------------------------------------------------------


@router.get("/dead-letters", response_model=list[DeadLetterOut], summary="Inspect the DLQ")
async def list_dead_letters(
    session: SessionDep,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
):
    return await repo.list_dead_letters(session, limit=limit, offset=offset)


@router.get("/stats", summary="Pipeline counters (Redis) and order totals (Postgres)")
async def stats(session: SessionDep, redis: RedisDep):
    counters = await redis_client.read_stats(redis)
    pipeline = Stats(**{k: v for k, v in counters.items() if k in Stats.model_fields})
    return {
        "pipeline": pipeline.model_dump(),
        "cache_hit_ratio": pipeline.cache_hit_ratio,
        "orders_by_status": await repo.count_by_status(session),
    }


@router.post("/stats/reset", status_code=status.HTTP_204_NO_CONTENT, summary="Reset counters")
async def reset_stats(redis: RedisDep):
    await redis_client.reset_stats(redis)


@router.get("/health", response_model=HealthOut, summary="Liveness / dependency probe")
async def health(response: Response):
    postgres, redis_ok, kafka = (
        await db.ping(),
        await redis_client.ping(),
        await producer.ping(),
    )
    healthy = postgres and redis_ok and kafka
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthOut(
        status="ok" if healthy else "degraded",
        postgres=postgres,
        redis=redis_ok,
        kafka=kafka,
    )

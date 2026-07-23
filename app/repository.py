"""Data access for orders — every DB read/write goes through here."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DeadLetter, Order, OrderStatus, StatusTransition
from app.schemas import OrderCreate


async def create_order(session: AsyncSession, payload: OrderCreate) -> Order:
    order = Order(
        id=uuid.uuid4(),
        customer_id=payload.customer_id,
        sku=payload.sku,
        quantity=payload.quantity,
        amount_cents=payload.amount_cents,
        status=OrderStatus.PENDING,
        fail_mode=payload.fail_mode.value if payload.fail_mode else None,
    )
    order.transitions.append(StatusTransition(to_status=OrderStatus.PENDING, note="order accepted"))
    session.add(order)
    await session.flush()
    return order


async def get_order(session: AsyncSession, order_id: uuid.UUID) -> Order | None:
    return await session.get(Order, order_id)


async def list_orders(
    session: AsyncSession,
    *,
    status: OrderStatus | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[Order]:
    stmt = select(Order).order_by(desc(Order.created_at)).limit(limit).offset(offset)
    if status is not None:
        stmt = stmt.where(Order.status == status)
    return list((await session.execute(stmt)).scalars().all())


async def transition(
    session: AsyncSession,
    order: Order,
    to_status: OrderStatus,
    *,
    note: str | None = None,
    attempts: int | None = None,
    failure_reason: str | None = None,
) -> Order:
    """Move an order to a new state and record the hop in the audit trail."""
    from_status = order.status
    order.status = to_status
    if attempts is not None:
        order.attempts = attempts
    order.failure_reason = failure_reason
    order.transitions.append(
        StatusTransition(from_status=from_status, to_status=to_status, note=note)
    )
    await session.flush()
    return order


async def record_dead_letter(
    session: AsyncSession,
    *,
    order_id: uuid.UUID | None,
    event_id: str | None,
    source_topic: str,
    partition: int,
    offset: int,
    attempts: int,
    error: str | None,
    payload: dict[str, Any],
) -> DeadLetter:
    entry = DeadLetter(
        order_id=order_id,
        event_id=event_id,
        source_topic=source_topic,
        partition=partition,
        offset=offset,
        attempts=attempts,
        error=error,
        payload=payload,
    )
    session.add(entry)
    await session.flush()
    return entry


async def list_dead_letters(
    session: AsyncSession, *, limit: int = 50, offset: int = 0
) -> list[DeadLetter]:
    stmt = select(DeadLetter).order_by(desc(DeadLetter.created_at)).limit(limit).offset(offset)
    return list((await session.execute(stmt)).scalars().all())


async def count_by_status(session: AsyncSession) -> dict[str, int]:
    from sqlalchemy import func

    stmt = select(Order.status, func.count()).group_by(Order.status)
    return {status: count for status, count in (await session.execute(stmt)).all()}

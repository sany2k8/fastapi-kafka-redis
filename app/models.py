"""SQLAlchemy models — Postgres is the source of truth, Redis is only a cache."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


# JSONB on Postgres, plain JSON everywhere else (keeps the unit tests on SQLite).
JsonCol = JSONB().with_variant(JSON(), "sqlite")


class OrderStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


TERMINAL_STATUSES = {OrderStatus.COMPLETED, OrderStatus.FAILED}


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    customer_id: Mapped[str] = mapped_column(String(64), index=True)
    sku: Mapped[str] = mapped_column(String(64))
    quantity: Mapped[int] = mapped_column(Integer)
    amount_cents: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16), default=OrderStatus.PENDING, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    failure_reason: Mapped[str | None] = mapped_column(Text, default=None)
    # Demo hook: lets a request deliberately steer the consumer down the
    # transient-retry or permanent-failure path.
    fail_mode: Mapped[str | None] = mapped_column(String(16), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    transitions: Mapped[list[StatusTransition]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        lazy="selectin",
        # Without an explicit order the audit trail comes back in whatever
        # order Postgres feels like, which makes it useless as a trail.
        order_by="StatusTransition.id",
    )


class StatusTransition(Base):
    """Append-only audit trail of every state change an order went through."""

    __tablename__ = "order_transitions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"), index=True
    )
    from_status: Mapped[str | None] = mapped_column(String(16))
    to_status: Mapped[str] = mapped_column(String(16))
    note: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    order: Mapped[Order] = relationship(back_populates="transitions")


class DeadLetter(Base):
    """Archive of everything the DLQ consumer pulled off `orders.dlq`."""

    __tablename__ = "dead_letters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[uuid.UUID | None] = mapped_column(default=None, index=True)
    event_id: Mapped[str | None] = mapped_column(String(64), default=None)
    source_topic: Mapped[str] = mapped_column(String(128))
    partition: Mapped[int] = mapped_column(Integer)
    # "offset" is a reserved word in Postgres — store it under a safe name.
    offset: Mapped[int] = mapped_column("msg_offset", BigInteger)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str | None] = mapped_column(Text, default=None)
    payload: Mapped[dict] = mapped_column(JsonCol)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


Index("ix_dead_letters_created_at", DeadLetter.created_at.desc())

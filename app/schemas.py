"""Pydantic request/response models and the Kafka event envelope."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.models import OrderStatus


class FailMode(StrEnum):
    """Demo control for exercising the retry / DLQ paths on purpose."""

    TRANSIENT = "transient"  # fails, retries with backoff, then lands in the DLQ
    PERMANENT = "permanent"  # fails once, straight to the DLQ (no retries)


class OrderCreate(BaseModel):
    customer_id: str = Field(min_length=1, max_length=64)
    sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0, le=1000)
    amount_cents: int = Field(ge=0)
    fail_mode: FailMode | None = None


class TransitionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    from_status: str | None
    to_status: str
    note: str | None
    created_at: datetime


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    customer_id: str
    sku: str
    quantity: int
    amount_cents: int
    status: OrderStatus
    attempts: int
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime
    transitions: list[TransitionOut] = Field(default_factory=list)


class OrderRead(OrderOut):
    """An order plus where the read was served from."""

    source: Literal["cache", "database"] = "database"


class OrderAccepted(BaseModel):
    id: uuid.UUID
    status: OrderStatus
    duplicate: bool = False
    message: str = "Order accepted for processing"


class DeadLetterOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    order_id: uuid.UUID | None
    event_id: str | None
    source_topic: str
    partition: int
    offset: int
    attempts: int
    error: str | None
    payload: dict[str, Any]
    created_at: datetime


class Stats(BaseModel):
    published: int = 0
    consumed: int = 0
    completed: int = 0
    failed: int = 0
    duplicates_skipped: int = 0
    retries_scheduled: int = 0
    dead_lettered: int = 0
    cache_hits: int = 0
    cache_misses: int = 0

    @property
    def cache_hit_ratio(self) -> float:
        total = self.cache_hits + self.cache_misses
        return round(self.cache_hits / total, 3) if total else 0.0


class HealthOut(BaseModel):
    status: Literal["ok", "degraded"]
    postgres: bool
    redis: bool
    kafka: bool


# --------------------------------------------------------------------------
# Kafka event envelope
# --------------------------------------------------------------------------


class OrderEvent(BaseModel):
    """What actually travels on the wire.

    The envelope is immutable: retry bookkeeping (attempt counter, next
    eligible time, last error) rides in Kafka *headers* instead, so a replayed
    payload is byte-identical to the original.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = "order.created"
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    order_id: uuid.UUID
    customer_id: str
    sku: str
    quantity: int
    amount_cents: int
    fail_mode: FailMode | None = None
